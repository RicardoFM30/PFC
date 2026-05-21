"""pipeline_inicio_xauto_Glue_Athena.py

Script maestro local de despliegue único (IaC) para el TFM.
Crea Amazon RDS, autodesubre la red, siembra datasets en sus motores cloud,
genera el stage en S3 para evitar aislamientos de VPC, drena Kafka y orquesta el Glue Job.
"""

import os
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from kafka import KafkaConsumer, TopicPartition

# Cargar variables de entorno explícitamente desde la raíz
load_dotenv()

# Configuración de logs limpia y corporativa para el TFM
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class CloudOrchestrator:
    def __init__(self):
        self.ruta_proyecto = Path(__file__).resolve().parent.parent
        self.ruta_datasets_raw = self.ruta_proyecto / "datasets" / "raw"
        self.ruta_global = self.ruta_datasets_raw / "Global"
        self.ruta_scripts = self.ruta_proyecto / "scripts"
        
        # --- 1. Variables de AWS del .env ---
        self.region_aws = os.getenv("AWS_REGION", "us-east-1")
        self.bucket_s3 = os.getenv("S3_BUCKET_NAME")
        self.glue_role_arn = os.getenv("AWS_GLUE_ROLE_ARN") 

        # --- 2. Variables para la CREACIÓN del RDS ---
        self.rds_instance_id = os.getenv("RDS_INSTANCE_ID", "servidor-pfc-poc")
        self.rds_db_name = os.getenv("RDS_DB_NAME", "proptechdb")
        self.rds_user = os.getenv("RDS_USER", "postgres")
        self.rds_password = os.getenv("RDS_PASSWORD")
        
        # --- 3. Otras fuentes operacionales ---
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db_name = os.getenv("MONGO_DATABASE", "pfc_proptech")
        self.mongo_coll_name = os.getenv("MONGO_COLLECTION", "reviews_raw")
        
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "busquedas_tiempo_real")

        # Nombres lógicos de los recursos en AWS Glue
        self.job_name = "pfc_proptech_federated_etl"

        # Clientes de AWS 
        self.s3_client = boto3.client('s3', region_name=self.region_aws)
        self.glue_client = boto3.client('glue', region_name=self.region_aws)
        self.ec2_client = boto3.client('ec2', region_name=self.region_aws)
        self.rds_client = boto3.client('rds', region_name=self.region_aws)

        # Variables de infraestructura dinámicas
        self.subnet_id = None
        self.security_group_id = None
        self.rds_endpoint = None  

    def imprimir_separador(self, titulo):
        logger.info("\n" + "="*70)
        logger.info(f"👉 {titulo}")
        logger.info("="*70)

    # =====================================================================
    # FASE 0.1: CREACIÓN DE INFRAESTRUCTURA TRANSACCIONAL (Amazon RDS)
    # =====================================================================
    def provisionar_y_esperar_rds(self):
        """🏗 Crea el Amazon RDS PostgreSQL y extrae su Endpoint."""
        self.imprimir_separador("FASE 0.1: APROVISIONAMIENTO DE AMAZON RDS (POSTGRESQL)")
        if not self.rds_password:
            logger.error("❌ ERROR CRÍTICO: Falta la variable RDS_PASSWORD en tu .env")
            return False

        try:
            logger.info(f"🚀 Solicitando a AWS la creación de la instancia RDS: '{self.rds_instance_id}'...")
            self.rds_client.create_db_instance(
                DBInstanceIdentifier=self.rds_instance_id,
                MasterUsername=self.rds_user,
                MasterUserPassword=self.rds_password,
                DBName=self.rds_db_name,
                Engine="postgres",
                EngineVersion="15.4",
                DBInstanceClass="db.t3.micro",  
                AllocatedStorage=20,
                PubliclyAccessible=True
            )
            logger.info("⏳ Orden aceptada por AWS. Iniciando despliegue de hardware...")
        except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
            logger.info(f"ℹ La instancia RDS '{self.rds_instance_id}' ya existe in tu cuenta. Saltando creación.")
        except Exception as e:
            logger.error(f"❌ Fallo al solicitar el RDS: {e}")
            return False

        while True:
            try:
                desc_response = self.rds_client.describe_db_instances(DBInstanceIdentifier=self.rds_instance_id)
                db_instance = desc_response['DBInstances'][0]
                estado = db_instance['DBInstanceStatus']
                
                if estado == 'available':
                    self.rds_endpoint = db_instance['Endpoint']['Address']
                    logger.info("\n" + "🌟"*20)
                    logger.info(f"✔ ¡BASE DE DATOS OPERATIVA Y LISTA!")
                    logger.info(f"🔗 RDS Endpoint detectado: {self.rds_endpoint}")
                    logger.info("🌟"*20)
                    break
                else:
                    logger.info(f"   [ESTADO RDS]: El servidor está '{estado}'... (Siguiente chequeo en 30s)")
                    time.sleep(30)
            except Exception as e:
                logger.error(f"❌ Error al consultar el estado del RDS: {e}")
                time.sleep(30)
        return True

    # =====================================================================
    # FASE 0.2: AUTODESCUBRIMIENTO DE LA ARQUITECTURA DE RED (VPC)
    # =====================================================================
    def descubrir_infraestructura_red_default(self):
        """🕵️‍♂️ Descubre las subredes y grupos de seguridad del Learner Lab."""
        self.imprimir_separador("FASE 0.2: AUTODESCUBRIMIENTO DE RED EN AWS LEARNER LAB")
        try:
            sg_response = self.ec2_client.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': ['default']}]
            )
            if sg_response['SecurityGroups']:
                self.security_group_id = sg_response['SecurityGroups'][0]['GroupId']
                logger.info(f"   ✔ Security Group ID: {self.security_group_id}")
            
            subnet_response = self.ec2_client.describe_subnets()
            if subnet_response['Subnets']:
                self.subnet_id = subnet_response['Subnets'][0]['SubnetId']
                logger.info(f"   ✔ Subnet ID: {self.subnet_id}")
                return True
        except Exception as e:
            logger.error(f"❌ Error crítico en el autodescubrimiento de red: {e}")
            return False

    # =====================================================================
    # FASE 0.3: INGESTA TRANSACCIONAL / SEMBRADO DE DATOS (CSV A MOTORES CLOUD)
    # =====================================================================
    def sembrar_datos_locales_a_motores(self):
        """🌱 Carga limpia inyectando los datos en 'listings_master' de RDS y generando stage en S3/MongoDB."""
        self.imprimir_separador("FASE 0.3: SEMBRADO E INGESTA DE DATASETS EN MOTORES CLOUD")
        
        csv_listings = self.ruta_global / "listings.csv"
        csv_reviews = self.ruta_global / "reviews.csv"

        # 1. Sembrado Seguro en Amazon RDS PostgreSQL apuntando a 'listings_master'
        try:
            str_conn = f"postgresql://{self.rds_user}:{self.rds_password}@{self.rds_endpoint}:5432/{self.rds_db_name}"
            engine = create_engine(str_conn)
            
            if csv_listings.exists():
                logger.info("🗑 Eliminando tabla previa 'listings_master' para recrearla limpiamente...")
                with engine.connect() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS listings_master CASCADE;"))
                    conn.commit()
                
                df_headers = pd.read_csv(csv_listings, nrows=1)
                dict_tipos_seguros = {}
                
                for col in df_headers.columns:
                    col_lower = col.lower()
                    if any(x in col_lower for x in ['name', 'text', 'description', 'about', 'overview', 'url', 'location', 'neighbourhood', 'neighborhood', 'amenities', 'license', 'source', 'verifications', 'bookable', 'availability']):
                        dict_tipos_seguros[col] = str
                    elif 'id' in col_lower:
                        dict_tipos_seguros[col] = str 

                logger.info(f"📥 Volcando bloques completos de '{csv_listings.name}' en la tabla 'listings_master'...")
                primer_chunk = True
                
                for chunk in pd.read_csv(csv_listings, chunksize=10000, dtype=dict_tipos_seguros, low_memory=False):
                    if 'price' in chunk.columns:
                        chunk['price'] = chunk['price'].astype(str).str.replace('$', '', regex=False) \
                                                                     .str.replace(',', '', regex=False) \
                                                                     .str.strip()
                        chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce')

                    if primer_chunk:
                        chunk.to_sql("listings_master", engine, if_exists="replace", index=False)
                        primer_chunk = False
                    else:
                        chunk.to_sql("listings_master", engine, if_exists="append", index=False)
                        
                logger.info("✔ Ingesta relacional completada con éxito en la tabla 'listings_master'.")
            else:
                logger.warning(f"⚠ Archivo local '{csv_listings}' no encontrado. Saltando siembra relacional.")
        except Exception as e:
            logger.error(f"❌ Error en la siembra de Amazon RDS: {e}"); return False

        # 2. Sembrado en MongoDB Atlas y duplicado analítico en S3 Raw (Estrategia Anti-Bloqueo de Red VPC)
        try:
            logger.info("🔌 Conectando al clúster NoSQL de MongoDB Atlas...")
            client = MongoClient(self.mongo_uri)
            db = client[self.mongo_db_name]
            coleccion = db[self.mongo_coll_name]

            documentos_actuales = coleccion.count_documents({})
            ruta_temp_reviews = self.ruta_global / "reviews_stage.json"

            if csv_reviews.exists():
                # 🎯 GENERACIÓN DEL STAGE COMPARTIDO: Creamos un JSON listo para S3 de forma mandatoria
                logger.info(f"📦 Creando stage analítico local de '{csv_reviews.name}' para aislar dependencias de red...")
                chunk_muestra = pd.read_csv(csv_reviews, nrows=25000, low_memory=False)
                chunk_muestra.to_json(ruta_temp_reviews, orient='records', lines=True)
                
                logger.info("📤 Subiendo stage de reseñas al Data Lake (s3://raw/reviews_mongo/)...")
                self.s3_client.upload_file(Filename=str(ruta_temp_reviews), Bucket=self.bucket_s3, Key="raw/reviews_mongo/reviews_stage.json")
                ruta_temp_reviews.unlink() # Limpiar archivo local temporal

                # Control de cuota para MongoDB Atlas
                if documentos_actuales == 0:
                    logger.info(f"📤 Volcando también lote controlado en la colección NoSQL '{self.mongo_coll_name}'...")
                    records = json.loads(chunk_muestra.to_json(orient='records'))
                    coleccion.insert_many(records)
                    logger.info("✔ Ingesta semiestructurada completada con éxito en MongoDB Atlas.")
                else:
                    logger.info(f"ℹ MongoDB ya contiene {documentos_actuales} documentos. Saltando escritura para proteger cuota.")
            
            client.close()
            return True
        except Exception as e:
            logger.error(f"❌ Error en la siembra de MongoDB/S3: {e}"); return False

    # =====================================================================
    # FASE 1: PREPARACIÓN DE DATOS E INYECCIÓN DE INFRAESTRUCTURA DE GLUE
    # =====================================================================
    def fase_1_1_volcar_kafka_a_s3(self):
        self.imprimir_separador("FASE 1.1: DRENANDO EVENTOS DE KAFKA LOCAL HACIA S3")
        eventos = []
        try:
            logger.info(f"🔌 Conectando al broker local de Kafka ({self.kafka_bootstrap})...")
            consumer = KafkaConsumer(
                bootstrap_servers=[self.kafka_bootstrap],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            tp = TopicPartition(self.kafka_topic, 0)
            consumer.assign([tp])
            consumer.seek_to_beginning(tp)
            inicio = consumer.position(tp)
            consumer.seek_to_end(tp)
            final = consumer.position(tp)
            
            logger.info(f"📊 Mensajes acumulados en el tópico '{self.kafka_topic}' = {final - inicio}")
            consumer.seek_to_beginning(tp)
            
            if final - inicio == 0:
                logger.warning("⚠ Kafka local vacío. Insertando evento de control por defecto.")
                eventos = [{"listing_id": "18674", "action": "ver_anuncio", "timestamp": time.time()}]
            else:
                logger.info("⏳ Absorbiendo clickstream acumulado...")
                mensajes_dict = consumer.poll(timeout_ms=2000, max_records=20000)
                if tp in mensajes_dict:
                    for msg in mensajes_dict[tp]:
                        eventos.append(msg.value)
                        
            consumer.close()

            ruta_temp_json = self.ruta_global / "stream_dump.json"
            with open(ruta_temp_json, 'w') as f:
                json.dump(eventos, f)

            logger.info(f"📤 Subiendo {len(eventos)} eventos analíticos al Data Lake en S3...")
            self.s3_client.upload_file(Filename=str(ruta_temp_json), Bucket=self.bucket_s3, Key="raw/eventos_kafka/stream_dump.json")
            ruta_temp_json.unlink()
            logger.info("✔ ¡Éxito! Logs disponibles en s3://raw/eventos_kafka/")
        except Exception as e:
            logger.error(f"❌ Fallo en la extracción de Kafka: {e}"); raise

    def fase_1_3_subir_script_pyspark_y_crear_job(self):
        self.imprimir_separador("FASE 1.3: SUBIENDO SCRIPT PYSPARK REAL Y CONFIGURANDO GLUE JOB")
        script_local_path = self.ruta_scripts / "job_pyspark_analitico.py"
        s3_script_key = "scripts/job_pyspark_analitico.py"
        s3_script_uri = f"s3://{self.bucket_s3}/{s3_script_key}"
        
        logger.info(f"📤 Subiendo código de procesamiento PySpark a tu bucket S3: {s3_script_uri}...")
        try:
            self.s3_client.upload_file(Filename=str(script_local_path), Bucket=self.bucket_s3, Key=s3_script_key)
            logger.info("✔ Archivo .py analítico cargado correctamente en el Data Lake.")
        except Exception as e:
            logger.error(f"❌ Error al subir el script analítico a S3: {e}"); raise

        logger.info(f"🏗 Registrando el Job '{self.job_name}' en el motor serverless de AWS Glue...")
        
        try:
            # 🎯 CORRECCIÓN: Eliminamos por completo el parámetro Connections para evitar el fallo de validación
            self.glue_client.create_job(
                Name=self.job_name, 
                Description='ETL Federado Multimodelo PropTech - TFM (Ejecución Spark)',
                Role=self.glue_role_arn, 
                Command={'Name': 'glueetl', 'ScriptLocation': s3_script_uri, 'PythonVersion': '3'},
                DefaultArguments={
                    '--job-language': 'python', '--enable-metrics': 'true'
                },
                GlueVersion='4.0', 
                NumberOfWorkers=2, 
                WorkerType='G.1X'
            )
            logger.info(f"✔ Job '{self.job_name}' aprovisionado correctamente en la nube.")
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"ℹ El Job '{self.job_name}' ya existía. Sincronizando puntero...")
            # Sincronizamos actualizando el script pero sin enviarle conexiones
            self.glue_client.update_job(
                JobName=self.job_name, 
                JobUpdate={
                    'Role': self.glue_role_arn, 
                    'Command': {'Name': 'glueetl', 'ScriptLocation': s3_script_uri, 'PythonVersion': '3'}
                }
            )
            logger.info("✔ Sincronización de código completada con éxito.")

    # =====================================================================
    # FASE 2: EXECUCIÓN Y ESCUCHA ACTIVA DEL PROCESAMIENTO CLOUD
    # =====================================================================
    def fase_2_ejecutar_y_monitorear_job(self):
        self.imprimir_separador("FASE 2: ORQUESTACIÓN Y MONITOREO DEL PROCESAMIENTO EN LA NUBE")
        logger.info(f"🚀 Despertando clúster de Spark en AWS para el Job '{self.job_name}'...")
        run_response = self.glue_client.start_job_run(JobName=self.job_name)
        run_id = run_response['JobRunId']
        
        while True:
            status_response = self.glue_client.get_job_run(JobName=self.job_name, RunId=run_id)
            job_run = status_response['JobRun']
            estado = job_run['JobRunState']
            
            if estado in ['STARTING', 'RUNNING']:
                logger.info(f"   [ESTADO]: Clúster {estado}... (Verificando en 20 segundos)")
                time.sleep(20)
            elif estado == 'SUCCEEDED':
                logger.info(f"\n🎉 ¡ETL FEDERADO COMPLETADO! Parquet en: s3://{self.bucket_s3}/curated/")
                break
            else:
                error_msg = job_run.get('ErrorMessage', 'No hay descripción disponible en la API.')
                logger.error(f"❌ Fallo en AWS Glue. Estado final: {estado}")
                logger.error(f"🔍 [MOTIVO REAL DEL FALLO]: {error_msg}")
                break

    # =====================================================================
    # FASE 3: AUTOMATIZACIÓN DEL CATÁLOGO DE CONSULTAS EN AMAZON ATHENA
    # =====================================================================
    def fase_3_automatizar_athena(self):
        self.imprimir_separador("FASE 3: AUTOMATIZACIÓN Y DESPLIEGUE EN AMAZON ATHENA")
        athena_client = boto3.client('athena', region_name=self.region_aws)
        
        # Carpeta obligatoria en S3 para guardar los resultados de las queries
        s3_output = f"s3://{self.bucket_s3}/athena-results/"
        
        # 1. Query para crear la base de datos lógica
        query_db = "CREATE DATABASE IF NOT EXISTS proptech_analytics_db;"
        
        # 2. Query para crear la tabla externa apuntando a tus 20 archivos Parquet
        query_tabla = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS proptech_analytics_db.dataset_master (
            listing_id STRING,
            name STRING,
            property_type STRING,
            room_type STRING,
            accommodates INT,
            bedrooms DOUBLE,
            beds DOUBLE,
            price DOUBLE,
            latitude DOUBLE,
            longitude DOUBLE,
            total_reviews_historicas BIGINT,
            total_clicks_acumulados BIGINT
        )
        STORED AS PARQUET
        LOCATION 's3://{self.bucket_s3}/curated/dataset_proptech_master/';
        """
        
        logger.info("🗄 Creando base de datos 'proptech_analytics_db' en Athena...")
        athena_client.start_query_execution(
            QueryString=query_db, 
            ResultConfiguration={'OutputLocation': s3_output}
        )
        time.sleep(3) # Pausa de cortesía para que AWS asimile el esquema
        
        logger.info("📊 Registrando tabla externa externa 'dataset_master' apuntando a los Parquet...")
        athena_client.start_query_execution(
            QueryString=query_tabla, 
            ResultConfiguration={'OutputLocation': s3_output}
        )
        logger.info("🎉 ¡Todo el pipeline automatizado! Datos listos para consumir mediante SQL.")

    def arrancar_pipeline_end_to_end(self):
        if self.provisionar_y_esperar_rds():
            if self.descubrir_infraestructura_red_default():
                
                # 🕹️ TIP: Si ya sembraste con éxito RDS y MongoDB, puedes comentar esta línea:
                self.sembrar_datos_locales_a_motores() 
                
                self.fase_1_1_volcar_kafka_a_s3()
                # self.fase_1_2_crear_conexiones_glue() # <- Desactivada para saltarse el bloqueo estricto de red
                self.fase_1_3_subir_script_pyspark_y_crear_job()
                self.fase_2_ejecutar_y_monitorear_job()
                self.fase_3_automatizar_athena()
                
if __name__ == "__main__":
    orchestrator = CloudOrchestrator()
    orchestrator.arrancar_pipeline_end_to_end()