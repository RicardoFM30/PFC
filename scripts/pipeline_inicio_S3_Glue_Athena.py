"""cargar_y_unificar.py

Módulo orquestador para la distribución multimodelo, volcado de streaming (Kafka)
y aprovisionamiento automático de la infraestructura analítica (AWS Glue y Athena) desde código.
"""

import os
import json
import logging
from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import boto3
from kafka import KafkaConsumer

# Configuración de logs para auditoría del pipeline
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class CloudArchitecturePipeline:
    def __init__(self):
        """Initializa rutas locales, carga variables de entorno y clientes AWS."""
        self.ruta_proyecto = Path(__file__).resolve().parent.parent
        self.ruta_global = self.ruta_proyecto / "datasets" / "raw" / "Global"
        
        # --- Variables de Entorno ---
        self.rds_connection_string = os.getenv("RDS_URL")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.bucket_s3 = os.getenv("S3_BUCKET_NAME")
        self.region_aws = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        
        # --- Nombres de Infraestructura Analítica ---
        self.glue_db_name = "pfc_proptech_raw_db"

        # --- Validación de Entorno ---
        if not all([self.rds_connection_string, self.mongo_uri, self.bucket_s3]):
            logger.error("❌ ERROR CRÍTICO: Faltan variables esenciales en el .env (RDS_URL, MONGO_URI, S3_BUCKET_NAME)")
            raise ValueError("Configuración incompleta.")

        # --- Clientes AWS (Boto3) ---
        self.s3_client = boto3.client('s3', region_name=self.region_aws)
        self.glue_client = boto3.client('glue', region_name=self.region_aws)

    def cargar_listings_a_rds(self):
        """Carga la dimensión física en Amazon RDS."""
        ruta_csv = self.ruta_global / "listings.csv"
        if not ruta_csv.is_file(): raise FileNotFoundError(f"Falta listings.csv")
        
        df_listings = pd.read_csv(ruta_csv, low_memory=False)
        logger.info("🚀 Insertando datos estructurales en Amazon RDS...")
        try:
            engine = create_engine(self.rds_connection_string)
            df_listings.to_sql(name="listings_master", con=engine, if_exists="replace", index=False)
            logger.info(f"✔ ¡Éxito! {len(df_listings)} registros físicos en RDS.")
        except Exception as e:
            logger.error(f"❌ Error RDS: {e}"); raise

    def cargar_reviews_a_mongodb(self):
        """Carga la dimensión textual en MongoDB Atlas."""
        ruta_csv = self.ruta_global / "reviews.csv"
        if not ruta_csv.is_file(): raise FileNotFoundError(f"Falta reviews.csv")
        
        df_reviews = pd.read_csv(ruta_csv, low_memory=False)
        logger.info("🚀 Indexando opiniones en MongoDB Atlas...")
        try:
            client = MongoClient(self.mongo_uri)
            db = client["proptech_db"]
            coleccion = db["reviews_raw"]
            coleccion.drop()
            coleccion.insert_many(df_reviews.to_dict(orient="records"))
            logger.info(f"✔ ¡Éxito! {len(df_reviews)} documentos en MongoDB Atlas.")
        except Exception as e:
            logger.error(f"❌ Error MongoDB: {e}"); raise

    def vaciar_y_subir_kafka_a_s3(self):
        """Drena todos los eventos acumulados en el broker local de Kafka y los persiste en S3."""
        logger.info("🚀 Conectando a Kafka local para extraer eventos de streaming...")
        eventos = []
        try:
            consumer = KafkaConsumer(
                "busquedas_tiempo_real",
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset='earliest',
                consumer_timeout_ms=3000, # Espera 3 segundos y asume que ya leyó todo
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            for msg in consumer:
                eventos.append(msg.value)
            consumer.close()
            
            if not eventos:
                logger.warning("⚠ No había eventos en Kafka. Creando evento de control básico.")
                eventos = [{"listing_id": 101, "ratio_busquedas_zona": 1.0, "timestamp": 1716240000}]

            # Guardar localmente de forma temporal para la subida
            ruta_temporal_json = self.ruta_global / "stream_dump.json"
            with open(ruta_temporal_json, 'w') as f:
                json.dump(eventos, f, indent=2)

            # Subir al Data Lake
            self.s3_client.upload_file(Filename=str(ruta_temporal_json), Bucket=self.bucket_s3, Key="raw/kafka/stream_dump.json")
            ruta_temporal_json.unlink() # Borrar temporal local
            logger.info(f"✔ ¡Éxito! {len(eventos)} eventos de Kafka volcados a s3://{self.bucket_s3}/raw/kafka/")
        except Exception as e:
            logger.error(f"❌ Error drenando Kafka: {e}"); raise

    def subir_csvs_a_raw_s3(self):
        """Sube copias de los ficheros planos a la capa raw de S3."""
        logger.info("🚀 Sincronizando capturas CSV con la capa Raw de S3...")
        try:
            self.s3_client.upload_file(Filename=str(self.ruta_global / "listings.csv"), Bucket=self.bucket_s3, Key="raw/rds/listings.csv")
            self.s3_client.upload_file(Filename=str(self.ruta_global / "reviews.csv"), Bucket=self.bucket_s3, Key="raw/mongodb/reviews.csv")
            logger.info("✔ Ficheros CSV cargados en las zonas origen de S3.")
        except Exception as e:
            logger.error(f"❌ Error subida S3: {e}"); raise

    def aprovisionar_glue_y_athena(self):
        """Crea mediante código la Base de Datos Analítica y registra los esquemas en Glue Data Catalog."""
        logger.info("🏗 Creando infraestructura de catálogo (AWS Glue Data Catalog)...")
        
        # 1. Crear Base de Datos en Glue si no existe
        try:
            self.glue_client.create_database(DatabaseInput={'Name': self.glue_db_name, 'Description': 'Base de datos analitica raw para TFM PropTech'})
            logger.info(f"✔ Base de datos '{self.glue_db_name}' creada en AWS Glue.")
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info(f"ℹ La base de datos '{self.glue_db_name}' ya existía en AWS Glue.")

        # 2. Definición explícita de esquemas (Tablas del Catálogo)
        def generar_input_tabla(nombre, s3_path, columnas, es_csv=True):
            serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" if es_csv else "org.openx.data.jsonserde.JsonSerDe"
            input_format = "org.apache.hadoop.mapred.TextInputFormat"
            output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            
            table_input = {
                'Name': nombre, 'TableType': 'EXTERNAL_TABLE',
                'StorageDescriptor': {
                    'Columns': columnas, 'Location': f"s3://{self.bucket_s3}/{s3_path}",
                    'InputFormat': input_format, 'OutputFormat': output_format,
                    'SerdeInfo': {
                        'SerializationLibrary': serde,
                        'Parameters': {'field.delim': ',', 'serialization.format': ','} if es_csv else {}
                    }
                },
                'Parameters': {'classification': 'csv' if es_csv else 'json', 'skip.header.line.count': '1' if es_csv else '0'}
            }
            return table_input

        # Columnas esenciales simplificadas para el Catálogo Glue
        cols_listings = [{'Name': 'listing_id', 'Type': 'bigint'}, {'Name': 'property_type', 'Type': 'string'}, {'Name': 'accommodates', 'Type': 'int'}, {'Name': 'price', 'Type': 'double'}, {'Name': 'region', 'Type': 'string'}]
        cols_reviews = [{'Name': 'listing_id', 'Type': 'bigint'}, {'Name': 'id', 'Type': 'bigint'}, {'Name': 'comments', 'Type': 'string'}, {'Name': 'region', 'Type': 'string'}]
        cols_kafka = [{'Name': 'listing_id', 'Type': 'bigint'}, {'Name': 'ratio_busquedas_zona', 'Type': 'double'}, {'Name': 'timestamp', 'Type': 'double'}]

        tablas = [
            ("rds_listings", "raw/rds/", cols_listings, True),
            ("mongodb_reviews", "raw/mongodb/", cols_reviews, True),
            ("kafka_stream", "raw/kafka/", cols_kafka, False)
        ]

        # Registrar tablas en AWS Glue
        for nombre_t, ruta_s3, columnas, es_csv in tablas:
            try:
                self.glue_client.create_table(DatabaseName=self.glue_db_name, TableInput=generar_input_tabla(nombre_t, ruta_s3, columnas, es_csv))
                logger.info(f"✔ Tabla externa '{nombre_t}' catalogada correctamente.")
            except self.glue_client.exceptions.AlreadyExistsException:
                logger.info(f"ℹ La tabla '{nombre_t}' ya está en el catálogo. Saltando.")

        # 3. Inicializar directorio de resultados para queries de Athena
        logger.info("🏗 Configurando espacio de trabajo para Amazon Athena...")
        self.s3_client.put_object(Bucket=self.bucket_s3, Key="athena-query-results/")
        logger.info(f"✔ Workspace de Athena listo en s3://{self.bucket_s3}/athena-query-results/")

    def ejecutar_pipeline_completo(self):
        logger.info("========================================================")
        logger.info("🔥 INICIANDO DESPLIEGUE COMPLETO E INFRAESTRUCTURA CLOUD")
        logger.info("========================================================")
        self.cargar_listings_a_rds()
        self.cargar_reviews_a_mongodb()
        self.vaciar_y_subir_kafka_a_s3()
        self.subir_csvs_a_raw_s3()
        self.aprovisionar_glue_y_athena()
        logger.info("========================================================")
        logger.info("🎉 ¡ÉXITO TOTAL! Todo cargado e infraestructura lista.")
        logger.info("========================================================")

if __name__ == "__main__":
    pipeline = CloudArchitecturePipeline()
    pipeline.ejecutar_pipeline_completo()