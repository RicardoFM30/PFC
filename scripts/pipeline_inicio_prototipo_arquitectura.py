"""pipeline_inicio.py

Script de flujo unificado para despliegue, ingesta y consolidación de datos.
Esta versión actúa como Infraestructura como Código (IaC): crea el bucket S3,
levanta una instancia RDS en AWS desde cero, crea los topics de Kafka y 
puebla todo con datos ficticios antes de consolidarlos.
"""

import json
import logging
import os
import time
import subprocess
from pathlib import Path
from typing import Dict, List, Any

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_env_variable(key: str) -> str:
    """Extrae una variable de entorno de forma estricta."""
    value = os.getenv(key)
    if not value:
        logger.error("❌ CRÍTICO: Variable de entorno REQUERIDA no definida: %s", key)
        raise ValueError(f"Falta configurar en el .env: {key}")
    return value


def ensure_kafka_running(bootstrap_servers: List[str]) -> None:
    logger.info("Fase 1: Verificando clúster de Kafka en %s...", bootstrap_servers)
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="pfc-check", request_timeout_ms=2000)
        admin.close()
        logger.info("✔ Apache Kafka está corriendo.")
    except Exception:
        logger.warning("⚠ Kafka no responde. Asegúrate de tener Docker Compose levantado o tu cluster activo.")


def create_s3_bucket(bucket_name: str, region: str) -> None:
    logger.info("Fase 1: Verificando/Creando bucket S3 '%s'...", bucket_name)
    s3_client = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        logger.info("✔ Bucket S3 listo.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info("✔ Bucket S3 ya existía y te pertenece.")
        else:
            raise


def deploy_rds_instance(instance_id: str, db_name: str, user: str, password: str, region: str) -> str:
    """Despliega una instancia RDS en AWS y devuelve el Host (Endpoint) cuando está lista."""
    logger.info("Fase 1: Verificando instancia Amazon RDS '%s'...", instance_id)
    rds_client = boto3.client("rds", region_name=region)
    
    try:
        # Comprobamos si ya existe para no crearla dos veces
        response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
        instance = response["DBInstances"][0]
        status = instance["DBInstanceStatus"]
        
        if status == "available":
            host = instance["Endpoint"]["Address"]
            logger.info("✔ La instancia RDS ya existe y está disponible en: %s", host)
            return host
        else:
            logger.info("⏳ La instancia existe pero está en estado '%s'. Esperando...", status)
            
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            logger.info("⚙️ La instancia no existe. Ordenando a AWS que la cree. ESTO TARDARÁ ENTRE 5 Y 10 MINUTOS...")
            rds_client.create_db_instance(
                DBInstanceIdentifier=instance_id,
                AllocatedStorage=20,
                DBName=db_name,
                Engine='postgres',
                MasterUsername=user,
                MasterUserPassword=password,
                DBInstanceClass='db.t3.micro',
                PubliclyAccessible=True # Necesario para conectarnos desde nuestro PC
                # Eliminado: SkipFinalSnapshot=True (Causaba el crash en boto3)
            )
        else:
            raise

    # Esperamos activamente a que AWS termine de montar el servidor
    waiter = rds_client.get_waiter('db_instance_available')
    logger.info("⏳ Esperando a que AWS finalice el aprovisionamiento físico (puede tardar hasta 10 mins)...")
    waiter.wait(DBInstanceIdentifier=instance_id)
    
    # Una vez lista, extraemos la URL de conexión (Host)
    response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
    host = response["DBInstances"][0]["Endpoint"]["Address"]
    logger.info("✔ ¡Instancia RDS creada con éxito! Host: %s", host)
    return host

def open_rds_firewall(instance_id: str, region: str) -> None:
    """Abre el puerto 5432 en el Security Group asociado al RDS para permitir la conexión."""
    logger.info("Fase 1.5: Configurando el Firewall (Security Group) de AWS...")
    rds_client = boto3.client("rds", region_name=region)
    ec2_client = boto3.client("ec2", region_name=region)
    
    try:
        # 1. Averiguamos qué Security Group le ha asignado AWS al RDS
        response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
        sg_id = response["DBInstances"][0]["VpcSecurityGroups"][0]["VpcSecurityGroupId"]
        
        # 2. Le decimos a EC2 que añada una regla para permitir el puerto 5432 desde cualquier sitio (0.0.0.0/0)
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 5432,
                    'ToPort': 5432,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                }
            ]
        )
        logger.info("✔ Regla de Firewall añadida. El puerto 5432 ahora está abierto.")
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            logger.info("✔ La regla del Firewall (puerto 5432) ya estaba abierta.")
        else:
            logger.error("❌ Error al modificar el Security Group: %s", e)
            raise

def init_rds_and_ingest(host: str, dbname: str, user: str, password: str, mock_listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Se conecta al RDS recién creado, crea la tabla e inyecta los datos ficticios."""
    logger.info("Fase 2: Conectando a RDS, creando esquema e insertando datos ficticios...")
    try:
        conn = psycopg2.connect(host=host, port=5432, dbname=dbname, user=user, password=password)
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Creamos la tabla desde cero
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                listing_id INT PRIMARY KEY,
                property_type VARCHAR(50),
                room_type VARCHAR(50),
                accommodates INT,
                bedrooms INT,
                beds INT,
                price DECIMAL(10,2)
            );
        """)
        
        # 2. Limpiamos por si ejecutamos el script varias veces (Idempotencia)
        cursor.execute("TRUNCATE TABLE listings;")
        
        # 3. Insertamos los datos ficticios
        for item in mock_listings:
            cursor.execute("""
                INSERT INTO listings (listing_id, property_type, room_type, accommodates, bedrooms, beds, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (item["listing_id"], item["property_type"], item["room_type"], 
                  item["accommodates"], item["bedrooms"], item["beds"], item["price"]))
            
        # 4. Leemos los datos para devolverlos al pipeline
        cursor.execute("SELECT * FROM listings;")
        records = cursor.fetchall()
        
        listings = []
        for record in records:
            r_dict = dict(record)
            if r_dict.get("price"):
                r_dict["price"] = float(r_dict["price"])
            listings.append(r_dict)
            
        cursor.close()
        conn.close()
        logger.info("✔ Base de datos RDS inicializada. %d alojamientos extraídos.", len(listings))
        return listings
        
    except Exception as e:
        logger.error("❌ Fallo crítico al operar con RDS. Comprueba que tu grupo de seguridad de AWS (VPC) permite el puerto 5432: %s", e)
        raise


def ingest_reviews_mongodb(mongo_uri: str, database: str, collection_name: str, reviews: List[Dict[str, Any]]) -> None:
    logger.info("Fase 2: Conectando a MongoDB Atlas e insertando reseñas ficticias.")
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection_name]
    
    collection.delete_many({}) 
    collection.insert_many(reviews)
    logger.info("✔ Reseñas inyectadas con éxito en Atlas.")


def process_kafka_stream(bootstrap_servers: List[str], topic_name: str, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("Fase 2: Publicando y consumiendo telemetría ficticia en Kafka.")
    # Publicar
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for event in events:
        producer.send(topic_name, event)
    producer.flush()
    
    # Consumir
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    records = [message.value for message in consumer]
    logger.info("✔ Leídos %d eventos de streaming.", len(records))
    return records


def build_fictitious_data() -> Dict[str, Any]:
    """Genera todo el ecosistema de datos ficticios para la PoC sin filtros."""
    listings = [
        {"listing_id": 1001, "property_type": "Apartment", "room_type": "Entire home", "accommodates": 4, "bedrooms": 2, "beds": 2, "price": 150.0},
        {"listing_id": 1002, "property_type": "House", "room_type": "Private room", "accommodates": 2, "bedrooms": 1, "beds": 1, "price": 65.0},
        {"listing_id": 1003, "property_type": "Loft", "room_type": "Entire home", "accommodates": 3, "bedrooms": 1, "beds": 2, "price": 110.0},
    ]
    reviews = [
        {"listing_id": 1001, "comments": "Increíble ubicación, muy limpio.", "score_sentimiento": 0.95},
        {"listing_id": 1001, "comments": "Un poco ruidoso por la noche.", "score_sentimiento": -0.20},
        {"listing_id": 1002, "comments": "El anfitrión fue muy amable.", "score_sentimiento": 0.85},
        {"listing_id": 1003, "comments": "Normal, acorde al precio.", "score_sentimiento": 0.50},
    ]
    events = [
        {"listing_id": 1001, "ratio_busquedas_zona": 2.5}, # Alta demanda
        {"listing_id": 1001, "ratio_busquedas_zona": 2.1},
        {"listing_id": 1002, "ratio_busquedas_zona": 0.8}, # Baja demanda
        {"listing_id": 1003, "ratio_busquedas_zona": 1.1},
    ]
    return {"listings": listings, "reviews": reviews, "events": events}


def aggregate_dataset(listings: List[Dict], reviews: List[Dict], events: List[Dict]) -> List[Dict]:
    logger.info("Fase 3: Unificando las tres fuentes (JOIN sin filtros)...")

    # Agrupar reviews por alojamiento
    reviews_by_id = {}
    for r in reviews:
        lid = r["listing_id"]
        reviews_by_id.setdefault(lid, {"comments": [], "scores": []})
        reviews_by_id[lid]["comments"].append(r["comments"])
        reviews_by_id[lid]["scores"].append(r["score_sentimiento"])

    # Agrupar eventos por alojamiento
    events_by_id = {}
    for e in events:
        lid = e["listing_id"]
        events_by_id.setdefault(lid, []).append(e["ratio_busquedas_zona"])

    consolidated = []
    for listing in listings:
        lid = listing["listing_id"]
        rev_data = reviews_by_id.get(lid, {"comments": [], "scores": [0.0]})
        evt_data = events_by_id.get(lid, [1.0])

        avg_score = sum(rev_data["scores"]) / max(len(rev_data["scores"]), 1)
        avg_demand = sum(evt_data) / max(len(evt_data), 1)

        consolidated.append({
            **listing,
            "todas_las_reviews": rev_data["comments"], # No filtramos nada
            "score_sentimiento_nlp": round(avg_score, 2),
            "demanda_tiempo_real": round(avg_demand, 2)
        })

    return consolidated


def save_to_s3(bucket_name: str, payload: str, region: str) -> None:
    logger.info("Fase 4: Subiendo Super Tabla a S3...")
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket_name, Key="gold_zone/dataset_unificado.json", Body=payload.encode("utf-8")
    )
    logger.info("✔ ¡Dataset subido con éxito a la nube!")


def main() -> None:
    logger.info("=====================================================================")
    logger.info("Iniciando Pipeline de Infraestructura como Código (IaC) e Ingesta")
    logger.info("=====================================================================")

    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.is_file():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError("Se requiere archivo .env con credenciales AWS y MongoDB.")

    # 0. Cargar credenciales
    region = get_env_variable("AWS_REGION")
    bucket_name = get_env_variable("S3_BUCKET_NAME")
    
    # Credenciales de intención para crear el RDS
    rds_instance_id = get_env_variable("RDS_INSTANCE_ID")
    rds_db_name = get_env_variable("RDS_DB_NAME")
    rds_user = get_env_variable("RDS_USER")
    rds_password = get_env_variable("RDS_PASSWORD")

    mongo_uri = get_env_variable("MONGO_URI")
    kafka_boot = get_env_variable("KAFKA_BOOTSTRAP_SERVERS").split(",")

    # 1. Desplegar Infraestructura
    ensure_kafka_running(kafka_boot)
    create_s3_bucket(bucket_name, region)
    
    # AWS creará el servidor y nos devolverá la IP
    rds_host = deploy_rds_instance(rds_instance_id, rds_db_name, rds_user, rds_password, region)

    # Abrimos el firewall automáticamente antes de intentar conectarnos
    open_rds_firewall(rds_instance_id, region)

    # 2. Generar e Inyectar Datos Ficticios
    mock_data = build_fictitious_data()
    
    listings_rds = init_rds_and_ingest(rds_host, rds_db_name, rds_user, rds_password, mock_data["listings"])
    ingest_reviews_mongodb(mongo_uri, "pfc_poc", "reviews_ficticias", mock_data["reviews"])
    events_kafka = process_kafka_stream(kafka_boot, "poc_demand_topic", mock_data["events"])

    # 3. Extraer Mongo (RDS y Kafka ya los tenemos en memoria)
    reviews_mongo = list(MongoClient(mongo_uri)["pfc_poc"]["reviews_ficticias"].find({}, {"_id": 0}))

    # 4. Consolidar (El JOIN total sin filtros)
    super_tabla = aggregate_dataset(listings_rds, reviews_mongo, events_kafka)
    
    # 5. Guardar en Data Lake
    save_to_s3(bucket_name, json.dumps(super_tabla, ensure_ascii=False, indent=2), region)

    logger.info("=====================================================================")
    logger.info("✔ Pipeline e Infraestructura completados con éxito.")
    logger.info("=====================================================================")


if __name__ == "__main__":
    main()