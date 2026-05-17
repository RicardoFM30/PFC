"""pipeline_inicio.py

Script de flujo unificado para despliegue, ingesta y consolidación de datos en el proyecto PropTech.
Esta versión automatiza el arranque de Kafka mediante Docker Compose si no se detecta activo,
e inyecta datos de simulación ligeros ideales para la defensa del Hito 1.
"""

import json
import logging
import os
import time
import subprocess
from pathlib import Path
from typing import Dict, List, Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient

# Configuración del sistema de logs para ver el proceso en tiempo real
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_env_variable(key: str, default: str = "") -> str:
    value = os.getenv(key, default)
    if not value:
        logger.warning("Variable de entorno %s no está definida, usando valor por defecto.", key)
    return value


def ensure_kafka_running(bootstrap_servers: List[str]) -> None:
    logger.info("Fase de Despliegue: Verificando si Apache Kafka está activo en %s...", bootstrap_servers)
    
    def is_kafka_alive() -> bool:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="pfc-check", request_timeout_ms=2000)
            admin.close()
            return True
        except Exception:
            return False

    if is_kafka_alive():
        logger.info("✔ Apache Kafka ya está corriendo y accesible.")
        return

    logger.warning("⚠ Kafka no responde. Intentando levantar el entorno Docker automático para el equipo...")

    script_root = Path(__file__).resolve().parent
    project_root = script_root.parent
    docker_compose_path = project_root / "docker" / "docker-compose.yml"

    if not docker_compose_path.is_file():
        logger.error("❌ Crítico: No se encontró el archivo docker-compose.yml en %s", docker_compose_path)
        return

    try:
        logger.info("Ejecutando 'docker compose up -d'...")
        subprocess.run(["docker", "compose", "-f", str(docker_compose_path), "up", "-d"], check=True)
    except Exception as e:
        logger.error("❌ Error al intentar levantar Docker Compose: %s", e)
        return

    max_intentos = 12
    tiempo_espera = 5
    logger.info("⏳ Esperando a que el contenedor inicie y KRaft esté listo (Máx 60s)...")
    
    for intento in range(1, max_intentos + 1):
        time.sleep(tiempo_espera)
        logger.info("   ↳ Intento %d/%d: ¿Estás listo Kafka?", intento, max_intentos)
        
        if is_kafka_alive():
            logger.info("✔ ¡Kafka ha arrancado correctamente y está listo para recibir datos!")
            time.sleep(2)
            return
            
    logger.error("❌ Kafka no ha respondido después de %d segundos. Revisa Docker Desktop.", max_intentos * tiempo_espera)


def create_s3_bucket(bucket_name: str, region: str) -> None:
    logger.info("Fase de despliegue: creando bucket S3 '%s' en la región '%s'.", bucket_name, region)
    s3_client = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info("Bucket S3 creado o ya existente: %s", bucket_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info("El bucket ya pertenece al usuario: %s", bucket_name)
        else:
            logger.error("Error creando bucket S3: %s", error)
            raise


def create_kafka_topic(bootstrap_servers: List[str], topic_name: str) -> None:
    logger.info("Fase de despliegue: creando tópico Kafka '%s'.", topic_name)
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="pfc-admin")
        topic_list = [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Tópico Kafka creado con éxito: %s", topic_name)
    except Exception as error:
        logger.warning("Nota: No se pudo crear el tópico Kafka de forma directa o ya existe: %s", error)


def simulate_rds_ingestion(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("Fase de ingesta: simulando inserción en RDS de datos de inmuebles (Listings).")
    return listings


def ingest_reviews_mongodb(mongo_uri: str, database: str, collection_name: str, reviews: List[Dict[str, Any]]) -> None:
    logger.info("Fase de ingesta: conectando a MongoDB Atlas e insertando reseñas cualitativas.")
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection_name]
    
    # Idempotencia: Limpiamos ejecuciones anteriores para que no se dupliquen datos
    collection.delete_many({}) 
    collection.insert_many(reviews)
    logger.info("✔ Reseñas inyectadas con éxito. Documentos en la nube: %s", collection.count_documents({}))


def publish_kafka_messages(bootstrap_servers: List[str], topic_name: str, events: List[Dict[str, Any]]) -> None:
    logger.info("Fase de ingesta: publicando mensajes de streaming en Kafka.")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in events:
        producer.send(topic_name, event)
    producer.flush()
    logger.info("✔ Mensajes de demanda en tiempo real enviados al tópico: %s", topic_name)


def consume_kafka_search_events(bootstrap_servers: List[str], topic_name: str, timeout: int = 10) -> List[Dict[str, Any]]:
    logger.info("Fase de procesamiento: consumiendo eventos dinámicos desde Kafka.")
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    records = [message.value for message in consumer]
    logger.info("Eventos de búsqueda leídos del flujo de Kafka: %d", len(records))
    return records


def aggregate_dataset(listings: List[Dict[str, Any]], reviews: List[Dict[str, Any]], search_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("Fase de procesamiento: unificando fuentes de datos híbridas en memoria (JOIN Lógico).")

    reviews_by_listing = {}
    for review in reviews:
        listing_id = review["listing_id"]
        if listing_id not in reviews_by_listing:
            reviews_by_listing[listing_id] = {"comments": [], "score_sentimiento": []}
        reviews_by_listing[listing_id]["comments"].append(review["comments"])
        reviews_by_listing[listing_id]["score_sentimiento"].append(review["score_sentimiento"])

    demand_index_by_listing = {}
    for event in search_events:
        listing_id = event["listing_id"]
        demand_index_by_listing.setdefault(listing_id, []).append(event["ratio_busquedas_zona"])

    consolidated = []
    for listing in listings:
        listing_id = listing["listing_id"]
        review_entry = reviews_by_listing.get(listing_id, {"comments": [], "score_sentimiento": [0.0]})
        demand_values = demand_index_by_listing.get(listing_id, [1.0])

        consolidated.append(
            {
                **listing,
                "comments": review_entry["comments"],
                "score_sentimiento": sum(review_entry["score_sentimiento"]) / max(len(review_entry["score_sentimiento"]), 1),
                "ratio_busquedas_zona": sum(demand_values) / max(len(demand_values), 1),
            }
        )

    logger.info("Dataset consolidado completado con %d filas maestras.", len(consolidated))
    return consolidated


def save_to_s3(bucket_name: str, object_key: str, payload: str, region: str) -> None:
    logger.info("Fase de procesamiento: subiendo dataset unificado a S3: s3://%s/%s", bucket_name, object_key)
    s3_client = boto3.client("s3", region_name=region)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=payload.encode("utf-8"))
    logger.info("✔ Dataset consolidado persistido en S3 de manera exitosa.")


def build_mock_data() -> Dict[str, Any]:
    """Genera datos ágiles y controlados para testear la arquitectura."""
    listings = [
        {"listing_id": 101, "property_type": "Apartamento", "accommodates": 4, "bathrooms": 2, "bedrooms": 2, "beds": 3, "precio_noche": 120.0},
        {"listing_id": 102, "property_type": "Estudio", "accommodates": 2, "bathrooms": 1, "bedrooms": 1, "beds": 1, "precio_noche": 85.0},
    ]
    reviews = [
        {"listing_id": 101, "comments": "Excelente ubicación, limpio y muy recomendable.", "score_sentimiento": 0.92},
        {"listing_id": 102, "comments": "Buena relación calidad-precio, perfecto.", "score_sentimiento": 0.81},
    ]
    search_events = [
        {"listing_id": 101, "ratio_busquedas_zona": 1.4},
        {"listing_id": 101, "ratio_busquedas_zona": 1.7},
        {"listing_id": 102, "ratio_busquedas_zona": 0.9},
    ]
    return {"listings": listings, "reviews": reviews, "search_events": search_events}


def main() -> None:
    logger.info("=====================================================================")
    logger.info("Iniciando Pipeline de Ingesta y Despliegue Automatizado - PropTech v2")
    logger.info("=====================================================================")

    script_root = Path(__file__).resolve().parent
    project_root = script_root.parent
    
    env_path = project_root / ".env"
    if env_path.is_file():
        logger.info("Cargando credenciales desde %s", env_path)
        load_dotenv(dotenv_path=env_path)
    else:
        logger.warning("Archivo .env no detectado. Se usarán variables locales por defecto.")

    region = get_env_variable("AWS_REGION", "us-east-1")
    bucket_name = get_env_variable("S3_BUCKET_NAME", "pfc-data-lake-ejemplo")
    kafka_bootstrap = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    kafka_topic = get_env_variable("KAFKA_TOPIC", "busquedas_tiempo_real")
    mongo_uri = get_env_variable("MONGO_URI", "mongodb://localhost:27017")
    mongo_database = get_env_variable("MONGO_DATABASE", "pfc_proptech")
    mongo_collection = get_env_variable("MONGO_COLLECTION", "reviews")

    # Ejecución del pipeline
    ensure_kafka_running(kafka_bootstrap)
    create_s3_bucket(bucket_name, region)
    create_kafka_topic(kafka_bootstrap, kafka_topic)

    mock_data = build_mock_data()
    listings = simulate_rds_ingestion(mock_data["listings"])
    ingest_reviews_mongodb(mongo_uri, mongo_database, mongo_collection, mock_data["reviews"])
    publish_kafka_messages(kafka_bootstrap, kafka_topic, mock_data["search_events"])
    
    time.sleep(3) 

    # Extracción y Consolidación
    reviews_from_db = list(MongoClient(mongo_uri)[mongo_database][mongo_collection].find({}, {"_id": 0}))
    logger.info("Reseñas recuperadas de MongoDB Atlas: %d", len(reviews_from_db))
    search_events_consumed = consume_kafka_search_events(kafka_bootstrap, kafka_topic, timeout=5)
    
    consolidated_dataset = aggregate_dataset(listings, reviews_from_db, search_events_consumed)
    dataset_payload = json.dumps(consolidated_dataset, ensure_ascii=False, indent=2)
    save_to_s3(bucket_name, "consolidated/proptech_dataset.json", dataset_payload, region)

    logger.info("=====================================================================")
    logger.info("✔ Pipeline completado correctamente de forma integrada e idempotente.")
    logger.info("=====================================================================")


if __name__ == "__main__":
    main()