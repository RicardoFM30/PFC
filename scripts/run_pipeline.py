"""run_pipeline.py

Script de flujo unificado para despliegue, ingesta y consolidación de datos en el proyecto PropTech.
Este archivo demuestra cómo integrar S3, MongoDB Atlas y Kafka en un único flujo de trabajo.
"""

import json
import logging
import os
import time
from typing import Dict, List, Any

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_env_variable(key: str, default: str = "") -> str:
    value = os.getenv(key, default)
    if not value:
        logger.warning("Variable de entorno %s no está definida, usando valor por defecto.", key)
    return value


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
        logger.info("Tópico Kafka creado: %s", topic_name)
    except Exception as error:
        logger.warning("No se pudo crear el tópico Kafka o ya existe: %s", error)


def simulate_rds_ingestion(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("Fase de ingesta: simulando inserción en RDS de datos de listings.")
    for listing in listings:
        logger.debug("Listing importado a RDS: %s", listing)
    return listings


def ingest_reviews_mongodb(mongo_uri: str, database: str, collection_name: str, reviews: List[Dict[str, Any]]) -> None:
    logger.info("Fase de ingesta: insertando reviews en MongoDB Atlas.")
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection_name]
    collection.insert_many(reviews)
    logger.info("Reseñas insertadas en MongoDB Atlas: %s documentos.", collection.count_documents({}))


def publish_kafka_messages(bootstrap_servers: List[str], topic_name: str, events: List[Dict[str, Any]]) -> None:
    logger.info("Fase de ingesta: publicando mensajes en Kafka.")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in events:
        producer.send(topic_name, event)
        logger.debug("Evento enviado a Kafka: %s", event)
    producer.flush()
    logger.info("Mensajes enviados al tópico Kafka: %s", topic_name)


def consume_kafka_search_events(bootstrap_servers: List[str], topic_name: str, timeout: int = 10) -> List[Dict[str, Any]]:
    logger.info("Fase de procesamiento: consumiendo eventos de búsqueda desde Kafka.")
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    records = [message.value for message in consumer]
    logger.info("Eventos de búsqueda leídos de Kafka: %d", len(records))
    return records


def aggregate_dataset(listings: List[Dict[str, Any]], reviews: List[Dict[str, Any]], search_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info("Fase de procesamiento: unificando fuentes de datos en memoria.")

    reviews_by_listing = {}
    for review in reviews:
        listing_id = review["listing_id"]
        if listing_id not in reviews_by_listing:
            reviews_by_listing[listing_id] = {
                "comments": [],
                "score_sentimiento": [],
            }
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
        demand_values = demand_index_by_listing.get(listing_id, [0.0])

        consolidated.append(
            {
                **listing,
                "comments": review_entry["comments"],
                "score_sentimiento": sum(review_entry["score_sentimiento"]) / max(len(review_entry["score_sentimiento"]), 1),
                "ratio_busquedas_zona": sum(demand_values) / max(len(demand_values), 1),
            }
        )

    logger.info("Dataset consolidado creado con %d registros.", len(consolidated))
    return consolidated


def save_to_s3(bucket_name: str, object_key: str, payload: str, region: str) -> None:
    logger.info("Fase de procesamiento: guardando dataset unificado en S3: %s/%s", bucket_name, object_key)
    s3_client = boto3.client("s3", region_name=region)
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=payload.encode("utf-8"))
    logger.info("Dataset consolidado guardado en S3 exitosamente.")


def build_mock_data() -> Dict[str, Any]:
    listings = [
        {
            "listing_id": 101,
            "property_type": "Apartamento",
            "accommodates": 4,
            "bathrooms": 2,
            "bedrooms": 2,
            "beds": 3,
            "precio_noche": 120.0,
        },
        {
            "listing_id": 102,
            "property_type": "Estudio",
            "accommodates": 2,
            "bathrooms": 1,
            "bedrooms": 1,
            "beds": 1,
            "precio_noche": 85.0,
        },
    ]

    reviews = [
        {
            "listing_id": 101,
            "comments": "Excelente ubicación, limpio y muy recomendable.",
            "score_sentimiento": 0.92,
        },
        {
            "listing_id": 102,
            "comments": "Buena relación calidad-precio, perfecto para una escapada de fin de semana.",
            "score_sentimiento": 0.81,
        },
    ]

    search_events = [
        {"listing_id": 101, "ratio_busquedas_zona": 1.4},
        {"listing_id": 101, "ratio_busquedas_zona": 1.7},
        {"listing_id": 102, "ratio_busquedas_zona": 0.9},
    ]

    return {
        "listings": listings,
        "reviews": reviews,
        "search_events": search_events,
    }


def main() -> None:
    logger.info("Inicio del pipeline de despliegue, ingesta y procesamiento.")

    region = get_env_variable("AWS_REGION", "us-east-1")
    bucket_name = get_env_variable("S3_BUCKET_NAME", "pfc-data-lake-ejemplo")
    kafka_bootstrap = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    kafka_topic = get_env_variable("KAFKA_TOPIC", "busquedas_tiempo_real")
    mongo_uri = get_env_variable("MONGO_URI", "mongodb://localhost:27017")
    mongo_database = get_env_variable("MONGO_DATABASE", "pfc_proptech")
    mongo_collection = get_env_variable("MONGO_COLLECTION", "reviews")

    create_s3_bucket(bucket_name, region)
    create_kafka_topic(kafka_bootstrap, kafka_topic)

    mock_data = build_mock_data()
    listings = simulate_rds_ingestion(mock_data["listings"])
    ingest_reviews_mongodb(mongo_uri, mongo_database, mongo_collection, mock_data["reviews"])

    publish_kafka_messages(kafka_bootstrap, kafka_topic, mock_data["search_events"])
    time.sleep(3)

    reviews_from_db = list(
        MongoClient(mongo_uri)[mongo_database][mongo_collection].find({}, {"_id": 0})
    )
    logger.info("Reseñas recuperadas para el procesamiento: %d", len(reviews_from_db))

    search_events_consumed = consume_kafka_search_events(kafka_bootstrap, kafka_topic, timeout=5)
    consolidated_dataset = aggregate_dataset(listings, reviews_from_db, search_events_consumed)

    dataset_payload = json.dumps(consolidated_dataset, ensure_ascii=False, indent=2)
    save_to_s3(bucket_name, "consolidated/proptech_dataset.json", dataset_payload, region)

    logger.info("Pipeline completado correctamente.")


if __name__ == "__main__":
    main()
