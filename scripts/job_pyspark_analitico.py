"""job_pyspark_analitico.py

Script analítico real que se ejecuta DENTRO del clúster de AWS Glue.
Extrae los datos en paralelo, realiza el JOIN federado por RAM y los guarda en Parquet.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, lit

# 1. Inicialización de los contextos nativos de Spark y Glue en la nube
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("🚀 Clúster de Spark encendido. Iniciando ETL Federado...")

# =====================================================================
# EXTRAER FUENTE 1: Amazon RDS (Estructurada - PostgreSQL de la VPC)
# =====================================================================
print("📥 Leyendo inventario maestro desde Amazon RDS PostgreSQL...")

# Usamos el método nativo de Glue para perforar la red privada de la base de datos
df_rds = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "conexion_rds_listings",
        "dbtable": "listings_master"
    }
).toDF()

# =====================================================================
# EXTRAER FUENTE 2: Stage de MongoDB Atlas desde S3 (Aislamiento de Red VPC)
# =====================================================================
print("📥 Leyendo corpus de reseñas extraídas desde el Stage de S3...")

# Al leerlo directo de S3 evitamos que el aislamiento de la VPC tire el clúster por falta de NAT Gateway
df_mongo = spark.read.json("s3://pfc-data-lake-proptech-ricardo/raw/reviews_mongo/reviews_stage.json")

# Aseguramos el filtrado y tipado consistente para el JOIN
df_mongo_filtered = df_mongo.select(col("listing_id").cast("string"), col("comments"))

# Agrupamos las reseñas para calcular cuántas tiene cada propiedad
df_reviews_agg = df_mongo_filtered.groupBy("listing_id") \
    .agg(count("comments").alias("total_reviews_historicas"))

# =====================================================================
# EXTRAER FUENTE 3: S3 Raw Data Lake (Streaming dump de Kafka)
# =====================================================================
print("📥 Leyendo logs de navegación (Clickstream) drenados desde S3...")
df_kafka = spark.read.json("s3://pfc-data-lake-proptech-ricardo/raw/eventos_kafka/stream_dump.json")

# Forzamos que la clave de cruce sea String para homogeneizar
df_kafka_filtered = df_kafka.select(col("listing_id").cast("string"), col("action"))

# Agrupamos las interacciones en tiempo real por propiedad para medir el interés
df_kafka_agg = df_kafka_filtered.groupBy("listing_id") \
    .agg(count("action").alias("total_clicks_acumulados"))

# =====================================================================
# FUSIÓN MULTIMODELO (El JOIN por Memoria RAM Distribuida)
# =====================================================================
print("🔀 Fusionando las 3 velocidades de datos por la clave común 'listing_id'...")

# Sincronizamos el ID del RDS a formato String antes de efectuar el cruce federado
df_rds = df_rds.withColumn("listing_id", col("listing_id").cast("string"))

df_consolidado = df_rds.join(df_reviews_agg, on="listing_id", how="left") \
                       .join(df_kafka_agg, on="listing_id", how="left") \
                       .na.fill(0, ["total_reviews_historicas", "total_clicks_acumulados"])

# =====================================================================
# PERSISTENCIA EN EL DATA LAKE COMPRIMIDO
# =====================================================================
print("💾 Persistiendo tablón unificado en formato columnar Apache Parquet...")
output_path = "s3://pfc-data-lake-proptech-ricardo/curated/dataset_proptech_master/"
df_consolidado.write.mode("overwrite").parquet(output_path)

print(f"🎉 ¡ETL completado con éxito! Matriz analítica salvada en: {output_path}")
job.commit()