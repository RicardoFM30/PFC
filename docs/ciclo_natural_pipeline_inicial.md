# Línea de Tiempo del Pipeline Inicial: De la PoC con Datos Ficticios al Tablón Unificado

Este documento reconstruye la **secuencia natural de ejecución** del pipeline original (previo a la industrialización con datos reales de *Inside Airbnb*), documentando el orden exacto en que se invocaban los scripts, los datos que circulaban entre ellos y el rol que cada componente desempeñaba dentro de la arquitectura federada. La traza corresponde al flujo implementado por `scripts/pipeline_inicio_prototipo_arquitectura.py` y orquestado de forma ampliada por `scripts/pipeline_inicio_xauto_Glue_Athena.py`.

---

## 1. Contexto del Pipeline Inicial

En su primera encarnación, el proyecto no consumía los CSVs reales de *Inside Airbnb*. El objetivo era **demostrar la viabilidad técnica de la arquitectura políglota** (RDS + MongoDB + Kafka) sin filtrar ni depurar todavía los datos. Para ello, el script generaba **datos ficticios en memoria** (`build_fictitious_data()`) que viajaban por las tres fuentes operacionales antes de consolidarse en una "Súper Tabla" en Amazon S3.

El flujo se diseñó con tres principios:

* **Infraestructura como Código (IaC):** Cada ejecución recrea la nube desde cero si es necesario.
* **Idempotencia:** Reejecuciones sucesivas no duplican datos ni rompen el esquema.
* **Aislamiento de fases:** Cada etapa tiene logs limpios y barreras de error explícitas.

---

## 2. Diagrama de Línea de Tiempo

```text
[T=0]   ▶ Carga de .env y validación de variables críticas
            │
            ▼
[T=1]   ▶ Fase 1: Despliegue de Infraestructura
            ├──▶ ensure_kafka_running()       → Verifica broker en localhost:9092
            ├──▶ create_s3_bucket()           → Crea bucket en AWS S3 (idempotente)
            ├──▶ deploy_rds_instance()        → Levanta RDS PostgreSQL (5-10 min)
            └──▶ open_rds_firewall()          → Abre puerto 5432 en Security Group
            │
            ▼
[T=2]   ▶ Fase 2: Inyección de Datos Ficticios
            ├──▶ build_fictitious_data()      → Genera 3 listings, 4 reviews, 4 eventos
            ├──▶ init_rds_and_ingest()        → CREATE TABLE + TRUNCATE + INSERT en RDS
            ├──▶ ingest_reviews_mongodb()     → delete_many() + insert_many() en Atlas
            └──▶ process_kafka_stream()       → Publica 4 eventos y los consume
            │
            ▼
[T=3]   ▶ Fase 3: JOIN Lógico Sin Filtros
            └──▶ aggregate_dataset()          → Unifica listings + reviews + eventos
            │
            ▼
[T=4]   ▶ Fase 4: Persistencia en Data Lake
            └──▶ save_to_s3()                 → Sube JSON a s3://<bucket>/gold_zone/
            │
            ▼
[T=5]   ▶ ✔ Pipeline completo
```

---

## 3. Descripción Detallada de Cada Fase

### 3.1 Fase 0 — Bootstrap y Validación de Variables

Antes de tocar la nube, el script verifica que el archivo `.env` exista en la raíz del proyecto y que contenga **todas** las credenciales obligatorias. Si falta una sola variable, el script aborta con `ValueError` para evitar estados parciales en AWS.

| Variable | Función |
|---|---|
| `AWS_REGION` | Región de despliegue (ej. `us-east-1`) |
| `S3_BUCKET_NAME` | Nombre del bucket del Data Lake |
| `RDS_INSTANCE_ID`, `RDS_DB_NAME`, `RDS_USER`, `RDS_PASSWORD` | Identidad de la base transaccional |
| `MONGO_URI` | Cadena de conexión a MongoDB Atlas |
| `KAFKA_BOOTSTRAP_SERVERS` | Lista separada por comas (ej. `localhost:9092`) |

> **Detalle de robustez:** La función `get_env_variable()` eleva el fallo a `ERROR` de logging antes de propagar la excepción, lo que permite distinguir en consola si el problema es de credenciales o de red.

### 3.2 Fase 1 — Despliegue de Infraestructura (IaC)

Esta fase materializa las tres patas de la arquitectura. Todas las operaciones son **idempotentes**: si el recurso ya existe, el script lo detecta y lo reutiliza en lugar de crearlo de nuevo.

#### 3.2.1 Verificación de Kafka

```python
admin = KafkaAdminClient(bootstrap_servers=..., request_timeout_ms=2000)
```

Si el broker no responde en 2 segundos, se registra un `warning` informativo. **No aborta** la ejecución, porque Kafka no es estrictamente necesario para las fases RDS y MongoDB.

#### 3.2.2 Creación del Bucket S3

Se distingue entre la región `us-east-1` (que no acepta `LocationConstraint`) y el resto. Si el bucket ya pertenece a la cuenta, se captura `BucketAlreadyOwnedByYou` y se continúa.

#### 3.2.3 Aprovisionamiento del RDS

Este es el **cuello de botella temporal** del pipeline. AWS tarda entre 5 y 10 minutos en levantar una instancia `db.t3.micro` desde cero. El script utiliza `waiter.wait(DBInstanceIdentifier=...)` con un *waiter* nativo de boto3 que hace polling cada 30 segundos hasta que el estado pasa a `available`.

> **Punto crítico:** Durante la espera, el log emite mensajes `⏳ La instancia está 'creating'...` para que el operador vea que el script sigue vivo y no se trata de un cuelgue.

#### 3.2.4 Apertura del Firewall

Una vez el RDS está disponible, EC2 necesita una regla en su `SecurityGroup` para aceptar conexiones al puerto 5432 desde `0.0.0.0/0`. El script:

1. Lee el `VpcSecurityGroupId` asociado al RDS.
2. Llama a `ec2_client.authorize_security_group_ingress()`.
3. Captura `InvalidPermission.Duplicate` para no fallar en reejecuciones.

### 3.3 Fase 2 — Inyección de Datos Ficticios

Los datos sintéticos se generan en el propio script con `build_fictitious_data()`:

| Fuente | Volumen | Ejemplo de registro |
|---|---|---|
| `listings` (RDS) | 3 anuncios | `{"listing_id": 1001, "room_type": "Entire home", "accommodates": 4, "price": 150.0}` |
| `reviews` (Mongo) | 4 reseñas con `score_sentimiento` | `{"listing_id": 1001, "comments": "Increíble ubicación", "score_sentimiento": 0.95}` |
| `events` (Kafka) | 4 eventos de demanda | `{"listing_id": 1001, "ratio_busquedas_zona": 2.5}` |

Cada fuente se inyecta por su canal nativo:

* **RDS:** `CREATE TABLE IF NOT EXISTS` seguido de `TRUNCATE` (limpieza) y un bucle de `INSERT` uno a uno (idempotencia total).
* **MongoDB:** `delete_many({})` y `insert_many(reviews)` sobre la colección.
* **Kafka:** `KafkaProducer.send()` con `value_serializer=json`, seguido de un `KafkaConsumer` con `auto_offset_reset='earliest'` y `consumer_timeout_ms=3000` para drenar el tópico.

### 3.4 Fase 3 — JOIN Lógico Sin Filtros

La función `aggregate_dataset()` realiza el JOIN en Python puro (sin SQL, sin Spark). El algoritmo:

1. Agrupa las reseñas por `listing_id` en un diccionario.
2. Agrupa los eventos de Kafka por `listing_id`.
3. Itera sobre los `listings` y, para cada uno, calcula:
   * `avg_score` = media aritmética de los `score_sentimiento`.
   * `avg_demand` = media aritmética de los `ratio_busquedas_zona`.
4. Empaqueta el resultado en un documento con **todas las reviews embebidas como lista** (sin filtrar).

> **Decisión de diseño:** En esta fase inicial, el JOIN **no aplica ningún filtro de calidad, deduplicación ni ventana temporal**. El objetivo es puramente estructural: demostrar que las tres fuentes pueden consolidarse en un único documento anidado.

### 3.5 Fase 4 — Persistencia en la Gold Zone

El tablón unificado (un JSON anidado) se serializa con `json.dumps(..., ensure_ascii=False, indent=2)` y se sube con `s3.put_object()` a la ruta:

```text
s3://<S3_BUCKET_NAME>/gold_zone/dataset_unificado.json
```

Esta es la **materia prima** que el cuaderno del Hito 1 consume después para iniciar el EDA real.

---

## 4. Línea de Tiempo del Pipeline Orquestado en AWS

El script `scripts/pipeline_inicio_xauto_Glue_Athena.py` es la **evolución industrial** del pipeline anterior. Mantiene la misma estructura lógica, pero delega el JOIN pesado a **AWS Glue (Spark)** y automatiza la catalogación en **Amazon Athena**. Su línea de tiempo es:

```text
[T=0]   ▶ Bootstrap y carga de .env
            │
            ▼
[T=1]   ▶ Fase 0.1: Aprovisionar RDS PostgreSQL y esperar a estado 'available'
            │
            ▼
[T=2]   ▶ Fase 0.2: Autodescubrimiento de VPC (Security Group 'default' + Subnet)
            │
            ▼
[T=3]   ▶ Fase 0.3: Siembra de datos reales en RDS y MongoDB Atlas
            ├──▶ Volcado por chunks de listings.csv a 'listings_master'
            └──▶ Volcado controlado de reviews.csv a Atlas + stage JSON en S3
            │
            ▼
[T=4]   ▶ Fase 1.1: Drenar Kafka local a S3 (s3://raw/eventos_kafka/stream_dump.json)
            │
            ▼
[T=5]   ▶ Fase 1.3: Subir job_pyspark_analitico.py a S3 y registrar Glue Job
            │
            ▼
[T=6]   ▶ Fase 2: Ejecutar el Glue Job y monitorear hasta estado 'SUCCEEDED'
            │
            ▼
[T=7]   ▶ Fase 3: Crear base de datos y tabla externa en Athena
            │
            ▼
[T=8]   ▶ ✔ Tablón maestro disponible en s3://curated/dataset_proptech_master/
```

Las diferencias clave con el pipeline inicial son:

| Aspecto | Pipeline Inicial | Pipeline Orquestado |
|---|---|---|
| Datos de entrada | Ficticios en memoria | CSVs reales de *Inside Airbnb* |
| JOIN | Python puro (listas/dicts) | AWS Glue + PySpark (distribuido) |
| Persistencia | JSON anidado | Parquet columnar particionado |
| Catalogación | Ninguna | Athena Data Catalog + tabla externa |
| Coste temporal | ~10 min (espera de RDS) | ~10-15 min (RDS + Glue startup) |

---

## 5. Ciclo de Vida Completo (Punto de Vista del Operador)

Desde la perspectiva del operador que ejecuta el proyecto por primera vez, el **ciclo natural** se resume en:

1. **Configurar credenciales** en `.env` (AWS, Mongo, Kafka, HF).
2. **Levantar Kafka local** con `docker-compose up -d`.
3. **(Opcional) Generar tráfico sintético** con `python scripts/añadir_eventos_kafka.py`.
4. **Ejecutar el pipeline de ingesta** con `python scripts/pipeline_inicio_prototipo_arquitectura.py` (versión PoC) o `python scripts/pipeline_inicio_xauto_Glue_Athena.py` (versión industrial).
5. **Verificar la Gold Zone** en S3 listando `s3://<bucket>/curated/dataset_proptech_master/`.
6. **Abrir el cuaderno del Hito 1** y continuar el flujo de modelado.

> **Convención operativa:** Los scripts de ingesta asumen que la raíz del proyecto se encuentra **un nivel por encima** de `scripts/`. Si se mueven a otra ubicación, las rutas absolutas resueltas con `Path(__file__).resolve().parent.parent` se romperán.

---

## 6. Por Qué Esta Línea de Tiempo es Importante

Documentar la secuencia exacta del pipeline inicial cumple tres funciones académicas y de ingeniería:

1. **Trazabilidad de auditoría:** Cualquier evaluador puede reproducir la PoC paso a paso y comparar los resultados con los del pipeline industrial.
2. **Identificación de deuda técnica:** La fase inicial demuestra que el JOIN se realizaba en Python puro. Migrar ese JOIN a **AWS Glue (Spark)** fue la decisión clave que permitió escalar a ~34.000 registros reales.
3. **Aislamiento del cambio de dataset:** El paso de "datos ficticios (3 listings)" a "datos reales (34.000 listings)" no modificó la arquitectura políglota ni los endpoints de la nube; solo cambió el **origen** de los datos y el **motor de procesamiento**.

---

## 7. Conclusión

El pipeline inicial es la **prueba de concepto** que valida la arquitectura federada antes de invertir en industrialización. Su línea de tiempo (Fases 0 → 4) es corta, determinista y autocontenida: no depende de servicios externos más allá de las credenciales del `.env`, y todos sus pasos están protegidos por idempotencia y logging explícito. Esta disciplina es la que permite que el cuaderno del Hito 1 pueda asumir, sin ambigüedad, que el tablón `dataset_unificado.json` (o, en su versión industrial, `dataset_unificado.parquet`) ya está disponible y saneado estructuralmente en S3.
