# Proyecto PFC PropTech: Sistema de Precios Dinámicos y Reputación para Alquileres Vacacionales

## Descripción general

Este repositorio contiene el desarrollo de un proyecto de fin de curso para un sistema empresarial de Machine Learning e Ingeniería de Datos en el sector PropTech. El objetivo es estimar el `precio_noche` óptimo para alquileres vacacionales mediante un modelo de regresión supervisada que combina datos físicos del inmueble, reseñas cualitativas y métricas de demanda en tiempo real.

## Arquitectura del proyecto

El diseño del proyecto se basa en una arquitectura híbrida multifuente que integra:

- Amazon RDS para datos estructurados y transaccionales.
- MongoDB Atlas para almacenamiento de reseñas y datos semiestructurados.
- Apache Kafka para ingestión de eventos de búsqueda y métricas dinámicas.
- Amazon S3 como Data Lake central.
- AWS Glue y Amazon Athena para procesos ELT y análisis.
- Hugging Face Hub para fine-tuning de modelos NLP.
- Gradio para la interfaz interactiva de usuario.

## Estructura del repositorio

```text
PFC/
├── datasets/
│   ├── raw/
│   └── processed/
├── notebooks/
│   └── hito_00_vision_problema.ipynb
├── scripts/
│   └── pipeline_inicio.py
├── models/
├── docs/
│   ├── arquitectura.md
│   └── decisiones_tecnicas.md
├── requirements.txt
├── .gitignore
├── .env.example
└── README.md

```

## Estado actual del proyecto

* Hito 0 iniciado y entregable teórico generado en `notebooks/hito_00_vision_problema.ipynb`.
* Justificación técnica de la arquitectura multifuente registrada en `docs/decisiones_tecnicas.md`.
* Script base de pipeline `scripts/pipeline_inicio.py` implementado con despliegue simulado, ingesta y consolidación hacia S3.

## Guía de lanzamiento y ejecución

Sigue estos pasos para preparar el entorno virtual y ejecutar el pipeline automatizado del Hito 0 en un equipo completamente limpio:

### 1. Prerrequisitos del sistema

Antes de arrancar, asegúrate de tener instalado en tu máquina local:

* **Python 3.9 o superior**.
* **Docker**

### 2. Configuración del entorno virtual

Abre tu terminal, navega hasta la carpeta raíz del proyecto (`PFC/`) y ejecuta secuencialmente los siguientes comandos:

```bash
# 1. Crear el entorno virtual de Python
python -m venv venv

# 2. Activar el entorno virtual
# En Windows (PowerShell / CMD):
.\venv\Scripts\activate
# En Mac / Linux / Git Bash:
source venv/bin/activate

# 3. Instalar las dependencias del proyecto
pip install -r requirements.txt

```

### 3. Configuración de variables de entorno

Duplica el archivo `.env.example`, renómbralo exactamente a **`.env`** en la raíz del proyecto y rellena los parámetros con tus credenciales reales de AWS y MongoDB Atlas:

```env
AWS_REGION=us-east-1
S3_BUCKET_NAME=tu-bucket-data-lake-proptech  # Debe ser un nombre global único en AWS
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=busquedas_tiempo_real
MONGO_URI=mongodb+srv://tu_usuario:tu_password@cluster.mongodb.net/
MONGO_DATABASE=pfc_proptech
MONGO_COLLECTION=reviews

```

### 4. Lanzamiento del pipeline

Con el entorno virtual activado y Docker Desktop encendido, ejecuta el script unificado:

```bash
python scripts/pipeline_inicio.py

```

### 5. Comportamiento automatizado (¿Qué sucede por detrás?)

El pipeline ejecutará las siguientes fases de forma inteligente y defensiva:

1. **Verificación de Kafka:** El script intentará comunicarse con el broker. Al detectar que está apagado, invocará a Docker de manera interna ejecutando `docker compose up -d` sobre el archivo de configuración alojado en `docker/docker-compose.yml`.
2. **Bucle de espera inteligente (Polling):** El script iniciará consultas de control cada 5 segundos (con un límite máximo de 60 segundos) dándole margen al contenedor para descargar la imagen e inicializar la arquitectura Kafka KRaft de forma segura. Avanzará en el instante exacto en el que Kafka responda de forma positiva.
3. **Despliegue e Ingesta:** Se creará automáticamente tu bucket en AWS S3 y el tópico en Kafka. Acto seguido, se insertarán los datos de la simulación en RDS, MongoDB Atlas y el stream en tiempo real de Kafka de forma idempotente (borrando ejecuciones previas para evitar duplicados).
4. **Consolidación:** Se extraerán los datos distribuidos, se realizará el `JOIN` lógico en memoria utilizando el identificador común de la vivienda y se subirá el archivo maestro consolidado directamente a tu Data Lake en S3.

### 6. Verificación del éxito

Sabrás que todo ha funcionado correctamente si visualizas el mensaje de cierre en la terminal:
`✔ Pipeline completado. Datos reales extraídos, transformados y cargados.`

Para realizar una auditoría completa y comprobar físicamente que los datos han llegado a su destino en cada herramienta, sigue estas rutas:

**1. Verificación en MongoDB Atlas (Datos cualitativos / Texto libre):**
- Inicia sesión en tu panel web de [MongoDB Atlas](https://cloud.mongodb.com/).
- En el menú lateral izquierdo, ve a **Database** y haz clic en el botón **Browse Collections** de tu clúster.
- Busca la base de datos `pfc_proptech`. Al desplegarla, entra en la colección `reviews`. Ahí podrás ver todos los documentos BSON/JSON insertados con los comentarios y puntuaciones.

**2. Verificación en Apache Kafka (Eventos en Streaming):**
- Dado que Kafka está encapsulado en tu contenedor Docker local, puedes "escuchar" el canal abriendo una nueva ventana en tu terminal y ejecutando este comando:
  ```bash
  docker exec -it pfc_kafka_proptech kafka-console-consumer --bootstrap-server localhost:9092 --topic busquedas_tiempo_real --from-beginning```
- Verás aparecer en tu consola la ráfaga de eventos JSON que simulan las búsquedas. (Pulsa Ctrl + C para salir del modo lectura).

**3. Verificación en Amazon S3 (Data Lake / Dataset unificado):**

- Inicia sesión en tu consola de AWS y dirígete al servicio S3.
- Entra en el bucket que configuraste en tu .env.
- Navega a la carpeta consolidated/. Encontrarás el dataset maestro unificado (proptech_dataset_real.json o proptech_dataset.json), listo para ser consumido en las siguientes fases de Machine Learning.