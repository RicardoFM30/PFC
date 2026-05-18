# Proyecto PFC PropTech: Sistema de Precios Dinámicos y Reputación para Alquileres Vacacionales

## Descripción general

Este repositorio contiene el desarrollo del Proyecto de Fin de Carrera (PFC) orientado al diseño e implementación de un sistema empresarial de Machine Learning e Ingeniería de Datos en el sector PropTech. El objetivo central es estimar la tarifa por noche (`price`) óptima para alojamientos turísticos mediante un modelo de regresión supervisada.

El sistema rompe con los silos de datos tradicionales al unificar características físicas del inmueble (infraestructura estática), reputación digital mediante el procesamiento de texto libre de opiniones (NLP) y el comportamiento de la demanda del mercado en tiempo real (streaming).

---

## Arquitectura del proyecto

El diseño se basa en una arquitectura híbrida y políglota multifuente que integra componentes locales y servicios en la nube de AWS:

* **Amazon RDS (PostgreSQL):** Almacenamiento de datos maestros estructurados y transaccionales del inventario de propiedades (`listings`).
* **MongoDB Atlas:** Almacenamiento NoSQL documental para el corpus masivo y semiestructurado de reseñas históricas (`reviews`).
* **Apache Kafka:** Ingestión y procesamiento de flujos de eventos de navegación (telemetría de clics y urgencia de demanda).
* **Amazon S3 (Data Lake):** Repositorio único de la verdad donde se consolidan las fuentes en matrices analíticas listas para el entrenamiento.
* **AWS Glue y Amazon Athena:** Servicios cloud-native para catalogación, procesos ETL masivos y validación relacional mediante SQL.

---

## Estructura del repositorio

A partir de la disposición actual del espacio de trabajo, el árbol del proyecto se estructura de la siguiente forma:

```text
PFC/
├── .vscode/                     # Configuración del entorno de desarrollo en VS Code
├── capturas/                    # Evidencias visuales y gráficas para la memoria del PFC
├── datasets/                    # Almacenamiento local segmentado de datos
│   ├── processed/               # Datasets limpios listos para ingeniería de características
│   └── raw/                     # Datos brutos de Inside Airbnb por ciudades
│       ├── Barcelona/
│       ├── Madrid/
│       ├── Málaga/
│       └── Sevilla/
├── docker/                      # Entornos de contenedores locales
│   └── docker-compose.yml       # Orquestación local de Apache Kafka (KRaft mode)
├── docs/                        # Documentación técnica del proyecto
│   └── decisiones_tecnicas.md   # Registro de justificaciones de arquitectura y diseño
├── models/                      # Almacenamiento de artefactos y pesos de los modelos entrenados
├── notebooks/                   # Cuadernos interactivos de desarrollo y análisis
│   └── hito_00_vision_problema.ipynb  # Entregable teórico y estrategia de variables
├── scripts/                     # Scripts de automatización y pipelines de producción
│   └── pipeline_inicio.py       # Pipeline unificado de despliegue IaC, ingesta y unificación
├── venv/                        # Entorno virtual aislado de Python
├── .env                         # Variables de entorno y credenciales privadas (Ignorado en Git)
├── .env.example                 # Plantilla de configuración de variables para despliegue
├── .gitignore                   # Exclusiones de control de versiones
├── README.md                    # Documentación principal del repositorio
└── requirements.txt             # Dependencias y librerías del proyecto organizadas por versiones

```

---

## Estado actual del proyecto (Hito 0)

* **Modelado analítico:** Hito 0 completado y documentado formalmente en el cuaderno interactivo `notebooks/hito_00_vision_problema.ipynb`.
* **Infraestructura automatizada:** Script unificado de despliegue (`scripts/pipeline_inicio.py`) configurado en modo **Infraestructura como Código (IaC)**. Es capaz de aprovisionar buckets S3, levantar instancias RDS reales en AWS, interactuar con el Security Group del Firewall y cruzar datos multifuente de forma idempotente.

---

## Guía de lanzamiento y ejecución

Sigue estos pasos para preparar el entorno virtual y lanzar todo el ecosistema de datos del proyecto:

### 1. Prerrequisitos del sistema

Asegúrate de contar con las siguientes herramientas activas en tu máquina local:

* **Python 3.9 o superior** (Entorno de desarrollo testeado en Python 3.14).
* **Docker Desktop** (Con el daemon encendido).

### 2. Configuración del entorno virtual

Navega en tu terminal hasta la raíz del proyecto (`PFC/`) y ejecuta los siguientes comandos para aislar e instalar las dependencias:

```bash
# 1. Crear el entorno virtual
python -m venv venv

# 2. Activar el entorno virtual
# En Windows (CMD / PowerShell):
.\venv\Scripts\activate
# En Mac / Linux / Git Bash:
source venv/bin/activate

# 3. Actualizar pip e instalar dependencias requeridas
pip install --upgrade pip
pip install -r requirements.txt

```

### 3. Configuración de variables de entorno (.env)

Crea un archivo llamado exactamente **`.env`** en la raíz del proyecto basándote en la plantilla `.env.example`. Rellena los campos con tus credenciales temporales de AWS Learner Lab y tu clúster de MongoDB Atlas:

```env
# Credenciales y configuración AWS (Temporales de Learner Lab)
AWS_ACCESS_KEY_ID=ASIARU5IC4M7...
AWS_SECRET_ACCESS_KEY=r5go4cQ+uaE3...
AWS_SESSION_TOKEN=IQoJb3JpZ2luX2VjEOb..........
AWS_REGION=us-east-1

# Configuración S3 (Data Lake)
S3_BUCKET_NAME=pfc-data-lake-proptech-ricardo

# Configuración para que AWS cree de forma dinámica el RDS
RDS_INSTANCE_ID=servidor-pfc-poc
RDS_DB_NAME=proptechdb
RDS_USER=postgres
RDS_PASSWORD=UnaPasswordSegura123!

# Configuración Apache Kafka Local
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=busquedas_tiempo_real

# Configuración MongoDB Atlas
MONGO_URI=mongodb+srv://usuario:contraseña@cluster.mongodb.net/?appName=Cluster
MONGO_DATABASE=pfc_proptech
MONGO_COLLECTION=reviews

```

> ⚠️ **Nota de AWS Learner Lab:** Las credenciales de la suite de estudiantes caducan periódicamente. Recuerda actualizar los campos `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` y `AWS_SESSION_TOKEN` en tu `.env` cada vez que reinicies el laboratorio en la consola web.

### 4. Lanzamiento del pipeline completo

Con tu entorno virtual activado y Docker Desktop en ejecución, lanza el script maestro:

```bash
python scripts/pipeline_inicio.py

```

---

## Comportamiento automatizado por capas

Al ejecutar el pipeline, el script procesa de forma defensiva y asíncrona la automatización de la infraestructura:

1. **Orquestación de Contenedores (Kafka):** El script valida la disponibilidad del puerto de Kafka. Si no responde, ejecuta un subproceso de Docker Compose sobre `docker/docker-compose.yml` para levantar el broker en modo KRaft, aplicando un bucle de espera inteligente (*polling*) hasta que esté listo.
2. **Aprovisionamiento de Data Lake (S3):** Conéctandose mediante el SDK `boto3`, verifica la existencia del bucket configurado. Si no existe, lo crea dinámicamente en la región establecida.
3. **Despliegue de Infraestructura como Código (Amazon RDS):** El script ordena a AWS la creación de una instancia de base de datos relacional PostgreSQL de tipo `db.t3.micro`. El código detiene la ejecución de forma segura mediante un *waiter* activo de `boto3` hasta que el servidor pasa de estado *Creating* a *Available* (Aproximadamente 5-10 minutos).
4. **Apertura del Firewall de AWS (Security Groups):** Una vez que RDS tiene IP pública, el script localiza de forma automática su *VPC Security Group* asignado e inyecta una regla de entrada de red para liberar el puerto TCP `5432` de forma remota.
5. **Poblado de Datos e Ingesta:** El script inicializa el esquema DDL en RDS (tabla `listings`), realiza una limpieza por idempotencia en MongoDB Atlas e inyecta ráfagas de datos ficticios en Kafka, Mongo y RDS de manera coordinada.
6. **Consolidación y Carga (JOIN Híbrido):** Extrae la información de las tres fuentes, realiza un acoplamiento estructurado en memoria usando la clave simétrica `listing_id` y sube la matriz resultante en formato JSON directamente a la zona de almacenamiento de S3.

---

## Verificación del éxito de la PoC

Sabrás que el pipeline se ha completado correctamente al visualizar el log de cierre en tu terminal:
`✔ Pipeline e Infraestructura completados con éxito.`

Para auditar de forma independiente que los datos se han distribuido y persistido correctamente en cada tecnología, puedes seguir estos pasos de verificación:

### A. Verificación en Amazon RDS (Datos Físicos Estructurados)

El script imprimirá en la terminal el host público asignado por AWS (ej. `servidor-pfc-poc.cqjscsd5i2xf.us-east-1.rds.amazonaws.com`).

* Abre tu gestor de base de datos preferido (DBeaver, pgAdmin o extensiones de VS Code).
* Crea una nueva conexión para **PostgreSQL** usando el Host impreso, puerto `5432`, base de datos `proptechdb`, usuario `postgres` y la contraseña de tu archivo `.env`.
* Ejecuta la consulta: `SELECT * FROM listings;` para verificar la existencia de los registros.

### B. Verificación en MongoDB Atlas (Datos Cualitativos / Texto Libre)

* Inicia sesión en la consola web de [MongoDB Atlas](https://cloud.mongodb.com/).
* Navega a **Database** -> **Browse Collections** dentro de tu clúster activo.
* Comprueba que se ha generado la base de datos establecida en tu `.env` y que la colección de reseñas contiene los documentos JSON de prueba con sus respectivos campos.

### C. Verificación en Amazon S3 (Data Lake / Súper Tabla Analítica)

* Inicia sesión en la consola web de AWS y entra al servicio de **S3**.
* Accede al bucket configurado (`pfc-data-lake-proptech-ricardo`).
* Navega al directorio **`gold_zone/`**. Allí encontrarás el archivo unificado **`dataset_unificado.json`**. Este objeto consolida la información de las tres infraestructuras y representa el punto de entrada directo para el entrenamiento de los algoritmos de regresión de Machine Learning.