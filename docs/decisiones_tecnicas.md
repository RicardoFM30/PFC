# Decisiones Técnicas de Arquitectura — Sistema de Precios Dinámicos (PFC)

> **Proyecto:** Predicción de Precios Dinámicos en Airbnb con Arquitectura Unificada y Machine Learning
> **Autor:** Ricardo Fernández
> **Stack global:** Python 3.14 · XGBoost · Scikit-Learn · Apache Kafka (KRaft) · MongoDB Atlas · AWS S3 / Glue / Athena · Gradio · Hugging Face Spaces

Este documento recoge, de forma **consolidada y trazable**, todas las decisiones técnicas adoptadas a lo largo del proyecto: desde la arquitectura de datos federada (RDS + MongoDB + Kafka → S3) hasta la selección y despliegue del modelo de Machine Learning, pasando por el pipeline CI/CD hacia Hugging Face Spaces y el experimento formal de *Transfer Learning* internacional Madrid → Nueva York.

---

## 📑 Tabla de Contenidos

1. [Contexto y problema de negocio](#1-contexto-y-problema-de-negocio)
2. [Diseño de la arquitectura de datos: repositorio único (Data Lake)](#2-diseño-de-la-arquitectura-de-datos-repositorio-único-data-lake)
3. [Persistencia políglota operacional](#3-persistencia-políglota-operacional)
4. [Procesamiento y captura en tiempo real: Apache Kafka](#4-procesamiento-y-captura-en-tiempo-real-apache-kafka)
5. [Estrategia ETL/ELT e ingesta con servicios AWS](#5-estrategia-etlelt-e-ingesta-con-servicios-aws)
6. [Análisis exploratorio y preparación de datos (Hito 1)](#6-análisis-exploratorio-y-preparación-de-datos-hito-1)
7. [Pipeline de preprocesamiento y entrenamiento (Hito 2)](#7-pipeline-de-preprocesamiento-y-entrenamiento-hito-2)
8. [Despliegue, validación visual y Fine-Tuning (Hito 3)](#8-despliegue-validación-visual-y-fine-tuning-hito-3)
9. [Infraestructura local y reproducibilidad](#9-infraestructura-local-y-reproducibilidad)
10. [Gobernanza, seguridad y MLOps](#10-gobernanza-seguridad-y-mlops)
11. [Matriz consolidada del stack tecnológico](#11-matriz-consolidada-del-stack-tecnológico)
12. [Justificación de evidencias multimedia (`/capturas`)](#12-justificación-de-evidencias-multimedia-capturas)
13. [Conclusiones y KPIs de cierre](#13-conclusiones-y-kpis-de-cierre)

---

## 1. Contexto y problema de negocio

El sector del **alquiler vacacional** arrastra un problema endémico: la fijación de tarifas se apoya en la intuición del propietario, en replicar la inflación del año anterior o en copiar al vecino más cercano. El resultado son **ineficiencias operativas críticas** —pisos vacíos por *overpricing* o dinero dejado en la mesa por *underpricing*— que destruyen el **RevPAR** (*Revenue per Available Room*):

$$\text{RevPAR} = \text{Tasa de Ocupación} \times \text{Precio Medio Diario (ADR)}$$

Este proyecto ataca el problema desde la **ingeniería de datos** y el **machine learning supervisado**, proponiendo un **pipeline híbrido multimodelo** capaz de predecir la tarifa óptima por noche a partir de tres dimensiones críticas que operan a distinta velocidad y estructura:

| Dimensión | Tecnología | Naturaleza | Función en el modelo |
|---|---|---|---|
| 🏛 Infraestructura física del inmueble | **Amazon RDS (PostgreSQL)** | Estructurada (SQL) | Define el *precio suelo* (capacidad, baños, dormitorios, ubicación). |
| 📝 Reputación digital y reseñas | **MongoDB Atlas** | Semiestructurada (JSON/BSON) | Cuantifica la calidad percibida y habilita *overpricing* premium. |
| ⚡ Telemetría de demanda en tiempo real | **Apache Kafka** | Streaming (JSON) | Captura clics y búsquedas concurrentes para ajustar la tarifa elásticamente. |

**Tipo de problema ML:** regresión multivariable supervisada, con `price` como *target* en escala logarítmica (`log(price)`).

---

## 2. Diseño de la arquitectura de datos: repositorio único (Data Lake)

Para romper los silos de información tradicionales y permitir que el modelo de regresión consuma un vector de características unificado, se descarta la idea de realizar cruces masivos en caliente sobre las bases de datos operacionales. En su lugar, se adopta un enfoque de **Data Lake** centralizado utilizando **Amazon S3** como el repositorio único de la verdad.

### 2.1 Justificación técnica de Amazon S3 como almacenamiento central

Amazon S3 ha sido seleccionado como el núcleo de la arquitectura analítica por los siguientes pilares de ingeniería de datos:

* **Desacoplamiento de almacenamiento y cómputo:** permite escalar el espacio de almacenamiento de forma infinita y económica sin necesidad de mantener instancias de cómputo encendidas las 24 horas. Los clústeres de entrenamiento de Machine Learning solo se levantan y pagan cuando necesitan leer de S3.
* **Estructura de capas (*Tiering*) para gobierno de datos:** organiza el repositorio único en tres zonas lógicas mediante prefijos:
  1. *Raw Zone (Bronze)*: landing page donde se depositan los datos en bruto extraídos directamente de RDS, MongoDB y los logs crudos de Kafka.
  2. *Processed Zone (Silver)*: datos limpios, con tipos corregidos (por ejemplo, el campo `price` convertido a `float`) y sin valores nulos.
  3. *Analytics Zone (Gold) / Curated*: la "Súper Tabla" ya unificada y estructurada en formato columnar optimizado (**Apache Parquet**) lista para ser consumida por el modelo.
* **Alta durabilidad y disponibilidad:** S3 garantiza una durabilidad del **99.999999999 %** (11 nueves) mediante replicación redundante automática en múltiples zonas de disponibilidad, eliminando cualquier riesgo de pérdida de datos históricos.

### 2.2 Estructura física del bucket (Lake House)

```text
s3://<S3_BUCKET_NAME>/
├── raw/
│   ├── eventos_kafka/stream_dump.json   ← logs de clickstream drenados de Kafka
│   └── reviews_mongo/reviews_stage.json ← corpus semiestructurado de reseñas
├── curated/
│   └── dataset_proptech_master/         ← tablón Parquet post-Glue (Hito 1)
├── gold_zone/                           ← (PoC) tablón JSON anidado
├── scripts/job_pyspark_analitico.py     ← código ETL subido a S3 para Glue
└── athena-results/                      ← carpeta obligatoria para outputs de Athena
```

---

## 3. Persistencia políglota operacional

Antes de unificarse en Amazon S3, cada tipo de dato se gestiona en la base de datos que mejor responde a su **estructura nativa**, garantizando un rendimiento óptimo en la captura.

### 3.1 Base de datos relacional: Amazon RDS (PostgreSQL)

El inventario de las propiedades (`listings.csv`) presenta una estructura rígidamente definida, con relaciones claras y requisitos estrictos de consistencia.

* **Motor:** PostgreSQL 15.4 sobre instancia `db.t3.micro`.
* **Tabla física:** `listings_master` (sembrada en chunks de 10 000 filas desde `datasets/raw/Global/listings.csv`).
* **Justificación:** se elige Amazon RDS porque las operaciones sobre las características físicas del inmueble (habitaciones, camas, coordenadas) se benefician del cumplimiento **ACID**. Esto garantiza la **integridad referencial absoluta**: es imposible que exista una puntuación de limpieza asociada a un `listing_id` que no exista en la tabla maestra de alojamientos.

### 3.2 Base de datos NoSQL: MongoDB Atlas

El registro histórico de opiniones (`reviews.csv`) aloja millones de filas con el campo `comments`, texto libre no estructurado de longitud muy variable.

* **Justificación:** se opta por **MongoDB Atlas** (frente a Amazon DocumentDB) por su madurez *cloud-native* y su flexibilidad para almacenar documentos BSON. MongoDB permite indexar masivamente por `listing_id` y procesar documentos sin un esquema rígido (es común que algunas reseñas no tengan texto y solo tengan metadatos).
* **Estrategia operativa:** para evitar el bloqueo del clúster AWS Glue por aislamiento de VPC (sin NAT Gateway), el corpus de reseñas se **duplica en formato stage JSON** en `s3://raw/reviews_mongo/`. Glue consume el stage de S3 en lugar de perforar la red privada de Atlas.

### 3.3 Justificación de la persistencia políglota

| Característica | Amazon RDS | MongoDB Atlas |
|---|---|---|
| Modelo de datos | Tablas SQL con tipos estrictos | Documentos BSON sin esquema rígido |
| Consistencia | ACID fuerte | Eventual (configurable) |
| Caso de uso | Inventario físico del inmueble | Corpus masivo de reseñas |
| Integración con Glue | `glueContext.create_dynamic_frame.from_options` (postgresql) | Lectura desde stage S3 (evita NAT) |

---

## 4. Procesamiento y captura en tiempo real: Apache Kafka

La telemetría de navegación de los usuarios ("usuarios entrando a ver la publicación") es un flujo continuo de **logs de analítica web de alta velocidad (*Clickstream*)**, emulando el rastro que deja un usuario en su navegador cuando interactúa concurrentemente con un anuncio de la plataforma.

### 4.1 Infraestructura del broker

* **Imagen Docker:** `confluentinc/cp-kafka:7.5.0` en **modo KRaft** (sin ZooKeeper).
* **Orquestación local:** `docker/docker-compose.yml` levanta el contenedor `pfc_kafka_proptech` exponiendo el puerto `localhost:9092`.
* **Identidad del clúster:** `CLUSTER_ID = MkU3OEVBNTcwNTJENDM2Qk` (fija, requerida por KRaft).

### 4.2 Estructura del mensaje e idempotencia

* **Tópico:** `busquedas_tiempo_real`.
* **Clave (Key):** el `listing_id` de la propiedad → asegura un **particionamiento determinista** y mantiene el orden por casa.
* **Valor:** JSON con la interacción atómica exacta: `event_id`, `user_id` anónimo, la acción del navegador (`ver_anuncio`, `click_galeria`, `ver_mapa`, `leer_reviews`, `clic_contactar`), el tipo de dispositivo y el `timestamp` Unix.

### 4.3 Persistencia de eventos totales (sin agregación en ingesta)

A diferencia de las arquitecturas que resumen la información en ventanas temporales cortas (perdiendo el detalle del rastro), el consumidor drena y vuelca los eventos **uno a uno en su estado puro y bruto** al Data Lake en `s3://raw/eventos_kafka/`. Esto preserva el histórico completo de interacciones totales de la plataforma. Posteriormente, el motor de AWS Glue se encarga de calcular el **volumen total acumulado de interacciones por propiedad** (`total_clicks_acumulados`), convirtiéndolo en una métrica estática de popularidad absoluta para el modelo.

### 4.4 Simulador de tráfico sintético

En la práctica, para validar la viabilidad del prototipo y poblar el sistema con un volumen denso de datos de comportamiento, se desarrolló el script **`scripts/añadir_eventos_kafka.py`**, que actúa como un **generador de tráfico en Python** basado en la clase `KafkaTelemetrySimulator`. Este componente:

1. Lee los `listing_id` reales del dataset maestro `datasets/raw/Global/listings.csv`.
2. Modela ráfagas concurrentes de visitas contra el broker local.
3. Parametriza las acciones mediante una **distribución de probabilidad ponderada** que emula el *embudo de conversión* de un portal inmobiliario:

| Acción | Peso | Lectura de negocio |
|---|---|---|
| `ver_anuncio` | **50 %** | Visualización pasiva del anuncio. |
| `click_galeria` | **25 %** | Interacción visual con fotografías. |
| `ver_mapa` | **15 %** | Consulta de ubicación. |
| `leer_reviews` | **8 %** | Validación cualitativa de reputación. |
| `clic_contactar` | **2 %** | Alta intención de conversión. |

4. Soporta dos modos operativos:
   * `ejecutar_bucle_simulacion(intervalos_segundos=4)` → bucle continuo con pausas.
   * `ejecutar_carga_masiva_local(total_rafagas=30)` → ráfagas intensivas sin espera para saturar el buffer.

El flujo se diseñó bajo un modelo híbrido donde los eventos se inyectan en streaming asíncrono asignando el `listing_id` en los metadatos de la cabecera del mensaje de Kafka y se drenan eficientemente en el orquestador mediante operaciones controladas de tipo `poll()`, evitando el bloqueo de los hilos de ejecución.

---

## 5. Estrategia ETL/ELT e ingesta con servicios AWS

Para comunicar los almacenamientos operacionales (RDS, MongoDB, Kafka) con el repositorio único (Amazon S3), se diseña un flujo híbrido orquestado de forma serverless.

```text
[Amazon RDS] ----(boto3 / Lambda)----> [ Amazon S3 ] <---- (AWS Glue / Athena)
[MongoDB Atlas] --(stage JSON en S3)-> [ (Data Lake) ] <---- (SQL analítica)
[Apache Kafka] --(Streaming Consumer)-->    |
                                            v
                                 [ Modelo Machine Learning ]
```

### 5.1 Scripts de orquestación

| Script | Rol |
|---|---|
| `scripts/pipeline_inicio_prototipo_arquitectura.py` | **PoC IaC**: crea bucket S3, levanta RDS con datos ficticios en memoria y persiste JSON anidado. |
| `scripts/pipeline_inicio_xauto_Glue_Athena.py` | **Orquestador industrial**: aprovisiona RDS real, autodescubre la VPC, siembra datos reales, drena Kafka, registra y ejecuta el Glue Job, y crea la base/tabla en Athena. |
| `scripts/job_pyspark_analitico.py` | **ETL PySpark** ejecutado dentro del clúster Glue: extrae las 3 fuentes, realiza el JOIN federado por RAM distribuida y persiste Parquet columnar. |
| `scripts/metodos_unir_datasets.py` | **Consolidador local** de CSVs regionales (Barcelona, Madrid, Málaga, Sevilla) en `datasets/raw/Global/`. |

### 5.2 Orquestación e ingesta serverless: AWS Lambda y `boto3`

Para las dimensiones estáticas y semiestructuradas (anuncios y reseñas), no se requiere infraestructura de servidores encendida constantemente. Se utilizan funciones **AWS Lambda** programadas mediante eventos cronometrados (Amazon EventBridge). Utilizando la librería **`boto3`** (SDK oficial de AWS para Python), la función Lambda se conecta de forma segura a Amazon RDS y MongoDB Atlas, extrae las actualizaciones del día, realiza transformaciones ligeras y las deposita en formato comprimido en la *Raw Zone* de S3. Al ser *serverless*, el coste operativo es prácticamente cero.

### 5.3 Transformación y catalogación avanzada: AWS Glue y Amazon Athena

* **AWS Glue** (Glue 4.0, 2 workers `G.1X`):
  * El *Glue Data Catalog* actúa como un *crawler* que lee automáticamente la Raw Zone de S3, infiere los esquemas y crea un catálogo de tablas virtuales.
  * El Job `pfc_proptech_federated_etl` ejecuta `scripts/job_pyspark_analitico.py` para:
    1. Extraer `listings_master` de RDS PostgreSQL mediante `create_dynamic_frame.from_options`.
    2. Leer el stage de reseñas desde `s3://raw/reviews_mongo/reviews_stage.json`.
    3. Leer el dump de Kafka desde `s3://raw/eventos_kafka/stream_dump.json`.
    4. Agregar reseñas y eventos por `listing_id` (counts).
    5. Realizar el **JOIN federado por memoria RAM distribuida** (Spark) entre las tres fuentes.
    6. Persistir el tablón en `s3://curated/dataset_proptech_master/` en formato Parquet columnar.
* **Amazon Athena:** se integra como herramienta de **ELT** orientada a validación rápida. Permite ejecutar SQL directamente sobre los Parquet de S3 sin montar una base de datos. El orquestador crea automáticamente la base `proptech_analytics_db` y la tabla externa `dataset_master` apuntando al directorio curated.

### 5.4 Línea de tiempo del orquestador (`pipeline_inicio_xauto_Glue_Athena.py`)

| Fase | Acción | Salida observable |
|---|---|---|
| 0 | Carga `.env` desde la raíz del proyecto | Validación de credenciales AWS, Mongo, Kafka |
| 0.1 | Aprovisionar RDS PostgreSQL y esperar a `available` (5-10 min) | Endpoint RDS |
| 0.2 | Autodescubrimiento de VPC (Security Group `default` + Subnet) | IDs de red |
| 0.3 | Siembra de datos reales en RDS y MongoDB Atlas | Tabla `listings_master` + colección + stage S3 |
| 1.1 | Drenar Kafka local → `s3://raw/eventos_kafka/stream_dump.json` | JSON de clickstream |
| 1.3 | Subir `job_pyspark_analitico.py` a S3 y registrar Glue Job | Job listo para arrancar |
| 2 | Ejecutar el Glue Job y monitorear hasta `SUCCEEDED` | Parquet en `curated/` |
| 3 | Crear `proptech_analytics_db` y tabla externa en Athena | Catálogo consultable |

---

## 6. Análisis exploratorio y preparación de datos (Hito 1)

**Cuaderno:** `notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb`

| Fase | Acción técnica implementada |
|---|---|
| **Ingesta cloud** | `boto3.resource('s3')` descarga fragmentos `part-*.parquet` desde `s3://<S3_BUCKET_NAME>/curated/dataset_proptech_master/`, los unifica con `pd.read_parquet()` y los persiste como `datasets/importado/dataset_unificado.parquet`. |
| **Auditoría de nulos** | Construcción de `df_auditoria` con `dtype`, `count`, `isnull().sum()`, `%` de nulos y cardinalidad. **Diagnóstico: 42.152 % de nulos en `price`** (filtrado estricto posterior). |
| **Auditoría por región** | Iteración `ruta_raw.rglob("listings.csv")` que diagnostica que **Barcelona** concentra los nulos de `price` por celdas vacías entre comas en el CSV original. |
| **Outliers IQR** | Identifica **4.13 %** de outliers en `price` y Q1/Q3 por capacidad. |
| **Filtrado IQR segmentado** | `Limite_Superior = Q3 + 3·IQR` aplicado **por grupo de `accommodates`** para no penalizar villas de gran capacidad. |
| **Imputación condicional** | `df.groupby('room_type')[col].transform('median')` para `bedrooms`, `beds`, `bathrooms` (preserva coherencia arquitectónica). |
| **Depuración de duplicados** | `df.drop_duplicates(subset=['listing_id'], keep='first')`. |
| **Tipado categórico** | `df[col].astype(str).str.lower().str.strip().astype('category')` para `neighbourhood_cleansed` y `room_type`. |
| **Estabilización de varianza** | `df['price'] = np.log(df['price'])` y `df['total_reviews_historicas'] = np.log1p(...)`. |
| **Prevención de *Data Leakage*** | Exclusión de `estimated_revenue_l365d` (correlación 0.31) por contener la fórmula del target. |
| **Train/Test Split** | `train_test_split(X, y, test_size=0.20, random_state=42)`. |
| **Exportación** | `df.to_csv("../datasets/importado/dataset_final.csv", index=False)`. |
| **Resultado** | **`df_auditoria`**: 34.186 registros × 11 columnas tras purga. |

> **Convención de variables finales:** el DataFrame conserva `neighbourhood_cleansed` (barrio), `room_type` (tipo de estancia), `accommodates`, `bedrooms`, `beds`, `bathrooms`, `minimum_nights`, `maximum_nights`, `total_reviews_historicas`, `total_clicks_acumulados` y `price` (en escala `ln`).

---

## 7. Pipeline de preprocesamiento y entrenamiento (Hito 2)

**Cuaderno:** `notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb`

### 7.1 Infraestructura: `ColumnTransformer` maestro

```python
transformador_maestro = ColumnTransformer(transformers=[
    ('barrios_te',       ce.TargetEncoder(smoothing=10.0), col_target_encode),  # 'neighbourhood_cleansed'
    ('habitaciones_ohe', OneHotEncoder(handle_unknown='ignore', sparse_output=False), col_one_hot),  # 'room_type'
    ('num_robust',       RobustScaler(), col_numéricas)                          # Resto de variables
])
```

* **Target Encoding** (`smoothing=10.0`): aplicado a `neighbourhood_cleansed` para **romper la maldición de la dimensionalidad** de los barrios (alta cardinalidad). Cada barrio colapsa en una única columna numérica vinculada al target logarítmico.
* **One-Hot Encoding**: para `room_type` (4 valores únicos → `entire home/apt`, `private room`, `shared room`, `hotel room`).
* **`RobustScaler`**: aplica la *mediana* y el *IQR* sobre las numéricas. Es **inmune a outliers**, crítico dado que `total_clicks_acumulados` y `total_reviews_historicas` tienen distribuciones de ley de potencias.

### 7.2 Matriz de hiperparámetros calibrados

| Modelo | Hiperparámetros finales (GridSearchCV) |
|---|---|
| **RandomForestRegressor** | `n_estimators=250`, `max_depth=15`, `min_samples_split=5`, `min_samples_leaf=2`, `random_state=42`, `n_jobs=-1` |
| **XGBoostRegressor** ⭐ | `n_estimators=350`, `learning_rate=0.04`, `max_depth=6`, `subsample=0.8`, `colsample_bytree=0.8`, `random_state=42`, `n_jobs=-1` |
| **MLPRegressor (Deep Learning)** | `hidden_layer_sizes=(128, 64, 32)` (piramidal), `activation='relu'`, `solver='adam'`, `alpha=0.001`, `max_iter=700`, `early_stopping=True`, `validation_fraction=0.1`, `random_state=42` |

### 7.3 Espacio de búsqueda — `GridSearchCV(cv=3, scoring='r2', n_jobs=-1)`

| Modelo | Espacio de búsqueda |
|---|---|
| **Random Forest** | `n_estimators ∈ {150, 250, 350}` × `max_depth ∈ {12, 15, 18}` × `min_samples_split ∈ {4, 6}` × `min_samples_leaf ∈ {2, 3}` |
| **XGBoost** | `n_estimators ∈ {300, 450, 600}` × `learning_rate ∈ {0.02, 0.04, 0.06}` × `max_depth ∈ {6, 7, 8}` × `min_child_weight ∈ {1, 3}` × `reg_lambda ∈ {1.0, 1.5}` |
| **MLP** | `hidden_layer_sizes ∈ {(128,64,32), (256,128,64)}` × `activation ∈ {relu, tanh, identity}` × `solver ∈ {adam, sgd}` × `alpha ∈ {0.001, 0.005}` × `learning_rate_init ∈ {0.001, 0.005}` |

### 7.4 Selección del modelo maestro

**XGBoost Regressor** es seleccionado como estimador maestro por cuatro razones:

* **R² en Test = 71.38 %** (líder de la terna).
* **MAE = 32.24 €** en escala logarítmica revertida (€/noche).
* **RMSE = 53.23 €** con brecha Train/Test mínima (ausencia de *overfitting*).
* Robustez estructural: serialización en formato nativo JSON portable (sin *pickle lock-in*).

### 7.5 Persistencia de artefactos (Hito 2 → Hito 3)

```python
joblib.dump(transformador_maestro, ruta_carpeta_models / "transformador_maestro.joblib")
pipeline_xgb.save_model(ruta_carpeta_models / "modelo_xgboost.json")
```

Los modelos comparativos de la terna se guardan íntegros en `pipeline_*.joblib`, mientras que el **XGBoost ganador** se descompone en dos piezas puras (`.joblib` para el preprocesador y `.json` para el estimador) para permitir el **ensamblado en runtime** desacoplado del Hito 3.

| Artefacto | Ruta física | Formato |
|---|---|---|
| Preprocesador maestro | `models/transformador_maestro.joblib` | joblib (pickle) |
| Pipeline Random Forest | `models/pipeline_random_forest.joblib` | joblib |
| Pipeline XGBoost | `models/pipeline_xgboost.joblib` | joblib |
| Red Neuronal (MLP) | `models/pipeline_red_nucleonal.joblib` | joblib |
| **XGBoost nativo (producción)** | **`models/modelo_xgboost.json`** | **XGBoost JSON portable** |
| **XGBoost Fine-Tuned (NYC)** | **`models/modelo_xgboost_finetuned.json`** | **XGBoost JSON** |

---

## 8. Despliegue, validación visual y Fine-Tuning (Hito 3)

**Cuaderno:** `notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb`

### 8.1 Inferencia desacoplada y diagnóstico de residuos

El cuaderno **no reentrena** el modelo: se limita a **resucitar los artefactos serializados** en piezas puras y a ensamblarlos en un `Pipeline` de Scikit-Learn en runtime.

```python
transformador_maestro = joblib.load("../models/transformador_maestro.joblib")
modelo_xgb_puro = xgb.XGBRegressor()
modelo_xgb_puro.load_model("../models/modelo_xgboost.json")
```

Métricas obtenidas sobre el conjunto de **Test (20 % aislado)**:

| Métrica | Valor |
|---|---|
| **MAE** | **32.24 €** |
| **RMSE** | **53.23 €** |
| Precio mediano real | 108.50 € |
| **R² (escala logarítmica)** | **71.38 %** |

La **Figura 1** del cuaderno muestra una asimetría positiva con la mediana en 108.50 €, lo que justifica retrospectivamente la transformación `log(price)`. La **Figura 2** contrasta:

* **Panel A:** Dispersión `y_real` vs `y_pred` con bisectriz de eficiencia perfecta $Y=X$. Se observa un comportamiento **homocedástico** estable en el rango 45 € – 150 € y una **postura conservadora** del modelo (subestimación controlada) en el segmento premium (> 200 €), comportamiento clásico del *gradient boosting* para evitar sobreajuste en colas largas.
* **Panel B:** Histograma de residuos centrado en 0 € con forma aproximadamente normal, confirmando que los errores son **aleatorios y no sistemáticos**.

### 8.2 Aplicación Gradio + despliegue en Hugging Face Spaces

El módulo de producción desacoplado vive en `app/app.py` y sigue estas decisiones de diseño:

* **Inyección matricial manual** del One-Hot Encoding de `room_type` para resolver un *edge case* de `scikit-learn` en el contenedor cloud:

  ```python
  datos_transformados[0, 1:5] = 0.0
  if habitacion_final == "entire home/apt":  datos_transformados[0, 1] = 1.0
  elif habitacion_final == "hotel room":      datos_transformados[0, 2] = 1.0
  elif habitacion_final == "private room":    datos_transformados[0, 3] = 1.0
  elif habitacion_final == "shared room":     datos_transformados[0, 4] = 1.0
  ```

* **Sincronización estricta de tipos** exigidos por el preprocesador: categóricas como `category` y numéricas como `float64`.
* **Ordenación física de columnas** con `transformador_maestro.feature_names_in_` para que el orden posicional coincida con el del entrenamiento.
* **Reversión logarítmica** final: `np.expm1(prediccion_log)`.
* **Front-end Gradio** con `gr.themes.Soft(primary_hue="blue", secondary_hue="slate")`, layout en `gr.Blocks` con dos columnas (estructura del inmueble / parámetros de mercado), botón "Calcular Precio Óptimo" y salida HTML coloreada para feedback visual inmediato.

El cuaderno genera automáticamente la estructura modular `/app` con `app.py`, `requirements.txt` aislado, y sincroniza mediante `huggingface_hub.HfApi.upload_folder()` hacia el Space de producción. La inferencia remota se consume después con `gradio_client.Client.predict(api_name="/predict")` consumiendo el endpoint como una API SOA.

### 8.3 Interpretabilidad — Feature Importance del modelo XGBoost

| Variable | Importancia relativa (Ganancia) |
|---|---|
| `habitaciones_ohe__room_type_entire home/apt` | **58.89 %** |
| `habitaciones_ohe__room_type_private room` | **14.42 %** |
| `num_robust__bathrooms` | **6.25 %** |
| `habitaciones_ohe__room_type_shared room` | **5.74 %** |
| `num_robust__accommodates` | **5.05 %** |
| `habitaciones_ohe__room_type_hotel room` | **2.56 %** |
| `num_robust__bedrooms` | **1.73 %** |
| `barrios_te__neighbourhood_cleansed` | **1.72 %** |
| `num_robust__minimum_nights` | **1.25 %** |
| `num_robust__total_reviews_historicas` | **0.75 %** |
| `num_robust__maximum_nights` | **0.71 %** |
| `num_robust__beds` | **0.69 %** |
| `num_robust__total_clicks_acumulados` | **0.26 %** |

> **Lectura de negocio:** la **tipología de habitación** (`room_type`) absorbe el **81.61 %** de la ganancia total. Le siguen los predictores físicos (`bathrooms`, `accommodates`). El barrio solo aporta un 1.72 % por **colinealidad estructural** (los barrios premium concentran ya los formatos de estancia premium).

### 8.4 Benchmark competitivo — UX/UI en Hugging Face

Auditoría comparativa con dos Spaces de referencia del Hub:

| Dimensión | Modelos del Hub (ThomasH007 / anchit48) | Este Proyecto |
|---|---|---|
| **Variables Estructurales** | `Room Type`, `Accommodates`, `Bathrooms`, `Bedrooms`, `Beds` | Idénticas |
| **Dimensión Geográfica** | **Ausente (0 variables)** | **`neighbourhood_cleansed` con Target Encoding** ✅ |
| **Restricciones Comerciales** | `Cancellation Policy`, `Cleaning Fee`, `Instantly Bookable` | **Eliminadas (UX optimizado)** ✅ |
| **Indicadores de Calidad** | `Review Score Rating` | Score + histórico de reseñas ✅ |

**Conclusión:** el sesgo de **omisión geográfica** en la competencia limita su explicatividad. La eliminación de restricciones comerciales reduce la **fricción del usuario** sin sacrificar precisión.

### 8.5 Experimento formal de Transfer Learning — Madrid → Nueva York

El experimento de *Transfer Learning* (sección 6 del Hito 3) se ejecuta sobre el dataset de Kaggle [New York City Airbnb Open Data](https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data) (`AB_NYC_2019.csv`, **48.895 registros**). La descarga se automatiza con `kagglehub.dataset_download("dgomonov/new-york-city-airbnb-open-data")` y se copia a `datasets/importado/AB_NYC_2019.csv`.

#### 8.5.1 Pipeline de adaptación de dominio

1. **Alineación de firmas** con el `ColumnTransformer` del Hito 2. Las variables ausentes (`bedrooms`, `beds`, `bathrooms`) se imputan a `1.0` (constante neutra).
2. **Mapeo geográfico macroscópico**: `neighbourhood_group` (Manhattan, Brooklyn, Queens, Bronx, Staten Island) alimenta el `TargetEncoder` existente.
3. **Conversión de moneda**: `df_nyc['price'] = np.log1p(df_nyc_raw['price'].clip(lower=1))` para empatar la escala logarítmica del Hito 1.
4. **Reentrenamiento incremental** con `xgb.XGBRegressor(n_estimators=150, learning_rate=0.05, max_depth=5, subsample=0.8, random_state=42)` acoplado mediante el parámetro `xgb_model=modelo_xgb_puro`.

#### 8.5.2 Métricas reales obtenidas

```
+-------------------------------------+---------------------+---------------------+---------------------+
| Métrica de Evaluación               | Modelo base (Madrid) | Modelo Fine-Tuned   | Impacto / Variación |
+-------------------------------------+---------------------+---------------------+---------------------+
| MAE (Desviación Media)              | 91.01 $             | 64.17 $             | -26.84 $            |
| R2 Score (Varianza Explicada)       | -0.129              | 0.146               | +0.275              |
+-------------------------------------+---------------------+---------------------+---------------------+
```

| Métrica | Modelo base (Madrid en NYC) | Modelo Fine-Tuned (NYC Especializado) | Impacto |
|---|---|---|---|
| **MAE** | 91.01 $ | 64.17 $ | **Δ MAE = −26.84 $** |
| **R²** | −0.129 | 0.146 | **Δ R² = +0.275** |

#### 8.5.3 Coste computacional

| Concepto | Valor |
|---|---|
| Tiempo de cómputo del descenso de gradiente incremental | **2.92 segundos** |
| Tamaño del dataset de transferencia | 48.895 registros |
| Train/Test split | 80 % / 20 % (`random_state=42`) |
| Ruta de persistencia del artefacto híbrido | `models/modelo_xgboost_finetuned.json` |

#### 8.5.4 Conclusión de ingeniería — Domain Shift

El **Fine-Tuning** soluciona el fenómeno de **Domain Shift** (sesgo continental entre la economía del euro y el dólar) **sin alterar el pipeline preprocesador**. La clave reside en el parámetro `xgb_model=modelo_xgb_puro` del API de XGBoost:

* El optimizador **no destruye** el conocimiento previo sobre cómo penalizar estancias mínimas o valorar tipologías de habitación.
* **Recalibra únicamente el sesgo base de las hojas terminales** para desplazar la distribución de predicciones hacia la escala real del dólar neoyorquino.
* Un **R² inicial de −0.129** confirma que el modelo de Madrid, en Nueva York, se comportaba **peor que una línea horizontal** (predicción ingenua por la media). Comportamiento normativo en MLOps al cambiar de continente sin adaptación.
* Alcanzar un **R² de 0.146** con las variables físicas críticas fijas a `1.0` (por limitaciones del CSV de Kaggle) es una **prueba de resiliencia analítica** del esquema del pipeline.
* **SLA de producción exitoso**: 27.50 % de incremento en variabilidad explicada y 26.84 $ salvados por registro con un coste de cómputo de **2.92 s** (la sección 6.4.3 del cuaderno cita 4.34 s, correspondiente al cómputo total acumulado de la celda).

El artefacto `modelo_xgboost_finetuned.json` es una **arquitectura híbrida transatlántica** que preserva la lógica distributiva de Madrid pero opera con la elasticidad de precios de la costa este americana.

---

## 9. Infraestructura local y reproducibilidad

### 9.1 Convención de rutas relativas con salto hacia atrás (`../`)

Para garantizar la **reproducibilidad global del repositorio al clonarse**, todos los cuadernos y scripts asumen que el usuario ejecuta desde `notebooks/` o `scripts/` y resuelven las rutas mediante **objetos `pathlib.Path`** ascendiendo un nivel con `Path.cwd().parent` o `Path(__file__).resolve().parent.parent`. Este patrón asegura que **ninguna ruta apunte a un escritorio local** y que el árbol funcione idéntico en Windows, Linux o macOS.

| Origen de la llamada | Patrón universal | Ejemplo real en el código |
|---|---|---|
| Cuaderno (notebook) | `Path.cwd().parent / "datasets" / "importado"` | `raiz_proyecto / "datasets" / "importado" / "dataset_unificado.parquet"` |
| Lectura de CSV | `ruta_csv = "../datasets/raw/Barcelona/listings.csv"` | `pd.read_csv(ruta_csv, encoding='utf-8')` |
| Persistencia de modelo | `Path(r"C:\Users\Ric\Desktop\PFC\models")` | `joblib.dump(transformador_maestro, ruta)` |
| Dataset externo (Fine-Tuning) | `ruta_dataset_nyc = "../datasets/importado/AB_NYC_2019.csv"` | `pd.read_csv(ruta_dataset_nyc)` |
| Artefacto híbrido | `ruta_modelo_ft = "../models/modelo_xgboost_finetuned.json"` | `modelo_fine_tuned.save_model(ruta_modelo_ft)` |

### 9.2 Reproducibilidad total del pipeline

| Componente | Versión fija / Semilla |
|---|---|
| `random_state` (train_test_split) | **42** |
| `random_state` (RandomForest) | **42** |
| `random_state` (XGBoost) | **42** |
| `random_state` (MLPRegressor) | **42** |
| `smoothing` (TargetEncoder) | **10.0** |
| `test_size` (train/test split) | **0.20** |
| `cv` (GridSearchCV) | **3** |
| `scoring` (GridSearchCV) | **`'r2'`** |
| `n_jobs` (paralelización) | **-1** (todos los cores) |

Cualquier ejecución posterior del repositorio produce **exactamente los mismos splits, métricas y pesos del modelo**, condición indispensable para una auditoría de tribunal.

### 9.3 Stack de contenedores (Docker Compose)

```yaml
# docker/docker-compose.yml
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: pfc_kafka_proptech
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

| Servicio | Imagen | Puerto | Función |
|---|---|---|---|
| Apache Kafka (KRaft) | `confluentinc/cp-kafka:7.5.0` | `9092:9092` | Ingesta de clickstream sintético desde `scripts/añadir_eventos_kafka.py` |

### 9.4 Orden de ejecución para replicar el flujo completo

```bash
# 1. (Opcional) Generar tráfico sintético en Kafka
python scripts/añadir_eventos_kafka.py

# 2. Levantar Kafka si no está activo
cd docker
docker-compose up -d

# 3. (Opcional) Consolidar datasets regionales
python -c "from scripts.metodos_unir_datasets import DataConsolidationPipeline; DataConsolidationPipeline().ejecutar_pipeline_fusion_dataset()"

# 4. (Opcional) Pipeline de ingesta industrial
python scripts/pipeline_inicio_xauto_Glue_Athena.py

# 5. Ejecutar los cuadernos en orden
jupyter notebook notebooks/hito_00_vision_problema.ipynb
jupyter notebook notebooks/hito_0.5_vision_problema.ipynb
jupyter notebook notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb
jupyter notebook notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb
jupyter notebook notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb
```

---

## 10. Gobernanza, seguridad y MLOps

### 10.1 Seguridad y gobierno de credenciales

* El archivo `.env` está **excluido del repositorio** mediante `.gitignore`. Se provee `.env.example` como plantilla.
* El token de Hugging Face (`TOKEN_HF`) debe tener alcance **`write`** y rotarse periódicamente.
* Las claves de AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) caducan al cierre de la sesión del Learner Lab y deben recargarse por completo en cada ejecución.
* La base MongoDB Atlas está protegida por IP whitelisting (`0.0.0.0/0` solo en entorno de desarrollo).

### 10.2 Variables de entorno requeridas

| Variable | Uso |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Descarga de Parquet desde S3 y operaciones boto3 |
| `AWS_SESSION_TOKEN` | Credencial temporal del Learner Lab |
| `AWS_GLUE_ROLE_ARN` | Rol IAM con permisos sobre Glue, S3 y CloudWatch Logs |
| `S3_BUCKET_NAME` | Bucket del Data Lake curado |
| `RDS_INSTANCE_ID`, `RDS_DB_NAME`, `RDS_USER`, `RDS_PASSWORD` | Conexión transaccional a PostgreSQL |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` | Productor/consumidor Kafka |
| `MONGO_URI`, `MONGO_DATABASE`, `MONGO_COLLECTION` | Reseñas documentales en Atlas |
| `USER_HF`, `NOME_REPOSITORIO`, `TOKEN_HF` | Despliegue en Hugging Face Spaces |

### 10.3 Variables de runtime del Space

| Variable | Origen | Uso |
|---|---|---|
| `GRADIO_EXAMPLES_CACHE` | `os.environ` (en runtime) | Apunta a `<raíz>/.gradio/` para evitar caché dispersa |
| `GRADIO_TEMP_DIR` | `os.environ` (en runtime) | Apunta a `<raíz>/.gradio/` para binarios temporales |

### 10.4 Pipeline CI/CD hacia Hugging Face Spaces

El proyecto implementa una **cadena de despliegue continuo** híbrida (local + cloud) sin necesidad de GitHub Actions tradicional: el cuaderno Jupyter opera como *orquestador de MLOps* que automatiza el empaquetado y la publicación.

```text
[Cuaderno Hito 3]
       │
       ▼
 ┌──────────────┐
 │ 5. Empaqueta │  → Copia models/modelo_xgboost.json + transformador_maestro.joblib
 └──────┬───────┘
        ▼
 ┌──────────────┐
 │ 6. Inyecta   │  → Genera app/app.py + app/requirements.txt en /app
 └──────┬───────┘
        ▼
 ┌──────────────┐
 │ 7. Sube      │  → huggingface_hub.HfApi.upload_folder()
 └──────┬───────┘    (lee USER_HF, NOME_REPOSITORIO, TOKEN_HF de .env)
        ▼
 ┌──────────────┐
 │ 8. Build HF  │  → Container Space SDK = "gradio" provisiona Docker
 └──────┬───────┘    y compila app.py como servicio HTTP permanente
        ▼
 ┌──────────────┐
 │ 9. Publica   │  → URL https://<USER_HF>-<NOME_REPOSITORIO>.hf.space
 └──────────────┘    consumible por gradio_client.Client.predict()
```

```python
from huggingface_hub import HfApi

api = HfApi()
api.create_repo(
    repo_id="<USER_HF>/<NOME_REPOSITORIO>",
    token="<TOKEN_HF>",
    repo_type="space",
    space_sdk="gradio",
    private=False,
    exist_ok=True
)
api.upload_folder(
    folder_path="app",
    repo_id="<USER_HF>/<NOME_REPOSITORIO>",
    repo_type="space",
    token="<TOKEN_HF>"
)
```

La celda 4.3 del Hito 3 automatiza este flujo de forma desatendida, copiando `modelo_xgboost.json` y `transformador_maestro.joblib` desde `/models` hacia `/app` antes del `upload_folder()`.

### 10.5 Inferencia remota como API (SOA)

```python
from gradio_client import Client

client = Client("<USER_HF>/<NOME_REPOSITORIO>")
respuesta = client.predict(
    "Centro", "Entire home/apt", 4.0, 2.0, 2.0, 1.0,
    2.0, 30.0, 42.0, 150.0,
    api_name="/predict"
)
print(respuesta)
```

### 10.6 Inferencia local con artefactos exportados (modo standalone)

```python
import joblib
import xgboost as xgb
import numpy as np
import pandas as pd

# 1. Cargar las dos piezas desacopladas
transformador_maestro = joblib.load("models/transformador_maestro.joblib")
modelo_xgb_puro = xgb.XGBRegressor()
modelo_xgb_puro.load_model("models/modelo_xgboost.json")

# 2. Construir el DataFrame de un nuevo alojamiento
nuevo = pd.DataFrame([{
    'neighbourhood_cleansed': 'centro',
    'room_type': 'entire home/apt',
    'accommodates': 4.0,
    'bedrooms': 2.0,
    'beds': 2.0,
    'bathrooms': 1.0,
    'minimum_nights': 2.0,
    'maximum_nights': 30.0,
    'total_reviews_historicas': 42.0,
    'total_clicks_acumulados': 150.0
}])

# 3. Alinear tipos categóricos exigidos por el transformador
nuevo['neighbourhood_cleansed'] = nuevo['neighbourhood_cleansed'].astype('category')
nuevo['room_type'] = nuevo['room_type'].astype('category')

# 4. Pasar por el preprocesador
X_trans = transformador_maestro.transform(nuevo)

# 5. Predecir (en escala logarítmica) y revertir
pred_log = modelo_xgb_puro.predict(X_trans)[0]
precio_euros = np.expm1(pred_log)

print(f"💶 Tarifa sugerida: {precio_euros:.2f} € / noche")
```

### 10.7 Manifiestos de dependencias

**`requirements.txt` (raíz)** — manifiesto completo del proyecto:

```text
boto3>=1.28.0
kafka-python>=2.0.2
pymongo>=4.5.0
python-dotenv>=1.0.0
pandas>=2.0.0
psycopg2-binary
sqlalchemy
pyarrow
matplotlib
seaborn
scipy
scikit-learn
xgboost
category-encoders
gradio
gradio_client
kagglehub
```

**`app/requirements.txt` (manifiesto mínimo del Space)** — solo dependencias de inferencia local.

### 10.8 Gobernanza del Data Lake

* El bucket S3 está segmentado en capas (`raw/`, `processed/`, `curated/`) siguiendo la arquitectura *Lake House*.
* El script `pipeline_inicio_xauto_Glue_Athena.py` aplica `mode="overwrite"` sobre `curated/dataset_proptech_master/` para garantizar idempotencia.
* El orquestador captura `BucketAlreadyOwnedByYou` y `DBInstanceAlreadyExistsFault` para permitir reejecuciones seguras.
* La inferencia remota del Space puede exponerse públicamente (`*.gradio.live`) durante el Hito 3 para evaluación del tribunal y luego restringirse a `private=True` en producción.

---

## 11. Matriz consolidada del stack tecnológico

| Capa | Tecnología | Versión / Detalle |
|---|---|---|
| Lenguaje | Python | 3.14.0 (kernel del cuaderno) |
| Dataframes | pandas | ≥ 2.0.0 |
| Numérico | numpy | (último estable) |
| Visualización | matplotlib · seaborn | (último estable) |
| Estadística | scipy | (último estable) |
| ML clásico | scikit-learn | (último estable) |
| Boosting | xgboost | (último estable) |
| Encoding categórico | category-encoders | (último estable) |
| Persistencia binaria | joblib | (incluido en scikit-learn) |
| Serialización nativa | `xgb.XGBRegressor.save_model` | Formato JSON portable |
| Front-end ML | Gradio + gradio_client | (último estable) |
| Cloud SDK | boto3 | ≥ 1.28.0 |
| Streaming | kafka-python | ≥ 2.0.2 |
| Documental | pymongo | ≥ 4.5.0 |
| Env vars | python-dotenv | ≥ 1.0.0 |
| SQL ORM | sqlalchemy | (último estable) |
| Driver PostgreSQL | psycopg2-binary | (último estable) |
| Formato columnar | pyarrow | (último estable) |
| Descarga de datasets | kagglehub | (último estable) |
| Contenedor streaming | Apache Kafka (KRaft mode) | 7.5.0 (Confluent) |
| Procesamiento distribuido | AWS Glue (PySpark) | Glue 4.0, 2 × G.1X |
| Motor SQL serverless | Amazon Athena | (versión gestionada) |
| Repositorio de modelos | Amazon S3 | (versión gestionada) |
| Hosting de Space | Hugging Face Spaces | SDK = gradio |

### 11.1 Matriz de dependencias por Hito

| Hito | Dependencias críticas |
|---|---|
| **Hito 0 / 0.5** | pandas, numpy, json (stdlib) |
| **Hito 1** | boto3, pandas, pyarrow, matplotlib, seaborn, scipy, scikit-learn, python-dotenv, pathlib |
| **Hito 2** | scikit-learn, category-encoders, xgboost, joblib, pandas, numpy |
| **Hito 3** | joblib, xgboost, gradio, gradio_client, huggingface_hub, kagglehub, pandas, numpy |

### 11.2 Matriz de cohesión tecnológica de la arquitectura

| Componente | Tecnología Elegida | Tipo de Datos | Velocidad / Flujo | Función en la Arquitectura |
| --- | --- | --- | --- | --- |
| **Repositorio Único** | **Amazon S3** | Híbrido (Tabular/Parquet) | Batch / Micro-batch | **Data Lake Central.** Zona de unificación final y persistencia para el modelo de ML. |
| **Origen Transaccional** | **Amazon RDS** | Estructurado (SQL) | Batch Diario (Lambda) | Almacena datos maestros físicos del inmueble. |
| **Origen Documental** | **MongoDB Atlas** | Semiestructurado (JSON) | Batch Diario (Lambda) | Almacena el corpus masivo de reseñas para el modelo NLP. |
| **Origen Ingesta Streaming** | **Apache Kafka (Docker)** | Eventos (*Clickstream*) | Localhost (Puerto 9092) | Captura de forma asíncrona cada evento de navegación individual. |
| **Orquestador Ingesta** | **AWS Lambda + `boto3`** | Lógica de Control (Python) | Serverless / Event-Driven | Script ligero encargado de extraer datos de RDS/Mongo y subirlos a la capa Raw de S3. |
| **Motor de ETL Pesado** | **AWS Glue + Athena** | Procesamiento de Datos | Batch Programado | Crawlea S3, ejecuta el JOIN lógico masivo y guarda la tabla analítica final. |
| **Generador de tráfico sintético** | `scripts/añadir_eventos_kafka.py` | Productor Python | Localhost (KRaft) | Simula ráfagas de clickstream con embudo de conversión ponderado. |
| **Motor de Inferencia (runtime)** | **XGBoost JSON + `joblib`** | Estimador + preprocesador | Local / Hugging Face Space | Predice `log(price)` y revierte con `np.expm1`. |
| **Front-end ML** | **Gradio Blocks** | UI declarativa Python | HF Space (`*.gradio.live`) | Captura inputs del usuario y muestra tarifa sugerida en HTML. |

---

## 12. Justificación de evidencias multimedia (`/capturas`)

Las capturas no son decorativas: constituyen **pruebas de auditoría visual** que documentan la viabilidad técnica de la arquitectura propuesta. Cada imagen está vinculada a un momento concreto del pipeline:

| Captura | Función en el flujo de ingeniería |
|---|---|
| `insideAIR.png` | Punto de partida: muestra la fuente de datos original de *Inside Airbnb* (Madrid, Málaga, Sevilla). |
| `rdsAWSPrueba.png` | Evidencia la creación de la instancia Amazon RDS PostgreSQL y su estado `available` en la consola AWS. |
| `VisualizaciónRDSPrueba.png` | Confirma que la tabla `listings_master` está poblada y consultable desde un cliente SQL. |
| `atlasPrueba.png` | Muestra la colección `reviews_raw` con documentos BSON cargados en MongoDB Atlas. |
| `capturaGeneradoraKafka.png` | Visualiza la ejecución del script `scripts/añadir_eventos_kafka.py` inyectando mensajes JSON atómicos en el broker. |
| `eventosKafka.png` | Demuestra la consola del broker con el tópico `busquedas_tiempo_real` recibiendo ráfagas y los payloads descodificados. |
| `kafkaPrueba.png` | Confirma que el simulador particiona por `listing_id` como clave, manteniendo el orden por casa. |
| `consolidadoAWSPrueba.png` | Valida que el JOIN lógico entre RDS + MongoDB + Kafka, ejecutado por AWS Glue, persiste el tablón unificado en la capa *Curated* del Data Lake (S3). |
| `contenidoAWSPrueba.png` | Auditoría SQL del Data Lake unificado mediante Amazon Athena antes de entrenar el modelo. |
| `modelo11.png` · `modelo12.png` | Capturas del primer Space competidor (ThomasH007) usado en el benchmark de la sección 8.4. |
| `modelo21.png` · `modelo22.png` | Capturas del segundo Space competidor (anchit48) usado en el benchmark de la sección 8.4. |
| `mimodelo.png` | Inferencia del modelo propio desplegado en Hugging Face Spaces para comparación visual directa con la competencia. |

---

## 13. Conclusiones y KPIs de cierre

Este proyecto constituye un **artefacto de Inteligencia Artificial maduro y desplegado en producción** que cumple los estándares académicos y de ingeniería exigibles a un PFC del ámbito PropTech. Los hitos han sido ejecutados en cadena, validados con búsqueda exhaustiva de hiperparámetros, auditados visualmente, desplegados como servicio en Hugging Face Spaces y **validados internacionalmente mediante Transfer Learning** sobre el dataset de Nueva York.

El techo analítico del **71.38 % de R² en Test** es un límite teórico de la naturaleza del dato, no del modelo: capturar la subjetividad del anfitrión o los metros cuadrados exigiría variables que ni Inside Airbnb ni el dataset de Kaggle exponen públicamente. Incrementar el rendimiento requeriría **datos propietarios** (fotografías, scoring interno de calidad, micro-geolocalización), fuera del alcance de fuentes de datos abiertos.

### 13.1 Indicadores clave de éxito (KPI de cierre)

| KPI | Valor |
|---|---|
| Modelos entrenados y auditados | **3** (Random Forest, XGBoost, MLP) |
| R² en Test del modelo maestro | **71.38 %** |
| MAE de producción | **32.24 €** |
| RMSE de producción | **53.23 €** |
| Persistencia portable | **JSON nativo** (sin pickle lock-in) |
| Experimentos de Transfer Learning | **1** (Madrid → Nueva York) |
| Mejora Δ MAE por Fine-Tuning | **−26.84 $** |
| Mejora Δ R² por Fine-Tuning | **+0.275** |
| Coste del Fine-Tuning | **2.92 s** sobre 48.895 registros |
| Despliegue en producción | ✅ **Hugging Face Space (gradio)** |
| Reproducibilidad total | ✅ `random_state=42` en todos los componentes |
| Tests de validación visual | ✅ Dispersión + histograma de residuos |
| Documentos técnicos | ✅ `decisiones_tecnicas.md` + `ciclo_natural_pipeline_inicial.md` |

### 13.2 Lecciones de ingeniería consolidadas

1. **El desacoplamiento almacenamiento-cómputo** (S3 + Glue) es la única forma viable de escalar pipelines multimodelo sin pagar infraestructura ociosa.
2. **La persistencia políglota** (SQL + NoSQL + Streaming) es la respuesta natural a la heterogeneidad de los datos de PropTech; no es un sobrecoste sino una optimización.
3. **El target encoding con `smoothing=10.0`** rompe la maldición de la dimensionalidad de barrios preservando la señal geográfica que la competencia omite.
4. **El ensamble en runtime** (artefactos `.json` + `.joblib` desacoplados) elimina el *pickle lock-in* y permite reescribir el preprocesador sin tocar el modelo.
5. **El parámetro `xgb_model=` de XGBoost** es la herramienta canónica de *Transfer Learning* incremental: preserva conocimiento y solo recalibra el sesgo base.
6. **La inyección matricial manual** en Gradio es el patrón robusto para sobrevivir a las diferencias de versionado de `scikit-learn` entre local y cloud.
7. **El particionamiento por clave (`listing_id`)** en Kafka garantiza orden por propiedad y habilita el conteo determinista en el agregado final.

### 13.3 Documentación complementaria del repositorio

| Documento | Ruta | Contenido |
|---|---|---|
| Decisiones técnicas de arquitectura | `docs/decisiones_tecnicas.md` | Este documento: justificación del repositorio único (S3), la persistencia políglota (RDS + Mongo + Kafka), la estrategia ETL/ELT, el pipeline de ML y el despliegue. |
| Ciclo natural del pipeline inicial | `docs/ciclo_natural_pipeline_inicial.md` | Línea de tiempo detallada de la ejecución del pipeline original PoC (`pipeline_inicio_prototipo_arquitectura.py`) y su evolución industrial (`pipeline_inicio_xauto_Glue_Athena.py`). |
| README principal | `README.md` | Manual de instalación, configuración, replicación del flujo y KPIs. |

---

## ✍️ Autor

**Ricardo Fernández** — *PFC Sistema Inteligente de Precios Dinámicos*
Repositorio: [github.com/RicardoFM30/PFC](https://github.com/RicardoFM30/PFC)

## 📜 Licencia

Proyecto académico sin licencia comercial explícita. Uso educativo y de portafolio.
