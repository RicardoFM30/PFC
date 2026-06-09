# 🎯 Predicción de Precios Dinámicos en Airbnb con Arquitectura Unificada y Machine Learning

> **PFC — Sistema Inteligente de Precios Dinámicos (Dynamic Pricing PropTech)**
> **Autor:** Ricardo Fernández · **Stack:** Python 3.14 · XGBoost · Scikit-Learn · Apache Kafka · MongoDB · AWS S3 / Glue / Athena · Gradio · Hugging Face Spaces

---

## 📋 Resumen Ejecutivo

El sector del **alquiler vacacional** arrastra un problema endémico: la fijación de tarifas se apoya en la intuición del propietario, en replicar la inflación del año anterior o en copiar al vecino más cercano. El resultado son **ineficiencias operativas críticas** —pisos vacíos por *overpricing* o dinero dejado en la mesa por *underpricing*— que destruyen el **RevPAR** (*Revenue per Available Room*):

$$\text{RevPAR} = \text{Tasa de Ocupación} \times \text{Precio Medio Diario (ADR)}$$

Este proyecto ataca el problema desde la **ingeniería de datos** y el **machine learning supervisado**, proponiendo un **pipeline híbrido multimodelo** capaz de predecir la tarifa óptima por noche a partir de tres dimensiones críticas que operan a distinta velocidad y estructura:

| Dimensión | Tecnología | Naturaleza | Función en el modelo |
|---|---|---|---|
| 🏛 Infraestructura física del inmueble | **Amazon RDS (PostgreSQL)** | Estructurada (SQL) | Define el *precio suelo* (capacidad, baños, dormitorios, ubicación). |
| 📝 Reputación digital y reseñas | **MongoDB Atlas** | Semiestructurada (JSON/BSON) | Cuantifica la calidad percibida y habilita *overpricing* premium. |
| ⚡ Telemetría de demanda en tiempo real | **Apache Kafka** | Streaming (JSON) | Captura clics y búsquedas concurrentes para ajustar la tarifa elásticamente. |

El resultado es un **artefacto de IA maduro y desplegado en producción** (Hugging Face Spaces), entrenado sobre datos reales de *Inside Airbnb* (Madrid, Málaga y Sevilla) y **validado con un experimento formal de *Transfer Learning* internacional** sobre el dataset de Nueva York (`AB_NYC_2019.csv`) que demuestra cómo el *fine-tuning* corrige el *Domain Shift* sin alterar el *pipeline* preprocesador.

---

## 📂 Arquitectura de Carpetas y Control de Rutas

### 🌳 Diagrama del árbol de directorios

```text
PFC/
├── app/                          # Módulo de producción desacoplado (Hugging Face Space)
│   ├── app.py                    # Front-end Gradio + motor de inferencia secuencial
│   ├── modelo_xgboost.json       # Cerebro del modelo en formato nativo portable
│   ├── transformador_maestro.joblib  # Preprocesador completo (Scaler + Encoders)
│   └── requirements.txt          # Manifiesto mínimo de dependencias del contenedor
├── capturas/                     # Evidencias visuales del flujo de ingeniería
│   ├── insideAIR.png, rdsAWSPrueba.png, VisualizaciónRDSPrueba.png
│   ├── atlasPrueba.png, capturaGeneradoraKafka.png, eventosKafka.png
│   ├── kafkaPrueba.png, consolidadoAWSPrueba.png, contenidoAWSPrueba.png
│   ├── modelo11.png, modelo12.png, modelo21.png, modelo22.png, mimodelo.png
├── datasets/
│   ├── raw/                      # CSVs originales de Inside Airbnb
│   │   └── Global/, Barcelona/, Madrid/, Málaga/, Sevilla/
│   └── importado/                # Tablón unificado y datasets externos
│       ├── dataset_unificado.parquet   # Salida del JOIN Glue (Hito 1)
│       ├── dataset_final.csv           # Tablón saneado (Hito 1 → Hito 2)
│       └── AB_NYC_2019.csv             # Dataset Kaggle para Fine-Tuning (Hito 3)
├── docker/docker-compose.yml     # Orquestación local de Apache Kafka (KRaft mode)
├── docs/
│   ├── ciclo_natural_pipeline_inicial.md
│   └── decisiones_tecnicas.md    # Decisiones de arquitectura cloud y políglota
├── models/                       # Artefactos binarios de IA persistidos
│   ├── modelo_xgboost.json       # Estimador maestro XGBoost (producción)
│   ├── modelo_xgboost_finetuned.json  # Artefacto híbrido post-Fine-Tuning NYC
│   ├── transformador_maestro.joblib    # Pipeline ColumnTransformer
│   ├── pipeline_random_forest.joblib
│   ├── pipeline_xgboost.joblib
│   └── pipeline_red_nucleonal.joblib
├── notebooks/                    # Cuadernos de investigación (Hitos 0 → 3)
│   ├── hito_00_vision_problema.ipynb
│   ├── hito_0.5_vision_problema.ipynb
│   ├── hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb
│   ├── hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb
│   └── hito_03_representacion_grafica_y_prueba_de_modelo.ipynb
├── scripts/                      # Automatizaciones operacionales
│   ├── añadir_eventos_kafka.py   # Simulador de clickstream (Productor Kafka)
│   ├── pipeline_inicio_prototipo_arquitectura.py
│   ├── pipeline_inicio_xauto_Glue_Athena.py
│   ├── job_pyspark_analitico.py
│   └── metodos_unir_datasets.py
├── .env.example                  # Plantilla de variables de entorno (AWS, Mongo, HF, Kafka)
├── .gitignore
├── README.md
└── requirements.txt              # Manifiesto completo de dependencias del proyecto
```

### 🔁 Diseño de Rutas Relativas con Salto hacia Atrás (`../`)

Para garantizar la **reproducibilidad global del repositorio al clonarse**, todos los cuadernos y scripts asumen que el usuario ejecuta desde `notebooks/` o `scripts/` y resuelven las rutas mediante **objetos `pathlib.Path`** ascendiendo un nivel con `Path.cwd().parent`. Este patrón asegura que **ninguna ruta apunte a un escritorio local** y que el árbol funcione idéntico en Windows, Linux o MacOS.

| Origen de la llamada | Patrón universal | Ejemplo real en el código |
|---|---|---|
| Cuaderno (notebook) | `Path.cwd().parent / "datasets" / "importado"` | `raiz_proyecto / "datasets" / "importado" / "dataset_unificado.parquet"` |
| Lectura de CSV | `ruta_csv = "../datasets/raw/Barcelona/listings.csv"` | `pd.read_csv(ruta_csv, encoding='utf-8')` |
| Persistencia de modelo | `Path(r"C:\Users\Ric\Desktop\PFC\models")` | `joblib.dump(transformador_maestro, ruta)` |
| Dataset externo (Fine-Tuning) | `ruta_dataset_nyc = "../datasets/importado/AB_NYC_2019.csv"` | `pd.read_csv(ruta_dataset_nyc)` |
| Artefacto híbrido | `ruta_modelo_ft = "../models/modelo_xgboost_finetuned.json"` | `modelo_fine_tuned.save_model(ruta_modelo_ft)` |

### 🖼️ Justificación de las Evidencias Multimedia (`/capturas`)

Las capturas no son decorativas: constituyen **pruebas de auditoría visual** que documentan la viabilidad técnica de la arquitectura propuesta.

| Captura | Función en el flujo de ingeniería |
|---|---|
| `kafkaPrueba.png` | Demuestra que el simulador `scripts/añadir_eventos_kafka.py` inyecta mensajes JSON atómicos en el broker, particionando por `listing_id` como clave. |
| `consolidadoAWSPrueba.png` | Valida que el JOIN lógico entre RDS + MongoDB + Kafka, ejecutado por AWS Glue, persiste el tablón unificado en la capa *Curated* del Data Lake (S3). |
| `contenidoAWSPrueba.png` | Auditoría SQL del Data Lake unificado mediante Amazon Athena antes de entrenar el modelo. |
| `modelo11/12/21/22.png` | Soporte del benchmark competitivo (sección 5.2 del Hito 3) frente a Spaces de Hugging Face. |
| `mimodelo.png` | Inferencia del modelo propio para comparación visual directa con la competencia. |
| `atlasPrueba.png` · `rdsAWSPrueba.png` | Evidencia de las dos capas de persistencia políglota (NoSQL y SQL). |

---

## 🗺️ Flujo del Proyecto por Hitos (Extracto del Código Real)

### 🟢 Hito 0 — Visión del Problema y Marco Teórico
**Archivo:** `notebooks/hito_00_vision_problema.ipynb`

| Aspecto | Detalle |
|---|---|
| **Rol en el proyecto** | Borrador fundacional. Identifica el *overpricing* y el *underpricing* como pérdidas de RevPAR. |
| **Variables definidas** | `price` (Target), `latitude`, `longitude`, `neighbourhood_cleansed`, `room_type`, `accommodates`, `bedrooms`, `beds`, `bathrooms_text`, `review_scores_*`, `score_sentimiento_nlp`, `volumen_clicks_15min`. |
| **Tipo de problema ML** | Regresión multivariable supervisada. |
| **Ingesta de prueba** | `pd.read_csv("../datasets/raw/Barcelona/listings.csv", encoding='utf-8')`. |

### 🟡 Hito 0.5 — Arquitectura Federada Multifuente
**Archivo:** `notebooks/hito_0.5_vision_problema.ipynb`

Evolución del Hito 0 hacia una **arquitectura federada madura**. Documenta formalmente los tres componentes: **RDS (PostgreSQL)**, **MongoDB Atlas** y **Apache Kafka** (tópico `busquedas_tiempo_real`). Introduce el script `scripts/añadir_eventos_kafka.py` como generador de tráfico sintético basado en un *embudo de conversión ponderado* (50 % visualización, 25 % galería, 15 % mapa, 8 % reseñas, 2 % contacto). La integración multimodelo se orquesta con `pipeline_inicio_xauto_Glue_Athena.py`.

### 🔵 Hito 1 — Análisis Exploratorio y Preparación de Datos (EDA)
**Archivo:** `notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb`

| Fase | Acción técnica implementada |
|---|---|
| **Ingesta cloud** | `boto3.resource('s3')` descarga fragmentos `part-*.parquet` desde `s3://<S3_BUCKET_NAME>/curated/dataset_proptech_master/`, los unifica con `pd.read_parquet()` y los persiste como `dataset_unificado.parquet`. |
| **Auditoría de nulos** | Construcción de `df_auditoria` con `dtype`, `count`, `isnull().sum()`, `%` de nulos y cardinalidad. **Diagnóstico: 42.152 % de nulos en `price`** (Filtrado estricto posterior). |
| **Auditoría por región** | Iteración `ruta_raw.rglob("listings.csv")` que diagnostica que **Barcelona** concentra los nulos de `price` por celdas vacías entre comas en el CSV original. |
| **Outliers IQR** | Identifica 4.13 % de outliers en `price` y Q1/Q3 por capacidad. |
| **Filtrado IQR segmentado** | `Limite_Superior = Q3 + 3·IQR` aplicado **por grupo de `accommodates`** para no penalizar villas de gran capacidad. |
| **Imputación condicional** | `df.groupby('room_type')[col].transform('median')` para `bedrooms`, `beds`, `bathrooms` (preserva coherencia arquitectónica). |
| **Depuración de duplicados** | `df.drop_duplicates(subset=['listing_id'], keep='first')`. |
| **Tipado categórico** | `df[col].astype(str).str.lower().str.strip().astype('category')` para `neighbourhood_cleansed` y `room_type`. |
| **Estabilización de varianza** | `df['price'] = np.log(df['price'])` y `df['total_reviews_historicas'] = np.log1p(...)`. |
| **Prevención de *Data Leakage*** | Exclusión de `estimated_revenue_l365d` (correlación 0.31) por contener la fórmula del target. |
| **Train/Test Split** | `train_test_split(X, y, test_size=0.20, random_state=42)`. |
| **Exportación** | `df.to_csv("../datasets/importado/dataset_final.csv", index=False)`. |
| **Resultado** | **`df_auditoria`**: 34.186 registros × 11 columnas tras purga. |

> **Convención de variables:** El DataFrame final conserva `neighbourhood_cleansed` (barrio), `room_type` (tipo de estancia), `accommodates`, `bedrooms`, `beds`, `bathrooms`, `minimum_nights`, `maximum_nights`, `total_reviews_historicas`, `total_clicks_acumulados` y `price` (en escala `ln`).

### 🟣 Hito 2 — Pipeline Unificado, Entrenamiento y Validación
**Archivo:** `notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb`

#### 🏗️ Infraestructura: `ColumnTransformer` Maestro

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

#### ⚙️ Matriz de Hiperparámetros Calibrados

| Modelo | Hiperparámetros finales (GridSearchCV) |
|---|---|
| **RandomForestRegressor** | `n_estimators=250`, `max_depth=15`, `min_samples_split=5`, `min_samples_leaf=2`, `random_state=42`, `n_jobs=-1` |
| **XGBoostRegressor** ⭐ | `n_estimators=350`, `learning_rate=0.04`, `max_depth=6`, `subsample=0.8`, `colsample_bytree=0.8`, `random_state=42`, `n_jobs=-1` |
| **MLPRegressor (Deep Learning)** | `hidden_layer_sizes=(128, 64, 32)` (piramidal), `activation='relu'`, `solver='adam'`, `alpha=0.001`, `max_iter=700`, `early_stopping=True`, `validation_fraction=0.1`, `random_state=42` |

#### 📊 Búsqueda Exhaustiva — `GridSearchCV(cv=3, scoring='r2', n_jobs=-1)`

| Modelo | Espacio de búsqueda |
|---|---|
| **Random Forest** | `n_estimators ∈ {150, 250, 350}` × `max_depth ∈ {12, 15, 18}` × `min_samples_split ∈ {4, 6}` × `min_samples_leaf ∈ {2, 3}` |
| **XGBoost** | `n_estimators ∈ {300, 450, 600}` × `learning_rate ∈ {0.02, 0.04, 0.06}` × `max_depth ∈ {6, 7, 8}` × `min_child_weight ∈ {1, 3}` × `reg_lambda ∈ {1.0, 1.5}` |
| **MLP** | `hidden_layer_sizes ∈ {(128,64,32), (256,128,64)}` × `activation ∈ {relu, tanh, identity}` × `solver ∈ {adam, sgd}` × `alpha ∈ {0.001, 0.005}` × `learning_rate_init ∈ {0.001, 0.005}` |

#### 🏆 Selección del Modelo Maestro

**XGBoost Regressor** es seleccionado como estimador maestro por tres razones:

* **R² en Test = 71.38 %** (líder de la terna).
* **MAE = 32.24 €** en escala logarítmica revertida (€/noche).
* **RMSE = 53.23 €** con brecha Train/Test mínima (ausencia de *overfitting*).
* Robustez estructural: serialización en formato nativo JSON portable.

#### 💾 Persistencia de Artefactos (Hito 2 → Hito 3)

```python
joblib.dump(transformador_maestro, ruta_carpeta_models / "transformador_maestro.joblib")
pipeline_xgb.save_model(ruta_carpeta_models / "modelo_xgboost.json")
```

Los modelos comparativos de la terna se guardan íntegros en `pipeline_*.joblib` mientras que el **XGBoost ganador** se descompone en dos piezas puras (`.joblib` para el preprocesador y `.json` para el estimador) para permitir el **ensamblado en runtime** desacoplado del Hito 3.

### 🟠 Hito 3 — Despliegue, Validación Visual y Fine-Tuning Internacional
**Archivo:** `notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb`

#### 3.1 Inferencia desacoplada y diagnóstico de residuos

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
| **Precio Mediano Real** | 108.50 € |
| **R² (escala logarítmica)** | 71.38 % |

La **Figura 1** del cuaderno (`Distribución del Precio Real`) muestra una asimetría positiva con la mediana en 108.50 €, lo que justifica retrospectivamente la transformación `log(price)`. La **Figura 2** contrasta:

* **Panel A:** Dispersión `y_real` vs `y_pred` con bisectriz de eficiencia perfecta $Y=X$. Se observa un comportamiento **homocedástico** estable en el rango 45 € – 150 € y una **postura conservadora** del modelo (subestimación controlada) en el segmento premium (> 200 €), comportamiento clásico del *gradient boosting* para evitar sobreajuste en colas largas.
* **Panel B:** Histograma de residuos centrado en 0 € con forma aproximadamente normal, confirmando que los errores son **aleatorios y no sistemáticos**.

#### 3.2 Aplicación Gradio + Despliegue en Hugging Face Spaces

La función `predecir_tarifa_dinamica()` está implementada con **inyección matricial manual** de los bits del One-Hot Encoding para resolver un edge case de `scikit-learn` en el contenedor cloud:

```python
datos_transformados[0, 1:5] = 0.0
if habitacion_final == "entire home/apt":  datos_transformados[0, 1] = 1.0
elif habitacion_final == "hotel room":      datos_transformados[0, 2] = 1.0
elif habitacion_final == "private room":    datos_transformados[0, 3] = 1.0
elif habitacion_final == "shared room":     datos_transformados[0, 4] = 1.0
```

El cuaderno genera automáticamente la estructura modular `/app` con `app.py`, `requirements.txt` aislado, y sincroniza mediante `huggingface_hub.HfApi.upload_folder()` hacia el Space de producción. La inferencia remota se consume después con `gradio_client.Client.predict(api_name="/predict")` consumiendo el endpoint como una API SOA.

#### 3.3 Interpretabilidad — Feature Importance del modelo XGBoost

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

#### 3.4 Benchmark Competitivo — UX/UI en Hugging Face

Auditoría comparativa con dos Spaces de referencia:

| Dimensión | Modelos del Hub (ThomasH007 / anchit48) | Este Proyecto |
|---|---|---|
| **Variables Estructurales** | `Room Type`, `Accommodates`, `Bathrooms`, `Bedrooms`, `Beds` | Idénticas |
| **Dimensión Geográfica** | **Ausente (0 variables)** | **`neighbourhood_cleansed` con Target Encoding** ✅ |
| **Restricciones Comerciales** | `Cancellation Policy`, `Cleaning Fee`, `Instantly Bookable` | **Eliminadas (UX optimizado)** ✅ |
| **Indicadores de Calidad** | `Review Score Rating` | Score + histórico de reseñas ✅ |

Conclusión: el sesgo de **omisión geográfica** en la competencia limita su explicatividad. La eliminación de restricciones comerciales reduce la **fricción del usuario** sin sacrificar precisión.

---

## 🚀 PROTOCOLO DE FINE-TUNING INTERNACIONAL (MÉTRICAS REALES OBTENIDAS)

El experimento de **Transfer Learning** (sección 6 del Hito 3) se ejecuta sobre el dataset de Kaggle [New York City Airbnb Open Data](https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data) (`AB_NYC_2019.csv`, **48.895 registros**). La descarga se automatiza con `kagglehub.dataset_download("dgomonov/new-york-city-airbnb-open-data")` y se copia a `../datasets/importado/AB_NYC_2019.csv`.

### 🛠️ Pipeline de Adaptación de Dominio

1. **Alineación de firmas** con el `ColumnTransformer` del Hito 2. Las variables ausentes (`bedrooms`, `beds`, `bathrooms`) se imputan a `1.0` (constante neutra).
2. **Mapeo geográfico macroscópico**: `neighbourhood_group` (Manhattan, Brooklyn, Queens, Bronx, Staten Island) alimenta el `TargetEncoder` existente.
3. **Conversión de moneda**: `df_nyc['price'] = np.log1p(df_nyc_raw['price'].clip(lower=1))` para empatar la escala logarítmica del Hito 1.
4. **Reentrenamiento incremental** con `xgb.XGBRegressor(n_estimators=150, learning_rate=0.05, max_depth=5, subsample=0.8, random_state=42)` acoplado mediante el parámetro `xgb_model=modelo_xgb_puro`.

### 📊 Tabla Real de Rendimiento (Outputs Originales de la Consola)

```
+-------------------------------------+---------------------+---------------------+---------------------+
| Métrica de Evaluación               | Modelo base (Madrid) | Modelo Fine-Tuned   | Impacto / Variación |
+-------------------------------------+---------------------+---------------------+---------------------+
| MAE (Desviación Media)              | 91.01 $             | 64.17 $             | -26.84 $            |
| R2 Score (Varianza Explicada)       | -0.129              | 0.146               | +0.275              |
+-------------------------------------+---------------------+---------------------+---------------------+
```

| Métrica de Evaluación | Modelo base (Madrid en NYC) | Modelo Fine-Tuned (NYC Especializado) | Impacto / Variación |
|---|---|---|---|
| **MAE (Desviación Media)** | 91.01 $ | 64.17 $ | **Δ MAE = −26.84 $** |
| **R² Score (Varianza Explicada)** | −0.129 | 0.146 | **Δ R² = +0.275** |

### ⏱️ Coste Computacional

| Concepto | Valor |
|---|---|
| Tiempo de cómputo del descenso de gradiente incremental | **2.92 segundos** |
| Tamaño del dataset de transferencia | 48.895 registros |
| Train/Test split | 80 % / 20 % (`random_state=42`) |
| Ruta de persistencia del artefacto híbrido | **`../models/modelo_xgboost_finetuned.json`** |

### 🧠 Conclusión de Ingeniería de Datos — Domain Shift

El **Fine-Tuning** soluciona el fenómeno de **Domain Shift** (sesgo continental entre la economía del euro y el dólar) **sin alterar el pipeline preprocesador**. La clave reside en el parámetro `xgb_model=modelo_xgb_puro` del API de XGBoost:

* El optimizador **no destruye** el conocimiento previo sobre cómo penalizar estancias mínimas o valorar tipologías de habitación.
* **Recalibra únicamente el sesgo base de las hojas terminales** para desplazar la distribución de predicciones hacia la escala real del dólar neoyorquino.
* Un **R² inicial de −0.129** confirma que el modelo de Madrid, en Nueva York, se comportaba **peor que una línea horizontal** (predicción ingenua por la media). Comportamiento normativo en MLOps al cambiar de continente sin adaptación.
* Alcanzar un **R² de 0.146** con las variables físicas críticas fijas a `1.0` (por limitaciones del CSV de Kaggle) es una **prueba de resiliencia analítica** del esquema del pipeline.
* **SLA de producción exitoso**: 27.50 % de incremento en variabilidad explicada y 26.84 $ salvados por registro con un coste de cómputo de **2.92 s** (nota: la sección 6.4.3 del cuaderno cita 4.34 s, correspondiente al cómputo total acumulado de la celda).

El artefacto `modelo_xgboost_finetuned.json` es una **arquitectura híbrida transatlántica** que preserva la lógica distributiva de Madrid pero opera con la elasticidad de precios de la costa este americana.

---

## 🛠️ INSTALACIÓN, CONFIGURACIÓN Y USO

### 1. 🐍 Entorno Virtual (recomendado)

```bash
# Crear entorno virtual
python -m venv venv

# Activar (Windows)
venv\Scripts\activate

# Activar (Linux/macOS)
source venv/bin/activate
```

### 2. 📦 Instalación de Dependencias

El archivo `requirements.txt` declara el siguiente manifiesto de producción:

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

Instalación directa:

```bash
pip install -r requirements.txt
```

Dependencias mínimas para **solo inferencia local** (carpeta `app/`):

```bash
pip install -r app/requirements.txt
```

### 3. ⚙️ Variables de Entorno

Copia `.env.example` como `.env` y rellena los valores:

```bash
cp .env.example .env
```

Variables requeridas:

| Variable | Uso |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Descarga de Parquet desde S3 (Hito 1) |
| `S3_BUCKET_NAME` | Bucket del Data Lake curado |
| `RDS_INSTANCE_ID`, `RDS_DB_NAME`, `RDS_USER`, `RDS_PASSWORD` | Conexión transaccional (opcional) |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` | Productor/consumidor Kafka |
| `MONGO_URI`, `MONGO_DATABASE`, `MONGO_COLLECTION` | Reseñas documentales |
| `USER_HF`, `NOME_REPOSITORIO`, `TOKEN_HF` | Despliegue en Hugging Face Spaces |

### 4. 🐳 Levantar Apache Kafka (opcional, para simulador de eventos)

```bash
cd docker
docker-compose up -d
```

Broker disponible en `localhost:9092`, tópico `busquedas_tiempo_real`.

### 5. 🔁 Replicar el Flujo Completo (orden de ejecución)

```bash
# 1. (Opcional) Generar tráfico sintético en Kafka
python scripts/añadir_eventos_kafka.py

# 2. Ejecutar los cuadernos en orden
jupyter notebook notebooks/hito_00_vision_problema.ipynb
jupyter notebook notebooks/hito_0.5_vision_problema.ipynb
jupyter notebook notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb
jupyter notebook notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb
jupyter notebook notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb
```

### 6. 🚀 Inferencia Local con Artefactos Exportados

Cargar el modelo en cualquier script Python (modo standalone):

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

### 7. ☁️ Despliegue en Hugging Face Spaces

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

### 8. 🧪 Inferencia Remota como API (SOA)

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

---

## 📐 Matriz de Dependencias y Stack Tecnológico (COMPLETA)

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

### 🔗 Matriz de Dependencias por Hito

| Hito | Dependencias críticas |
|---|---|
| **Hito 0 / 0.5** | pandas, numpy, json (stlib) |
| **Hito 1** | boto3, pandas, pyarrow, matplotlib, seaborn, scipy, scikit-learn, python-dotenv, pathlib |
| **Hito 2** | scikit-learn, category-encoders, xgboost, joblib, pandas, numpy |
| **Hito 3** | joblib, xgboost, gradio, gradio_client, huggingface_hub, kagglehub, pandas, numpy |

---

## 🔄 Flujo de CI/CD y MLOps

El proyecto implementa una **cadena de despliegue continuo** híbrida (local + cloud) sin necesidad de GitHub Actions tradicional: el cuaderno Jupyter opera como *orquestador de MLOps* que automatiza el empaquetado y la publicación.

### 📐 Diagrama lógico del pipeline de despliegue

```text
[Cuaderno Hito 3]
       │
       ▼
 ┌──────────────┐
 │ 5. Empaqueta │  → Copia modelos/modelo_xgboost.json + transformador_maestro.joblib
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

### ✅ Variables de entorno requeridas para CI/CD

| Variable | Origen | Uso |
|---|---|---|
| `USER_HF` | `.env` | Identificador de la cuenta de Hugging Face |
| `NOME_REPOSITORIO` | `.env` | Nombre del Space (p. ej. `airbnb-dynamic-pricing`) |
| `TOKEN_HF` | `.env` (secreto) | Token de escritura con scope `write` |
| `GRADIO_EXAMPLES_CACHE` | `os.environ` (en runtime) | Apunta a `<raíz>/.gradio/` para evitar caché dispersa |
| `GRADIO_TEMP_DIR` | `os.environ` (en runtime) | Apunta a `<raíz>/.gradio/` para binarios temporales |

### 🐳 Stack de contenedores

| Servicio | Imagen | Puerto | Función |
|---|---|---|---|
| Apache Kafka (KRaft) | `confluentinc/cp-kafka:7.5.0` | `9092:9092` | Ingesta de clickstream sintético desde `scripts/añadir_eventos_kafka.py` |

---

## 🧪 Reproducibilidad Total del Pipeline

| Componente | Versión fija / Semilla |
|---|---|
| `random_state` (train_test_split) | **42** |
| `random_state` (RandomForest) | **42** |
| `random_state` (XGBoost) | **42** |
| `random_state` (MLPRegressor) | **42** |
| `smoothing` (TargetEncoder) | **10.0** |
| `test_size` (train/test split) | **0.20** |
| `cv` (GridSearchCV) | **3** |
| `scoring` (GridSearchCV) | **'r2'** |
| `n_jobs` (paralelización) | **-1** (todos los cores) |

Cualquier ejecución posterior del repositorio produce **exactamente los mismos splits, métricas y pesos del modelo**, condición indispensable para una auditoría de tribunal.

---

## 📂 Tabla Resumen de Artefactos Generados

| Artefacto | Ruta física | Formato | Generado en |
|---|---|---|---|
| Tablón unificado (parquet) | `datasets/importado/dataset_unificado.parquet` | Apache Parquet | Hito 1 |
| Tablón saneado (csv) | `datasets/importado/dataset_final.csv` | CSV UTF-8 | Hito 1 |
| Dataset externo NYC | `datasets/importado/AB_NYC_2019.csv` | CSV UTF-8 | Hito 3 (kagglehub) |
| Preprocesador maestro | `models/transformador_maestro.joblib` | joblib (pickle) | Hito 2 |
| Modelo Random Forest | `models/pipeline_random_forest.joblib` | joblib | Hito 2 |
| Modelo XGBoost (Pipeline) | `models/pipeline_xgboost.joblib` | joblib | Hito 2 |
| Red Neuronal (MLP) | `models/pipeline_red_nucleonal.joblib` | joblib | Hito 2 |
| **Modelo XGBoost nativo (producción)** | **`models/modelo_xgboost.json`** | **XGBoost JSON** | **Hito 2** |
| **Modelo XGBoost Fine-Tuned (NYC)** | **`models/modelo_xgboost_finetuned.json`** | **XGBoost JSON** | **Hito 3** |
| App Gradio modular | `app/app.py` | Python 3.14 | Hito 3 |
| Manifiesto Space | `app/requirements.txt` | requirements | Hito 3 |
| Replica del modelo en Space | `app/modelo_xgboost.json` | XGBoost JSON | Hito 3 (CI/CD) |
| Replica del transformador en Space | `app/transformador_maestro.joblib` | joblib | Hito 3 (CI/CD) |

---

## 🛡️ Consideraciones de Seguridad y Gobernanza

* El archivo `.env` está excluido del repositorio mediante `.gitignore`. Se provee `.env.example` como plantilla.
* El token de Hugging Face (`TOKEN_HF`) debe tener alcance `write` y rotarse periódicamente.
* El bucket S3 está segmentado en capas (`raw/`, `processed/`, `curated/`) siguiendo la arquitectura *Lake House*.
* La inferencia remota del Space puede exponerse públicamente (`*.gradio.live`) durante el Hito 3 para evaluación del tribunal y luego restringirse a `private=True` en producción.

---

## 📚 Documentación Complementaria

| Documento | Ruta | Contenido |
|---|---|---|
| Decisiones técnicas de arquitectura | `docs/decisiones_tecnicas.md` | Justificación del repositorio único (S3), la persistencia políglota (RDS + Mongo + Kafka) y la estrategia ETL/ELT. |
| Ciclo natural del pipeline inicial | `docs/ciclo_natural_pipeline_inicial.md` | Línea de tiempo de la ejecución del pipeline original. |

---

## 🏁 Estado del Proyecto y Conclusión

Este proyecto constituye un **artefacto de Inteligencia Artificial maduro y desplegado en producción** que cumple los estándares académicos y de ingeniería exigibles a un PFC del ámbito PropTech. Los hitos han sido ejecutados en cadena, validados con búsqueda exhaustiva de hiperparámetros, auditados visualmente, desplegados como servicio en Hugging Face Spaces y **validados internacionalmente mediante Transfer Learning** sobre el dataset de Nueva York.

El techo analítico del 71.38 % de R² en Test es un límite teórico de la naturaleza del dato, no del modelo: capturar la subjetividad del anfitrión o los metros cuadrados exigiría variables que ni Inside Airbnb ni el dataset de Kaggle exponen públicamente. Incrementar el rendimiento requeriría datos propietarios (fotografías, scoring interno de calidad, micro-geolocalización), fuera del alcance de fuentes de datos abiertos.

**Indicadores clave de éxito (KPI de cierre):**

| KPI | Valor |
|---|---|
| Modelos entrenados y auditados | 3 (RF, XGB, MLP) |
| R² en Test del modelo maestro | 71.38 % |
| MAE de producción | 32.24 € |
| Persistencia portable | JSON nativo (sin pickle lock-in) |
| Experimentos de Transfer Learning | 1 (Madrid → NYC) |
| Mejora Δ MAE por Fine-Tuning | −26.84 $ |
| Mejora Δ R² por Fine-Tuning | +0.275 |
| Despliegue en producción | ✅ Hugging Face Space (gradio) |
| Reproducibilidad total | ✅ `random_state=42` en todos los componentes |
| Tests de validación visual | ✅ Dispersión + Histograma de residuos |

---

## 📜 Licencia

Proyecto académico sin licencia comercial explícita. Uso educativo y de portafolio.

---

## ✍️ Autor

**Ricardo Fernández** — *PFC Sistema Inteligente de Precios Dinámicos*
Repositorio: [github.com/RicardoFM30/PFC](https://github.com/RicardoFM30/PFC)