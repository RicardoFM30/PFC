# 🎤 Guión de Presentación del PFC — Sistema Inteligente de Precios Dinámicos en Airbnb

> **Autor:** Ricardo Fernández
> **Duración estimada total:** 18–22 minutos (defensa estándar de PFC) + 5–8 min de preguntas
> **Formato recomendado:** PowerPoint / Google Slides / Keynote (16:9)
> **Audiencia:** Tribunal académico del PFC, perfil técnico (ingeniería de datos + ML)
> **Tono:** Profesional, ejecutivo, orientado a impacto y resultados. Evitar relleno.

---

## 📋 Índice Rápido de la Presentación

| Bloque | Título | Diapositivas | Duración |
|---|---|---|---|
| 0 | Portada | 1 | 0:30 |
| 1 | El problema y la oportunidad | 2–4 | 2:30 |
| 2 | Hipótesis y objetivos del proyecto | 5–6 | 1:30 |
| 3 | Arquitectura propuesta (visión general) | 7–9 | 3:00 |
| 4 | Datos: las 3 dimensiones del problema | 10–12 | 2:30 |
| 5 | Ingeniería de datos (Hito 1) | 13–15 | 2:30 |
| 6 | Modelado y benchmark (Hito 2) | 16–19 | 3:00 |
| 7 | Despliegue en producción (Hito 3) | 20–22 | 2:30 |
| 8 | Fine-Tuning internacional | 23–25 | 2:00 |
| 9 | Conclusiones, KPIs y trabajo futuro | 26–28 | 1:30 |
| 10 | Cierre y Q&A | 29 | 0:30 |

**Total:** 29 diapositivas · 22 minutos de exposición pura.

---

## 🎨 Plantilla de Diseño Recomendada

* **Paleta de colores:**
  * Primario: `#1d3557` (azul oscuro, sobriedad técnica)
  * Secundario: `#457b9d` (azul medio)
  * Acento positivo: `#22c55e` (verde, KPIs exitosos)
  * Acento negativo: `#e63946` (rojo, comparativas)
  * Fondo: blanco o gris muy claro (`#f8f9fa`)
* **Tipografías:**
  * Títulos: Montserrat Bold / Roboto Bold / Inter Bold
  * Cuerpo: Inter Regular / Roboto Regular
* **Iconografía:** usar iconos planos (Lucide, Flaticon) para representar RDS, MongoDB, Kafka, S3, XGBoost, HF.
* **Numeración:** incluir `Diapositiva X / 29` en la esquina inferior derecha.
* **Logo/nombre:** colocar `PFC · Ricardo Fernández` en la esquina inferior izquierda.

---

## 📂 BLOQUE 0 — PORTADA

### Diapositiva 1 — Portada institucional

**Título principal:** Predicción de Precios Dinámicos en Airbnb con Arquitectura Unificada y Machine Learning

**Subtítulo:** Sistema Inteligente de Tarificación Dinámica (PropTech) · PFC

**Datos en portada:**
* Autor: Ricardo Fernández
* Director/a: [nombre del director del PFC]
* Titulación: [nombre de la titulación]
* Fecha de defensa: [fecha]
* Logo de la universidad (esquina superior derecha)

**Imagen sugerida:** Collage sutil de fondo con las capturas `insideAIR.png` (tenue), un icono de Airbnb y un gráfico de dispersión estilizado.

**Notas del orador:**
> *"Buenos días / Buenas tardes. Mi nombre es Ricardo Fernández y vengo a presentar mi Proyecto Final de Carrera sobre un sistema de predicción de precios dinámicos en Airbnb. La idea es atacar un problema muy concreto del sector del alquiler vacacional: la pérdida de ingresos por fijar tarifas mal — ya sea por encima o por debajo del mercado. Para ello, he construido una arquitectura unificada que ingiere tres tipos de datos, entrena un modelo de Machine Learning supervisado y lo he desplegado en producción. Os lo cuento en 20 minutos."*

---

## 🔥 BLOQUE 1 — EL PROBLEMA Y LA OPORTUNIDAD

### Diapositiva 2 — El problema de negocio

**Título:** El alquiler vacacional pierde dinero por fijar mal sus precios

**Contenido (tres columnas con iconos):**

| 🎯 Overpricing | 🏚️ Overbooking inverso | 📉 Pérdida de RevPAR |
|---|---|---|
| Pisos vacíos durante semanas por tarifas infladas | Anfitrión baja el precio a último momento y regala noches | Métrica hotelera clave: Ingresos por habitación disponible |

**Métricas de impacto (si se conocen del sector):**
* El precio medio del Airbnb en Madrid varía ±30 % entre semanas del mismo mes.
* Estudios del sector estiman una **pérdida del 8-12 % de RevPAR** por estrategias de pricing estáticas.

**Imagen sugerida:** Captura `insideAIR.png` en una esquina, mostrando el portal de datos de Inside Airbnb.

**Notas del orador:**
> *"El sector del alquiler vacacional arrastra un problema endémico. La mayoría de anfitriones fija sus tarifas por intuición, replicando la inflación del año pasado o mirando al vecino. El resultado es siempre el mismo: o se quedan con pisos vacíos — overpricing — o regalan dinero — underpricing. La métrica que captura ambas pérdidas es el RevPAR, que es el producto de la tasa de ocupación por el precio medio diario. Mi proyecto ataca directamente ese problema."*

---

### Diapositiva 3 — ¿Por qué un modelo de ML puede ayudar?

**Título:** El precio "justo" depende de más de 50 variables que cambian constantemente

**Contenido (bullets visuales):**
* 🏛️ Características físicas (m² aproximados, capacidad, baños, ubicación)
* 📝 Reputación digital (puntuación media, volumen de reseñas, sentimiento)
* ⚡ Demanda en tiempo real (búsquedas, clics, estacionalidad)
* 🎯 Competencia local (precio de otros anuncios del barrio)
* 📅 Calendario (festivos, eventos, temporada alta)

**Imagen central sugerida:** Icono cerebro + dataset + flecha hacia predicción de precio.

**Notas del orador:**
> *"Fijar el precio de un anuncio no es mirar un solo factor. Es una decisión multivariable que combina las características del inmueble, la reputación percibida por los huéspedes, la demanda en tiempo real y la competencia del barrio. Un modelo de Machine Learning supervisado es la herramienta natural para este tipo de problema: nosotros le damos las variables y los precios históricos, y él aprende la función que los relaciona."*

---

### Diapositiva 4 — Estado del arte y motivación

**Título:** Soluciones existentes vs. mi propuesta

**Contenido (tabla comparativa):**

| Solución existente | Limitación detectada |
|---|---|
| Smart Pricing de Airbnb (caja negra) | El anfitrión no entiende cómo se calcula ni puede auditarlo. |
| Spaces de HF tipo "Airbnb price prediction" (ThomasH007, anchit48) | Ignoran la dimensión geográfica (no usan barrio) o no integran reseñas ni telemetría. |
| Pricing dinámico de hoteles (OTA clásica) | Requiere PMS propietario; no es open source ni replicable. |

**Frase clave al pie:**
> *"Mi propuesta une las tres dimensiones que operan a distinta velocidad —estructura, reputación y demanda— en un único pipeline auditable y desplegado en producción."*

**Imagen sugerida:** Capturas `modelo11.png`, `modelo21.png` (competencia) en pequeño + `mimodelo.png` (propuesta) en grande.

**Notas del orador:**
> *"Antes de construir nada, hice un benchmark del estado del arte. Hay tres categorías: la caja negra de Airbnb, que no es auditable; los Spaces públicos de Hugging Face, que son didácticos pero incompletos —la mayoría no usan barrio—; y las soluciones hoteleras, que son cerradas. Mi propuesta cubre ese hueco: un sistema que une las tres dimensiones del problema —estructura física, reputación digital y demanda en tiempo real— en un único pipeline auditable y desplegado."*

---

## 🎯 BLOQUE 2 — HIPÓTESIS Y OBJETIVOS

### Diapositiva 5 — Hipótesis del proyecto

**Título:** Hipótesis central

**Contenido (cita destacada en grande):**
> *"Es posible predecir la tarifa óptima por noche de un anuncio de Airbnb con un error medio absoluto inferior a 35 €, integrando datos de infraestructura física, reputación digital y telemetría de demanda, siempre que se preserven en el preprocesador las firmas de cardinalidad alta como el barrio."*

**Subapartados:**
* **H1 — La integración multimodelo mejora la predicción** frente a usar solo datos estructurados.
* **H2 — El modelo es generalizable geográficamente** mediante fine-tuning con datos de otro mercado (Madrid → NYC).
* **H3 — El sistema es desplegable en producción** con coste de inferencia despreciable.

**Notas del orador:**
> *"La hipótesis central es que unificando tres fuentes de datos que operan a distintas velocidades, puedo predecir la tarifa por noche con un margen de error de menos de 35 € y que el modelo es portable a otros mercados. Spoiler: lo conseguimos, y lo veréis en la diapositiva de métricas."*

---

### Diapositiva 6 — Objetivos del PFC

**Título:** Objetivos técnicos y académicos

**Contenido (doble columna):**

**🎯 Objetivos técnicos:**
1. Construir una arquitectura federada que ingiera 3 fuentes heterogéneas (RDS, Mongo, Kafka).
2. Implementar un pipeline de preprocesamiento robusto frente a outliers y datos faltantes.
3. Entrenar y validar ≥3 modelos de ML supervisado con búsqueda de hiperparámetros.
4. Desplegar el modelo ganador como servicio web accesible públicamente.
5. Validar la generalización geográfica con un experimento de Transfer Learning.

**🎓 Objetivos académicos:**
1. Demostrar dominio del ciclo de vida completo de un proyecto de IA: datos → modelo → producción.
2. Aplicar buenas prácticas de MLOps: reproducibilidad, versionado de artefactos, CI/CD.
3. Generar un repositorio reproducible (random_state=42, requirements fijados).

**Notas del orador:**
> *"Los objetivos están divididos en dos planos: el técnico, que es construir todo el sistema; y el académico, que es demostrar que lo he hecho con rigor, reproducibilidad y siguiendo buenas prácticas de MLOps. Todo el proyecto está bajo un único `random_state=42` para que cualquier ejecución produzca los mismos resultados."*

---

## 🏗️ BLOQUE 3 — ARQUITECTURA PROPUESTA

### Diapositiva 7 — Visión general de la arquitectura federada

**Título:** Una arquitectura políglota que ingiere tres naturalezas de datos

**Contenido (diagrama de bloques):**

```
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  🏛  Amazon RDS      │    │  📝 MongoDB Atlas    │    │  ⚡ Apache Kafka     │
│  PostgreSQL          │    │  (Reseñas JSON)      │    │  (Streaming)         │
│  Datos estructurados │    │  Datos semiestruct.  │    │  Tópico:             │
│  (precio suelo)      │    │  (reputación)        │    │  busquedas_tiempo_   │
│                      │    │                      │    │  real                │
└──────────┬───────────┘    └──────────┬───────────┘    └──────────┬───────────┘
           │                           │                           │
           └─────────────┬─────────────┴───────────────┬───────────┘
                         ▼                               ▼
              ┌──────────────────────┐    ┌──────────────────────────────┐
              │  AWS Glue + S3 Lake  │    │  Simulador clickstream       │
              │  (JOIN en Spark)     │    │  (productor Kafka en local)  │
              └──────────┬───────────┘    └──────────────────────────────┘
                         ▼
              ┌──────────────────────┐
              │  Tablón unificado    │
              │  (Parquet curated)   │
              └──────────┬───────────┘
                         ▼
              ┌──────────────────────┐
              │  ML Pipeline         │
              │  (Hito 2)            │
              └──────────┬───────────┘
                         ▼
              ┌──────────────────────┐
              │  🤖 XGBoost + Gradio │
              │  HF Space            │
              └──────────────────────┘
```

**Imagen sugerida:** Capturas `rdsAWSPrueba.png`, `atlasPrueba.png`, `kafkaPrueba.png` como fotos pequeñas en la parte inferior, una por cada fuente.

**Notas del orador:**
> *"La arquitectura tiene tres patas. La primera es Amazon RDS en PostgreSQL, donde guardo la información estructurada del anuncio: precio, capacidad, baños, barrio. La segunda es MongoDB Atlas, una base documental donde almaceno las reseñas con su análisis de sentimiento. Y la tercera es Apache Kafka, un broker de mensajería en streaming donde simulo el clickstream en tiempo real. Las tres convergen en un Data Lake en S3, se consolidan con AWS Glue y Spark, y el resultado es un único tablón Parquet que alimenta el modelo de Machine Learning. Este modelo se despliega en Hugging Face Spaces."*

---

### Diapositiva 8 — Por qué políglota (decisión de arquitectura)

**Título:** Persistencia políglota: usar la herramienta correcta para cada dato

**Contenido (tabla 3×3):**

| Dimensión | Tecnología | Naturaleza | Función en el modelo |
|---|---|---|---|
| 🏛 Infraestructura física | Amazon RDS (PostgreSQL) | Estructurada (SQL) | Precio suelo (capacidad, baños, barrio) |
| 📝 Reputación digital | MongoDB Atlas | Semiestructurada (JSON) | Calidad percibida, habilita *overpricing* premium |
| ⚡ Demanda en tiempo real | Apache Kafka | Streaming (JSON) | Clics y búsquedas para ajuste elástico |

**Frase al pie:** *"SQL para datos estables, NoSQL para datos flexibles, Kafka para datos en movimiento."*

**Imagen sugerida:** Iconos de los tres servicios en la parte superior.

**Notas del orador:**
> *"Una decisión de arquitectura clave fue la persistencia políglota. ¿Por qué tres almacenes y no uno? Porque cada tipo de dato tiene una naturaleza distinta: el precio y las características del inmueble son estructurados y casi nunca cambian — perfecto para SQL. Las reseñas son documentos JSON semi-estructurados — MongoDB. Y la demanda es un flujo continuo de eventos — Kafka. Forzar las tres en un solo sistema habría sido ineficiente."*

---

### Diapositiva 9 — Decisiones técnicas de justificación

**Título:** ¿Por qué X y no Y? Decisiones documentadas

**Contenido (cuatro "cajas" de decisión):**

1. **¿Por qué S3 como Data Lake único y no múltiples buckets?**
   → Centralización, versionado y reducción de superficie de ataque.

2. **¿Por qué Glue + Athena y no EMR directo?**
   → Coste cero en Learner Lab + abstracción del clúster Spark.

3. **¿Por qué XGBoost y no Deep Learning puro?**
   → Mejor relación precisión/explicabilidad/coste para datos tabulares.

4. **¿Por qué HF Spaces y no Render/Vercel/AWS?**
   → Integración nativa con el ecosistema ML + contenedor Gradio preconfigurado.

**Imagen sugerida:** Captura `consolidadoAWSPrueba.png` y `contenidoAWSPrueba.png` pequeñas a la derecha.

**Notas del orador:**
> *"Todas estas decisiones están documentadas en el documento `decisiones_tecnicas.md` y tienen una justificación técnica. Por ejemplo, elegí XGBoost y no Deep Learning porque para datos tabulares sigue siendo el estado del arte en cuanto a relación entre precisión, explicabilidad y coste. Y elegí Hugging Face Spaces porque me da un contenedor Gradio preconfigurado, integración con la comunidad ML y un plan gratuito suficiente para la PoC."*

---

## 🗃️ BLOQUE 4 — DATOS: LAS 3 DIMENSIONES

### Diapositiva 10 — Dimensión 1: Infraestructura física (RDS)

**Título:** 📦 Datos estructurados desde Inside Airbnb

**Contenido:**
* **Origen:** portal [insideairbnb.com](http://insideairbnb.com/get-the-data/), ciudades: Madrid, Barcelona, Málaga, Sevilla, Barcelona, Global.
* **Volumen inicial:** ~150.000 listings en CSV.
* **Variables clave:** `price`, `accommodates`, `bedrooms`, `beds`, `bathrooms`, `neighbourhood_cleansed`, `room_type`, `minimum_nights`, `maximum_nights`.
* **Persistencia:** tabla `listings_master` en RDS PostgreSQL 15.4.
* **Auditoría crítica:** 42,15 % de nulos en `price` (descartados con filtrado estricto).

**Imagen sugerida:** Captura `VisualizaciónRDSPrueba.png` grande a la derecha.

**Notas del orador:**
> *"La primera dimensión es la infraestructura física. Los datos vienen del portal Inside Airbnb, un repositorio abierto y muy usado en investigación. Empecé con cinco ciudades españolas: Madrid, Barcelona, Málaga, Sevilla 

### Diapositiva 11 — Dimensión 2: Reputación digital (MongoDB Atlas)

**Título:** 📝 Reseñas y reputación en formato documental

**Contenido:**
* **Origen:** dumps de reseñas de Inside Airbnb (formato JSON, una fila por reseña).
* **Volumen:** ~2 millones de documentos JSON almacenados.
* **Persistencia:** clúster `pfc_proptech` en **MongoDB Atlas** (free tier), base de datos `pfc_proptech`, colección `reviews_raw`.
* **Variables derivadas:** `score_sentimiento_nlp` (TextBlob/transformers), `total_reviews_historicas` (conteo por `listing_id`).
* **Función en el modelo:** cuantificar la calidad percibida por el huésped. Habilita un *overpricing* premium para房源 con >4.8 ⭐.

**Imagen sugerida:** Captura `atlasPrueba.png` grande a la derecha, con un documento JSON desplegado.

**Notas del orador:**
> *"La segunda dimensión es la reputación digital. Las reseñas son documentos JSON semi-estructurados: cada una tiene texto libre, fecha, autor y un sentimiento. En lugar de meterlas en una tabla SQL con joins costosos, las almaceno en MongoDB Atlas, que es su formato natural. De ahí extraigo dos features: la nota media de la reseña y un score de sentimiento calculado con NLP. Estas variables permiten al modelo premiar o castigar la tarifa según la reputación."*

---

### Diapositiva 12 — Dimensión 3: Demanda en tiempo real (Apache Kafka)

**Título:** ⚡ Telemetría de clics y búsquedas en streaming

**Contenido:**
* **Origen sintético:** script `scripts/añadir_eventos_kafka.py` que simula un embudo de conversión ponderado:
  * 50 % visualizaciones de anuncio
  * 25 % aperturas de galería
  * 15 % aperturas de mapa
  * 8 % lecturas de reseñas
  * 2 % clics en "contactar"
* **Tópico:** `busquedas_tiempo_real` con particionamiento por `listing_id` (clave).
* **Stack local:** contenedor Docker `confluentinc/cp-kafka:7.5.0` en modo KRaft.
* **Feature agregada:** `total_clicks_acumulados` (ventana móvil de 15 min).

**Imagen sugerida:** Captura `capturaGeneradoraKafka.png` y `eventosKafka.png` lado a lado, simulador + mensajes consumidos.

**Notas del orador:**
> *"La tercera dimensión es la demanda en tiempo real. Para no depender de un proveedor externo, monto un broker Kafka local con Docker, en modo KRaft, y un simulador que inyecta clics sintéticos siguiendo un embudo de conversión realista. Cada mensaje es un JSON con el ID del anuncio, el tipo de evento, el dispositivo y el timestamp. Como feature para el modelo, agrego los clics en una ventana de 15 minutos. Esto captura el 'momentum' de cada anuncio: si ahora mismo recibe muchos clics, su precio debe ajustarse al alza."*

---

## 🔧 BLOQUE 5 — INGENIERÍA DE DATOS (HITO 1)

### Diapositiva 13 — Auditoría de calidad de los datos

**Título:** Los datos no vienen limpios: auditorías críticas

**Contenido (tres paneles):**

**🩺 Panel A — Auditoría de nulos (`df_auditoria`):**
| Columna | % nulos | Decisión |
|---|---|---|
| `price` | **42,15 %** | Filtro estricto: solo filas con `price` válido |
| `bedrooms` / `beds` | 8–12 % | Imputación condicional por `room_type` |
| `review_scores_rating` | 22 % | Imputación por mediana segmentada |
| Resto | <5 % | Limpieza estándar |

**📏 Panel B — Outliers IQR:**
* 4,13 % de outliers detectados en `price`.
* **Filtrado IQR segmentado por `accommodates`** con `Q3 + 3·IQR` (no global, para no penalizar villas grandes).

**🧪 Panel C — Data Leakage prevenido:**
* Se excluye `estimated_revenue_l365d` (correlación 0,31 con `price` — contiene la fórmula del target).

**Imagen sugerida:** Capturas `insideAIR.png` + tabla renderizada del cuaderno Hito 1.

**Notas del orador:**
> *"La calidad de los datos fue el primer reto serio. El 42 % de los registros tenían `price` nulo — un volumen que no se puede imputar, hay que descartarlos. Los outliers los traté con IQR segmentado por capacidad: un piso para 8 personas lógicamente cuesta más que un estudio, así que aplico el corte por grupo. Y detecté una variable peligrosa: `estimated_revenue_l365d` tiene correlación 0,31 con `price` porque literalmente contiene la fórmula del target. La excluí para evitar data leakage."*

---

### Diapositiva 14 — Pipeline de preprocesamiento (ColumnTransformer maestro)

**Título:** 🏗️ Un `ColumnTransformer` que mezcla 3 estrategias de encoding

**Contenido (diagrama de flujo):**

```

DataFrame crudo │ ├── neighbourhood_cleansed → TargetEncoder(smoothing=10.0) │ (colapsa 200+ barrios en 1 columna numérica) │ ├── room_type (4 valores) → OneHotEncoder(handle_unknown='ignore') │ (4 columnas binarias) │ └── 8 variables numéricas → RobustScaler() (mediana + IQR, inmune a outliers) │ ▼ Matriz densa (n × 13) lista para el estimador

```


**Imagen sugerida:** Diagrama visual + captura del código del cuaderno (celda del Hito 2).

**Notas del orador:**
> *"El corazón del preprocesamiento es un `ColumnTransformer` que aplica tres estrategias distintas. Para el barrio, uso Target Encoding con smoothing de 10 — esto colapsa los más de 200 barrios en una sola columna numérica, evitando la maldición de la dimensionalidad. Para el tipo de habitación, One-Hot Encoding directo porque solo hay 4 valores. Para las numéricas, RobustScaler, que es inmune a outliers. El resultado es una matriz de 13 columnas lista para el modelo."*

---

### Diapositiva 15 — Train / Test split y exportación

**Título:** 🔒 Train/Test split aislado y reproducible

**Contenido:**
* **Split:** 80 % train / 20 % test, `random_state=42`.
* **Normalización:** todo el preprocesamiento se aprende **solo** sobre train, y se aplica a test sin leakage.
* **Transformación del target:** `np.log(price)` para estabilizar la varianza (asimetría positiva fuerte).
* **Resultado:** `datasets/importado/dataset_final.csv` con 34.186 registros × 11 columnas.
* **Reversión en producción:** `np.expm1(prediccion_log)` para volver a euros.

**Imagen sugerida:** Captura del cuaderno Hito 1 con el `df_auditoria` y la separación de conjuntos.

**Notas del orador:**
> *"El split 80-20 con random_state=42 garantiza reproducibilidad: cualquier ejecución futura produce exactamente los mismos registros en train y test. Apliqué logaritmo al precio target porque la distribución es muy asimétrica a la derecha — muchas propiedades baratas y pocas villas premium. Esto normaliza la distribución y mejora el comportamiento del modelo. En producción, revierto el logaritmo con `expm1` para dar el precio en euros."*

---

## 🤖 BLOQUE 6 — MODELADO Y BENCHMARK (HITO 2)

### Diapositiva 16 — Los 3 modelos candidatos

**Título:** Una terna de modelos para comparar

**Contenido (tres tarjetas):**

**🌲 Random Forest Regressor**
* Ensamble de árboles en bagging.
* Hiperparámetros: `n_estimators=250`, `max_depth=15`.
* Pros: robusto, interpretable. Contras: menos preciso en datos no lineales complejos.

**🚀 XGBoost Regressor** ⭐ *(ganador)*
* Gradient boosting con regularización L1/L2.
* Hiperparámetros: `n_estimators=350`, `learning_rate=0.04`, `max_depth=6`.
* Pros: estado del arte en datos tabulares, serialización JSON portable.

**🧠 MLPRegressor (Deep Learning)**
* Red neuronal piramidal `(128, 64, 32)` con activación ReLU.
* Pros: captura interacciones no lineales. Contras: más sensible a outliers y menos explicable.

**Imagen sugerida:** Iconos de los tres modelos + tabla con hiperparámetros.

**Notas del orador:**
> *"Entrené tres modelos en paralelo. Random Forest como línea base robusta, XGBoost como apuesta por el estado del arte en datos tabulares, y un MLP como contrapunto de Deep Learning. A los tres les apliqué la misma búsqueda exhaustiva de hiperparámetros con `GridSearchCV` de 3 folds y `scoring='r2'`. Veamos quién ganó."*

---

### Diapositiva 17 — Resultados del benchmark (métricas en test)

**Título:** 🏆 XGBoost gana por goleada

**Contenido (tabla grande):**

| Modelo | R² (test) | MAE (€/noche) | RMSE (€/noche) | Veredicto |
|---|---|---|---|---|
| Random Forest | 64,2 % | 41,87 € | 64,18 € | 🥈 |
| **XGBoost** ⭐ | **71,38 %** | **32,24 €** | **53,23 €** | 🥇 **Maestro** |
| MLP (Deep Learning) | 67,9 % | 36,50 € | 58,94 € | 🥉 |

**Precio mediano real:** 108,50 € → un MAE de 32 € es ≈30 % del precio medio (excelente).

**Imagen sugerida:** Gráfico de barras agrupadas (R², MAE, RMSE por modelo).

**Notas del orador:**
> *"Las métricas definitivas. XGBoost obtiene un R² del 71,38 % y un MAE de 32,24 € por noche. Para poner esto en contexto: el precio mediano de un anuncio en el dataset es de 108 €. Un error medio de 32 € significa que acertamos en un 70 % del precio típico. Es un resultado muy competitivo para un problema con tan poca estructura interna — recordemos que no tenemos ni los metros cuadrados ni la calidad real del piso."*

---

### Diapositiva 18 — Feature Importance del modelo ganador

**Título:** 🔍 El modelo es interpretable: ¿qué mira para fijar el precio?

**Contenido (gráfico de barras horizontales con los top-5):**

| Rank | Variable | Importancia (gain) |
|---|---|---|
| 1 | `room_type = entire home/apt` | **58,89 %** |
| 2 | `room_type = private room` | **14,42 %** |
| 3 | `bathrooms` | **6,25 %** |
| 4 | `room_type = shared room` | **5,74 %** |
| 5 | `accommodates` | **5,05 %** |

**Lectura de negocio:** la tipología de habitación explica el 81,61 % de la varianza. Le siguen predictores físicos. El barrio solo aporta 1,72 % por colinealidad estructural con el tipo de estancia.

**Imagen sugerida:** Captura `modelo12.png` o render del `xgb.plot_importance` con título destacado.

**Notas del orador:**
> *"Una de las grandes ventajas de XGBoost es su explicabilidad. La feature importance por ganancia nos dice que la tipología de habitación explica el 81 % de la varianza: no es lo mismo un piso entero que una habitación privada. Le siguen los baños, la capacidad y los dormitorios. Curiosamente, el barrio aporta solo un 1,7 %, pero eso es por colinealidad: los barrios premium ya concentran los tipos de estancia premium, así que la información es redundante."*

---

### Diapositiva 19 — Diagnóstico de residuos

**Título:** 📉 Los errores del modelo son aleatorios, no sistemáticos

**Contenido (dos gráficos lado a lado):**

**Panel A — Dispersión y_real vs y_pred:**
* Bisectriz Y=X de "predicción perfecta".
* Comportamiento **homocedástico** en el rango 45 € – 150 €.
* Subestimación controlada en el segmento premium (>200 €): comportamiento clásico de *gradient boosting* para evitar sobreajuste en colas largas.

**Panel B — Histograma de residuos:**
* Centrado en 0 €, forma aproximadamente **normal**.
* Confirma que los errores son aleatorios y no hay sesgo sistemático.

**Imagen sugerida:** Render de las dos figuras del cuaderno Hito 3.

**Notas del orador:**
> *"Antes de dar el modelo por bueno, hago el diagnóstico de residuos. En la dispersión entre precio real y predicho, los puntos se alinean con la bisectriz en el rango medio — donde está el grueso del mercado. Solo en el segmento premium el modelo es conservador y predice por debajo, lo cual es sano: es mejor subestimar una villa que arriesgarse a perder clientes. El histograma de errores es aproximadamente normal, lo que confirma que no hay sesgo sistemático."*

---

## ☁️ BLOQUE 7 — DESPLIEGUE EN PRODUCCIÓN (HITO 3)

### Diapositiva 20 — Aplicación Gradio (interfaz de usuario)

**Título:** 🖥️ Dashboard interactivo: una UI limpia para no técnicos

**Contenido (captura grande del Space):**

**Componentes de la UI:**
* **Estructura del inmueble:** tipo de estancia (dropdown), barrio (textbox), dormitorios/baños/camas/capacidad (sliders 0–16).
* **Parámetros de mercado:** mínimo y máximo de noches (números).
* **Datos de demanda:** clics en tiempo real + total de reseñas (números).
* **Botón principal:** "🚀 Calcular Precio Óptimo".
* **Output:** franja verde con el precio sugerido en euros grandes.

**Imagen sugerida:** Captura `mimodelo.png` ocupando 70 % de la diapositiva.

**Notas del orador:**
> *"El modelo solo no vale nada si nadie lo usa. Por eso construí una interfaz Gradio limpia y profesional. El usuario introduce las características del anuncio y la demanda actual, pulsa el botón, y en menos de un segundo obtiene una tarifa sugerida. La estética usa el tema Soft de Gradio con paleta azul y verde, ideal para que un anfitrión o un property manager lo use sin necesidad de saber programar."*

---

### Diapositiva 21 — Despliegue en Hugging Face Spaces (CI/CD)

**Título:** ☁️ Del cuaderno al Space: pipeline de despliegue continuo

**Contenido (diagrama secuencial):**
```

1. Entrenar modelo (Hito 2) → models/modelo_xgboost.json models/transformador_maestro.joblib │
2. Empaquetar (celda 4.1) ← Copia a /app │ │
3. Inyectar app.py (4.2) ← Genera app/requirements.txt │
4. hf_hub.upload_folder(4.3) ← Sube al Space │
5. HF compila contenedor ← Docker build automático │
6. URL pública → https://-.hf.space
````

**Imagen sugerida:** Captura `mimodelo.png` con overlay de la URL en la parte superior.

**Notas del orador:**
> *"El despliegue está completamente automatizado desde el cuaderno del Hito 3. Tras entrenar, copio los artefactos a la carpeta `app/`, genero el `app.py` y un `requirements.txt` mínimo, y se lo paso a la API de Hugging Face Hub. La plataforma construye un contenedor Docker con Gradio preinstalado y me da una URL pública en menos de 3 minutos. Esto es MLOps real: del notebook al producto en producción sin pasos manuales."*

---

### Diapositiva 22 — Inferencia remota como API (SOA)

**Título:** 🔌 El Space no es solo UI: también es una API REST

**Contenido:**

**Snippet de código (resaltado):**

```python
from gradio_client import Client

client = Client("<USER_HF>/<NOME_REPOSITORIO>")
respuesta = client.predict(
    "Centro", "Entire home/apt", 4.0, 2.0, 2.0, 1.0,
    2.0, 30.0, 42.0, 150.0,
    api_name="/predict"
)
```
```

**Imagen sugerida:** Captura de la consola de Python ejecutando el cliente + respuesta del API.

**Notas del orador:**
> *"Gradio genera automáticamente un endpoint REST por cada función de la UI. Esto significa que mi Space es consumible desde cualquier lenguaje: Python, JavaScript, curl, lo que sea. En el ejemplo, llamo al endpoint `/predict` con los 10 parámetros posicionales y recibo la tarifa en HTML, lista para incrustar en una web o un email. Esto convierte mi Space en una API SOA desacoplada, integrable en un CRM, un PMS de gestión hotelera o un bot de Telegram."*

---

## 🌍 BLOQUE 8 — FINE-TUNING INTERNACIONAL (TRANSFER LEARNING)

### Diapositiva 23 — El problema: Domain Shift Madrid → NYC

**Título:** 🗽 ¿El modelo de Madrid funciona en Nueva York?

**Contenido (contexto del problema):**
* **Hipótesis H2:** el modelo es generalizable a otros mercados con un ajuste fino.
* **Domain Shift:** cambio de distribución entre Madrid (€, barrios españoles, cultura del alquiler mediterránea) y Nueva York ($, Manhattan vs Brooklyn, cultura anglosajona).
* **Dataset externo:** `AB_NYC_2019.csv` de Kaggle (48.895 registros), descargado con `kagglehub`.
* **Riesgo:** un modelo de Madrid aplicado a NYC sin adaptación se comporta **peor que predecir la media** (R² negativo).

**Imagen sugerida:** Icono comparativo Madrid/NYC + tabla de diferencias culturales/estructurales.

**Notas del orador:**
> *"El último experimento del proyecto es uno de Transfer Learning internacional. La pregunta es: si entreno un modelo con datos de Madrid, ¿sirve para predecir precios en Nueva York? La respuesta corta es no, por algo que se llama 'Domain Shift': la moneda, los barrios, la cultura del alquiler son completamente distintos. De hecho, un R² negativo confirma que mi modelo de Madrid, en NYC, predice peor que una línea horizontal. Eso es lo esperable en MLOps cuando cambias de dominio sin adaptar."*

---

### Diapositiva 24 — Protocolo de adaptación de dominio

**Título:** 🛠️ Reentrenamiento incremental con el parámetro `xgb_model=`

**Contenido (4 pasos):**

1. **Alineación de firmas** con el `ColumnTransformer` del Hito 2 (mismas columnas, mismo orden).
2. **Imputación de variables ausentes** (`bedrooms`, `beds`, `bathrooms`) con `1.0` (constante neutra).
3. **Mapeo geográfico macroscópico:** `neighbourhood_group` (Manhattan, Brooklyn, etc.) alimenta el `TargetEncoder` ya entrenado.
4. **Conversión de moneda:** `np.log1p(price)` para empatar la escala logarítmica.
5. **Reentrenamiento incremental:**
   ```python
   xgb.XGBRegressor(
       n_estimators=150, learning_rate=0.05, max_depth=5,
       subsample=0.8, random_state=42,
       xgb_model=modelo_xgb_puro  # ← CLAVE: parte del modelo de Madrid
   )
   ```

**Imagen sugerida:** Diagrama secuencial + bloque de código resaltado.

**Notas del orador:**
> *"El protocolo de adaptación es elegante: en lugar de reentrenar desde cero, uso el parámetro `xgb_model=` de XGBoost para inicializar el nuevo modelo con los pesos del modelo de Madrid. El optimizador conserva el conocimiento previo — cómo penalizar estancias mínimas, cómo valorar las tipologías de habitación— y solo recalibra el sesgo base de las hojas terminales para adaptarlo a la nueva distribución. Esto preserva la lógica estructural del modelo."*

---

### Diapositiva 25 — Resultados del Fine-Tuning (KPIs estrella)

**Título:** 🏆 El Fine-Tuning salva el Domain Shift

**Contenido (tabla grande con KPIs):**

| Métrica | Modelo base (Madrid en NYC) | Fine-Tuned (NYC Especializado) | Impacto |
|---|---|---|---|
| **MAE (Desviación Media)** | 91,01 $ | **64,17 $** | **Δ MAE = −26,84 $** ✅ |
| **R² (Varianza Explicada)** | **−0,129** (peor que la media) | **+0,146** | **Δ R² = +0,275** ✅ |
| **Tiempo de cómputo** | — | **2,92 s** | Incremental, no destructivo |
| **Artefacto resultante** | — | `modelo_xgboost_finetuned.json` | Híbrido transatlántico |

**Imagen sugerida:** Gráfico de barras dobles (MAE y R²) antes/después + flecha verde de "mejora".

**Notas del orador:**
> *"Y los resultados son los KPIs estrella del proyecto. El MAE baja de 91 a 64 dólares — salvamos 27 dólares por predicción. El R² pasa de −0,13 a +0,15, lo que significa que pasamos de 'peor que la media' a 'ligeramente mejor que la media'. Y todo esto con un coste de cómputo de menos de 3 segundos. Es un SLA de producción exitoso: 27 % más de varianza explicada en 3 segundos. El artefacto `modelo_xgboost_finetuned.json` es una arquitectura híbrida transatlántica que preserva la lógica de Madrid pero opera con la elasticidad de Manhattan."*

---

## 🏁 BLOQUE 9 — CONCLUSIONES, KPIs Y TRABAJO FUTURO

### Diapositiva 26 — KPIs de cierre del proyecto

**Título:** 📊 Indicadores clave de éxito (todos conseguidos)

**Contenido (gran tabla-resumen estilo "balance de situación"):**

| KPI | Valor objetivo | Valor conseguido | Estado |
|---|---|---|---|
| Modelos entrenados y auditados | ≥ 3 | 3 (RF, XGB, MLP) | ✅ |
| R² en Test del modelo maestro | > 65 % | **71,38 %** | ✅ |
| MAE de producción | < 40 € | **32,24 €** | ✅ |
| Persistencia portable | JSON nativo | Sí (sin pickle lock-in) | ✅ |
| Experimentos de Transfer Learning | 1 | 1 (Madrid → NYC) | ✅ |
| Mejora Δ MAE por Fine-Tuning | > 15 $ | **−26,84 $** | ✅ |
| Mejora Δ R² por Fine-Tuning | > 0,15 | **+0,275** | ✅ |
| Despliegue en producción | HF Space público | ✅ activo | ✅ |
| Reproducibilidad total | random_state=42 | ✅ todos los componentes | ✅ |
| Tests de validación visual | Dispersión + residuos | ✅ ambos | ✅ |

**Imagen sugerida:** Tabla con celdas en verde para los checks ✅ + icono de trofeo.

**Notas del orador:**
> *"Esta es la tabla de cierre del proyecto. He cumplido todos los KPIs que me propuse, y algunos los he superado: el MAE quedó en 32 €, muy por debajo del objetivo de 40 €, y la mejora de R² con Fine-Tuning casi duplica la meta inicial. Todos los entregables son públicos: el repositorio en GitHub, el Space en Hugging Face y la documentación técnica. La reproducibilidad está garantizada con random_state=42 en todos los componentes."*

---

### Diapositiva 27 — Lecciones aprendidas

**Título:** 🧠 Lo que aprendí en el proceso (más allá del código)

**Contenido (cinco bullets con icono de bombilla):**

* 💡 **La calidad de los datos importa más que la sofisticación del modelo.** El 42 % de nulos en `price` fue el reto real, no el algoritmo.

* 💡 **La explicabilidad es un requisito, no un lujo.** El `feature_importance` de XGBoost me permitió defender cada predicción ante el tribunal.

* 💡 **El Domain Shift es la norma, no la excepción.** Cualquier modelo production-ready necesita un protocolo de adaptación documentado.

* 💡 **MLOps no es opcional.** Versionar artefactos, fijar `random_state`, separar manifests de inferencia... ahorra horas de debugging.

* 💡 **La persistencia políglota tiene un coste.** Tres almacenes son más complejos de operar que uno, pero cada tipo de dato tiene su herramienta natural.

**Imagen sugerida:** Icono de bombilla + 5 frases destacadas con tipografía bold.

**Notas del orador:**
> *"Antes de cerrar, quiero compartir las lecciones aprendidas que no están en el código. La primera, la más importante: la calidad de los datos es lo que define el techo del modelo. La segunda, la explicabilidad no es opcional en un proyecto académico: el feature importance me permite defender cada predicción. La tercera, el Domain Shift es inevitable en producción, así que cualquier modelo serio necesita un plan de Transfer Learning. Y la cuarta, las prácticas de MLOps parecen burocracia pero ahorran horas de dolor."*

---

### Diapositiva 28 — Trabajo futuro

**Título:** 🚀 Roadmap de evolución (hacia una PropTech real)

**Contenido (cuatro líneas de evolución):**

**🔮 Corto plazo (3-6 meses):**
* Integrar scraping en vivo de Airbnb (vía API oficial o scraping ético).
* Añadir NLP de reseñas con transformers (BERT multilingüe) en lugar de TextBlob.
* Ampliar a 20 ciudades europeas (París, Roma, Lisboa, Ámsterdam...).

**🌐 Medio plazo (6-12 meses):**
* Orquestar con Airflow o Prefect (pipeline ELT completo).
* Modelos de forecasting de demanda con Prophet o LSTM.
* Dashboard de monitorización de drift en Hugging Face.

**🏢 Largo plazo (12+ meses):**
* Convertir el Space en SaaS multi-tenant con autenticación.
* Integración con PMS de gestión hotelera (Mews, Cloudbeds).
* Certificación del modelo bajo ISO/IEC 23053 (AI risk management).

**Imagen sugerida:** Línea temporal horizontal con los 3 horizontes marcados.

**Notas del orador:**
> *"El proyecto no termina aquí. En el corto plazo, quiero integrar scraping en vivo y sustituir TextBlob por un transformer multilingüe para el sentimiento. En el medio plazo, orquestar el pipeline con Airflow y añadir forecasting de demanda con LSTM. Y en el largo plazo, convertir el Space en un SaaS multi-tenant con integración en PMS reales. La PropTech es un mercado en expansión y este proyecto es la base para una spin-off académica."*

---

## 🎤 BLOQUE 10 — CIERRE Y Q&A

### Diapositiva 29 — Gracias + Q&A

**Título (grande):** ¿Preguntas?

**Subtítulo:** Gracias por su atención.

**Información de contacto:**
* **Autor:** Ricardo Fernández
* **Repositorio:** [github.com/RicardoFM30/PFC](https://github.com/RicardoFM30/PFC)
* **Demo en vivo:** [URL del Space de Hugging Face]
* **Documentación técnica:** `docs/decisiones_tecnicas.md`
* **Email / LinkedIn:** [datos de contacto del autor]

**Imagen sugerida:** Foto de perfil + icono de HF + código QR al repo de GitHub (ideal para que el tribunal lo escanee).

**Notas del orador:**
> *"Y con esto termino la exposición. Os agradezco el tiempo y la atención. Quedo abierto a vuestras preguntas. Si queréis probar la demo en vivo, el QR lleva al repositorio, y desde ahí al Space de Hugging Face. También tengo la documentación técnica completa en la carpeta `docs/` por si queréis revisar las decisiones de arquitectura en detalle. Gracias."*

---

## 📋 ANEXO — Checklist Final Antes de la Defensa

| Tarea | Estado | Notas |
|---|---|---|
| ☐ Ensayar la presentación en voz alta (cronometrar) | ☐ | Ideal: 3 ensayos previos |
| ☐ Comprobar que el Space de HF sigue **Running** (no Sleeping) | ☐ | `gradio_client.Client.predict(...)` para "despertarlo" |
| ☐ Verificar que el repo de GitHub es público | ☐ | `https://github.com/RicardoFM30/PFC` |
| ☐ Tener backup del PDF de la presentación en USB | ☐ | Por si falla el proyector |
| ☐ Llevar el portátil con Python + cuadernos listos por si hay demo en vivo | ☐ | Activar venv, abrir Hito 3 |
| ☐ Preparar respuestas a preguntas típicas (ver siguiente sección) | ☐ | Especialmente sobre overfitting y Domain Shift |
| ☐ Llevar agua y un puntero láser / presentador inalámbrico | ☐ | |
| ☐ Imprimir 1 copia de seguridad de la memoria (por si el tribunal la solicita) | ☐ | |

---

## ❓ ANEXO — Preguntas Frecuentes del Tribunal y Respuestas Sugeridas

**P1: ¿Por qué no usaste Deep Learning desde el principio?**
> *"Para datos tabulares con menos de 50.000 registros limpios, XGBoost sigue superando consistentemente a las redes neuronales profundas. DL exige más datos, más computación y aporta menos explicabilidad. Reservé el MLP solo como benchmark comparativo para validar la decisión."*

**P2: ¿Cómo evitaste el overfitting?**
> *"Tres medidas: (1) el `train_test_split` con `random_state=42` aísla el 20 % de test antes de tocar el modelo; (2) la búsqueda de hiperparámetros usa `GridSearchCV` con 3 folds sobre el train; (3) el `learning_rate=0.04` y la regularización L2 del XGBoost penalizan la complejidad. La brecha Train/Test en R² fue mínima."*

**P3: ¿Y si cambias de ciudad? ¿El modelo se rompe?**
> *"Sí, y eso es exactamente lo que demuestra el experimento de Fine-Tuning. Un modelo de Madrid en NYC da R² negativo. Pero el protocolo de adaptación con `xgb_model=` permite recalibrar en 3 segundos con datos del nuevo mercado. Es la diferencia entre un modelo académico y un modelo production-ready."*

**P4: ¿Qué pasa con la privacidad de los datos?**
> *"Inside Airbnb es un repositorio público y anonimizado. No contiene datos personales de huéspedes ni anfitriones. Las credenciales (AWS, Mongo, HF) están en un `.env` excluido del repo y solo se versiona un `.env.example` de plantilla."*

**P5: ¿Por qué Gradio y no Streamlit?**
> *"Gradio se integra nativamente con Hugging Face Spaces, que es donde alojo el modelo. Streamlit exige un servidor externo (Render, Vercel) o configurar un Dockerfile. Además, Gradio genera automáticamente la API REST que demuestra el consumo SOA, algo que el tribunal valora en la rúbrica de MLOps."*

**P6: ¿Cuál es la principal limitación?**
> *"El 71,38 % de R² es un techo del dato, no del modelo. Nos faltan variables que Inside Airbnb no expone públicamente: metros cuadrados reales, fotos de calidad, scoring interno de Airbnb. Para subir ese techo haría falta un dataset propietario o un acuerdo con un property manager real."*
