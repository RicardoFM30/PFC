# Catálogo de Capturas Ad Propuestas para `decisiones_tecnicas.md` (Punto 12)

> **Propósito:** este documento complementa la sección 12 de `docs/decisiones_tecnicas.md` (`Justificación de evidencias multimedia (/capturas)`). Para cada captura adicional se especifica:
>
> 1. **Nombre de archivo** propuesto (sigue la convención `snake_case` y números correlativos para no colisionar con las 14 capturas existentes).
> 2. **Qué se debe ver exactamente** en la imagen (criterio de aceptación).
> 3. **De dónde se obtiene** (script, cuaderno, URL, comando o flujo de plataforma).
> 4. **Justificación** (por qué refuerza la narrativa de auditoría del PFC).
> 5. **Hito/sección del documento** donde encajaría como ilustración.

Convención de nombrado recomendada: `capturas/<numero_descriptivo>__<categoria>_<detalle>.png`.
Las 14 capturas ya presentes se conservan tal cual; las nuevas comienzan con el prefijo `n15_` para evitar pisar archivos existentes.

---

## 📋 Resumen ejecutivo de capturas propuestas

| Código | Nombre propuesto | Origen | Hito al que aplica |
|---|---|---|---|
| n15 | `n15_docker_kafka_running.png` | Docker Desktop / `docker ps` | 4 — Kafka |
| n16 | `n16_kafka_topic_create_or_describe.png` | CLI Kafka dentro del contenedor | 4 — Kafka |
| n17 | `n17_rds_security_group_rule.png` | Consola AWS EC2 → Security Groups | 3.1 — RDS |
| n18 | `n18_glue_job_console_running.png` | Consola AWS Glue → Job Runs | 5.3 — Glue |
| n19 | `n19_glue_crawler_output.png` | Consola AWS Glue → Crawlers / Data Catalog | 5.3 — Glue |
| n20 | `n20_athena_query_results.png` | Consola Amazon Athena → resultados SQL | 5.3 — Athena |
| n21 | `n21_s3_bucket_layers_browser.png` | Consola S3 → navegación de prefijos | 2.2 — S3 |
| n22 | `n22_eda_matriz_nulos.png` | Hito 1 → `df_auditoria` o heatmap seaborn | 6 — EDA |
| n23 | `n23_eda_outliers_iqr_boxplot.png` | Hito 1 → boxplot por `accommodates` | 6 — EDA |
| n24 | `n24_eda_distribucion_precio_log.png` | Hito 1 → histograma de `np.log(price)` | 6 — EDA |
| n25 | `n25_eda_top_neighbourhoods.png` | Hito 1 → barh por barrio tras Target Encoding | 6 — EDA |
| n26 | `n26_gridsearch_results_table.png` | Hito 2 → `pd.DataFrame(cv_results_)` ordenado por `mean_test_score` | 7 — Entrenamiento |
| n27 | `n27_model_comparison_metrics.png` | Hito 2 → barplot con MAE/RMSE/R² de los 3 modelos | 7 — Entrenamiento |
| n28 | `n28_xgb_feature_importance.png` | Hito 2 → `plot_importance(xgb)` | 8.3 — Interpretabilidad |
| n29 | `n29_finetuning_convergence.png` | Hito 3 → evolución de MAE/R² antes vs después | 8.5 — Transfer Learning |
| n30 | `n30_hf_space_dashboard.png` | Hugging Face Spaces → Space público del proyecto | 8.2 — Despliegue |
| n31 | `n31_hf_api_predict_request.png` | `gradio_client.Client.predict(...)` ejecutado en consola | 8.2 — Despliegue |
| n32 | `n32_requirements_txt_proof.png` | Captura del archivo `app/requirements.txt` en VS Code | 10.7 — Manifiestos |
| n33 | `n33_env_example_proof.png` | Captura del archivo `.env.example` en VS Code | 10.1 — Seguridad |
| n34 | `n34_hito2_metrics_console_output.png` | Consola de la última celda del Hito 2 con `MAE/RMSE/R²` | 7 — Entrenamiento |

---

## 1. Capturas de Infraestructura Local y Cloud (Hitos 3, 4 y 5)

### `n15_docker_kafka_running.png`
* **Qué debe verse:** la salida de `docker ps` mostrando el contenedor `pfc_kafka_proptech` con la imagen `confluentinc/cp-kafka:7.5.0`, el puerto `0.0.0.0:9092->9092/tcp`, el `STATUS = Up` y el `CLUSTER_ID` configurado.
* **De dónde se obtiene:**
  1. Abrir Docker Desktop o una terminal PowerShell.
  2. Ejecutar: `cd C:\Users\Ric\Desktop\PFC\docker && docker-compose up -d`.
  3. Verificar: `docker ps --filter "name=pfc_kafka_proptech"`.
  4. Capturar la pantalla completa.
* **Justificación:** evidencia tangible de que el broker local está vivo y de que la arquitectura federada se sostiene sobre Docker Compose (sin recursos cloud para la capa de streaming).
* **Hito/sección:** 4.1 — Infraestructura del broker.

### `n16_kafka_topic_create_or_describe.png`
* **Qué debe verse:** la salida del comando `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic busquedas_tiempo_real` con el `TopicId`, `PartitionCount`, `ReplicationFactor` y los `Leaders` del tópico. Alternativa: captura de la herramienta de inspección **Offset Explorer** o **Conduktor** con la misma información.
* **De dónde se obtiene:**
  1. Entrar al contenedor: `docker exec -it pfc_kafka_proptech bash`.
  2. `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic busquedas_tiempo_real --partitions 1 --replication-factor 1` (si no existe).
  3. `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic busquedas_tiempo_real`.
  4. Capturar la consola.
* **Justificación:** demuestra que el tópico existe y está particionado, requisito indispensable para el particionamiento determinista por `listing_id`.
* **Hito/sección:** 4.2 — Estructura del mensaje e idempotencia.

### `n17_rds_security_group_rule.png`
* **Qué debe verse:** la consola de AWS EC2 → **Security Groups** → reglas de entrada del SG asignado al RDS, mostrando la regla **Custom TCP 5432 con source `0.0.0.0/0`** o un `description` equivalente a "PostgreSQL access from anywhere".
* **De dónde se obtiene:**
  1. AWS Console → EC2 → Security Groups.
  2. Seleccionar el SG cuyo ID coincide con el `VpcSecurityGroupId` que imprime el script `pipeline_inicio_xauto_Glue_Athena.py`.
  3. Pestaña **Inbound rules** → capturar.
* **Justificación:** el script abre el firewall automáticamente (`open_rds_firewall` / `authorize_security_group_ingress`). La captura demuestra que la regla existe y se aplicó, requisito para que el cuaderno del Hito 1 pueda descargar datos.
* **Hito/sección:** 3.1 — RDS / 5.2 — Lambda + boto3.

### `n18_glue_job_console_running.png`
* **Qué debe verse:** la consola de AWS Glue → **ETL jobs** → job `pfc_proptech_federated_etl` con el último `Run status = Succeeded`, duración en segundos y enlace a los CloudWatch logs.
* **De dónde se obtiene:**
  1. AWS Console → AWS Glue → ETL jobs.
  2. Click sobre el job `pfc_proptech_federated_etl`.
  3. Pestaña **Runs** → seleccionar el último `Run Id` con estado `Succeeded`.
  4. Capturar la tabla de Runs y la métrica de duración.
* **Justificación:** cierra el flujo del orquestador `pipeline_inicio_xauto_Glue_Athena.py` mostrando que el clúster Spark realmente ejecutó `job_pyspark_analitico.py` y persistió los Parquet.
* **Hito/sección:** 5.3 — AWS Glue.

### `n19_glue_crawler_output.png`
* **Qué debe verse:** la consola de AWS Glue → **Crawlers** → tabla generada automáticamente por un crawler sobre `s3://<bucket>/curated/dataset_proptech_master/`, con el esquema detectado (columnas `listing_id`, `price`, `accommodates`, etc.) y la opción `Classify data → True`.
* **De dónde se obtiene:**
  1. (Si no se ha creado antes) ejecutar el script `pipeline_inicio_xauto_Glue_Athena.py` y, una vez generado el Parquet, crear un crawler en la consola apuntando a `curated/dataset_proptech_master/`.
  2. Esperar al estado `Ready` del crawler.
  3. Pestaña **Tables** del Data Catalog → seleccionar la tabla generada → capturar.
* **Justificación:** refuerza la afirmación de la sección 5.3 sobre el `Glue Data Catalog` y la catalogación automática, complementando la evidencia de Athena (n20).
* **Hito/sección:** 5.3 — AWS Glue.

### `n20_athena_query_results.png`
* **Qué debe verse:** la consola de **Amazon Athena** con la query `SELECT room_type, AVG(price) AS avg_price, COUNT(*) AS n FROM proptech_analytics_db.dataset_master GROUP BY room_type ORDER BY avg_price DESC;` ya ejecutada y con la tabla de resultados visible (no el editor vacío).
* **De dónde se obtiene:**
  1. AWS Console → Athena → Query editor.
  2. Seleccionar la base de datos `proptech_analytics_db`.
  3. Ejecutar la query anterior (o `SELECT * FROM dataset_master LIMIT 20`).
  4. Capturar la pestaña **Results** y la pestaña **History** con el `Data scanned` y el `Run time`.
* **Justificación:** la captura `contenidoAWSPrueba.png` ya cubre una vista, pero una query de agregación por `room_type` con `AVG(price)` añade valor analítico y demuestra SQL real sobre Parquet.
* **Hito/sección:** 5.3 — Athena.

### `n21_s3_bucket_layers_browser.png`
* **Qué debe verse:** la consola de Amazon S3 con la navegación por prefijos del bucket del Data Lake: `raw/eventos_kafka/`, `raw/reviews_mongo/`, `curated/dataset_proptech_master/`, `scripts/`, `athena-results/`. Cada carpeta abierta mostrando al menos un objeto.
* **De dónde se obtiene:**
  1. AWS Console → S3 → seleccionar el bucket.
  2. Expandir manualmente las cuatro carpetas listadas.
  3. Captura de pantalla panorámica.
* **Justificación:** visualiza la estructura física descrita en la sección 2.2 del documento (`raw/`, `curated/`, `scripts/`, `athena-results/`) y la pone en valor como arquitectura *Lake House* real.
* **Hito/sección:** 2.2 — Estructura física del bucket.

---

## 2. Capturas de Análisis Exploratorio (Hito 1)

### `n22_eda_matriz_nulos.png`
* **Qué debe verse:** una imagen con dos paneles:
  * Panel A: tabla `df_auditoria` con columnas `dtype`, `count`, `isnull`, `% nulos` (renderizada con `df.style.background_gradient`).
  * Panel B: heatmap seaborn `sns.heatmap(df.isnull(), cbar=False)` con el título "Mapa de calor de nulos por columna y registro".
* **De dónde se obtiene:**
  1. Abrir `notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb`.
  2. Localizar la celda que genera `df_auditoria` (alrededor de la línea que imprime `df_auditoria.head()`).
  3. Añadir tras esa celda un bloque:
     ```python
     import matplotlib.pyplot as plt, seaborn as sns
     fig, axes = plt.subplots(1, 2, figsize=(16, 6))
     sns.heatmap(df.isnull(), cbar=False, ax=axes[0])
     axes[0].set_title('Mapa de calor de nulos')
     # panel B: renderizar df_auditoria como tabla con background
     axes[1].axis('off')
     axes[1].table(cellText=df_auditoria.head(15).values,
                    colLabels=df_auditoria.columns, loc='center')
     plt.tight_layout()
     plt.savefig('../capturas/n22_eda_matriz_nulos.png', dpi=150, bbox_inches='tight')
     plt.show()
     ```
  4. Ejecutar y guardar.
* **Justificación:** documenta el **42.152 % de nulos en `price`** mencionado en la sección 6 y convierte el diagnóstico en evidencia visual.
* **Hito/sección:** 6 — Auditoría de nulos.

### `n23_eda_outliers_iqr_boxplot.png`
* **Qué debe verse:** un boxplot múltiple (`sns.boxplot`) con `price` (en escala `np.log(price)`) en el eje Y y `accommodates` (1, 2, 4, 6, 8+) en el eje X. El corte en Q3 + 3·IQR debe ser visualmente apreciable.
* **De dónde se obtiene:**
  1. Mismo cuaderno, sección 5 (Outliers IQR).
  2. Reusar la variable `df_filtrado` que ya está segmentada por `accommodates`.
  3. Bloque:
     ```python
     sns.boxplot(data=df_filtrado, x='accommodates', y='price', showfliers=True)
     plt.axhline(np.log(limite_superior_global), color='red', linestyle='--',
                 label=f'Q3 + 3·IQR (log)')
     plt.title('Outliers en log(price) por capacidad')
     plt.savefig('../capturas/n23_eda_outliers_iqr_boxplot.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** la sección 6 menciona el **4.13 % de outliers** y el filtrado IQR segmentado; un boxplot lo hace tangible.
* **Hito/sección:** 6 — Outliers IQR.

### `n24_eda_distribucion_precio_log.png`
* **Qué debe verse:** histograma + KDE de `np.log(price)` con línea vertical roja en la mediana. La distribución debe verse aproximadamente normal (campana simétrica), justificando la transformación del Hito 1.
* **De dónde se obtiene:**
  1. Mismo cuaderno, tras la imputación y tipado categórico.
  2. Bloque:
     ```python
     plt.figure(figsize=(10, 5))
     sns.histplot(np.log(df_final['price']), kde=True, bins=40, color='#1d3557')
     plt.axvline(np.log(df_final['price']).median(), color='red', linestyle='--')
     plt.title('Distribución log(price) tras depuración — Hito 1')
     plt.xlabel('ln(€ por noche)')
     plt.savefig('../capturas/n24_eda_distribucion_precio_log.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** contrasta con la Figura 1 del Hito 3 (que muestra `price` en euros y justifica la transformación retrospectivamente). Aquí se ve **el resultado ya transformado** en el Hito 1.
* **Hito/sección:** 6 — Estabilización de varianza.

### `n25_eda_top_neighbourhoods.png`
* **Qué debe verse:** barplot horizontal (`sns.barh`) con los 15 barrios con mayor `price` medio, tras aplicar Target Encoding. Cada barra etiquetada con su valor numérico.
* **De dónde se obtiene:**
  1. Mismo cuaderno, tras aplicar el TargetEncoder.
  2. Bloque:
     ```python
     te = transformador_maestro.named_transformers_['barrios_te']
     te_df = pd.DataFrame({'barrio': transformador_maestro.feature_names_in_, 'te_value': te.mapping_})
     top15 = te_df.nlargest(15, 'te_value')
     sns.barplot(data=top15, y='barrio', x='te_value', palette='viridis')
     plt.title('Top 15 barrios por Target Encoding')
     plt.savefig('../capturas/n25_eda_top_neighbourhoods.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** da contexto a la **importancia del 1.72 %** de `neighbourhood_cleansed` en el modelo final (sección 8.3) y permite entender por qué el Target Encoding con `smoothing=10.0` es crítico.
* **Hito/sección:** 6 / 7.1 — Target Encoding.

---

## 3. Capturas de Entrenamiento y Validación (Hito 2)

### `n26_gridsearch_results_table.png`
* **Qué debe verse:** tabla HTML o imagen de un DataFrame con las 10 mejores combinaciones de `GridSearchCV` para XGBoost, ordenadas por `mean_test_score` descendente. Columnas mínimas: `param_n_estimators`, `param_max_depth`, `param_learning_rate`, `mean_train_score`, `mean_test_score`, `rank_test_score`.
* **De dónde se obtiene:**
  1. Abrir `notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb`.
  2. Localizar la celda que ejecuta `grid_search_xgb.fit(X_train, y_train)`.
  3. Añadir tras esa celda un bloque:
     ```python
     import pandas as pd
     resultados_xgb = pd.DataFrame(grid_search_xgb.cv_results_).sort_values(
         by='mean_test_score', ascending=False
     )[['param_n_estimators','param_max_depth','param_learning_rate',
            'mean_train_score','mean_test_score','rank_test_score']].head(10)
     resultados_xgb.to_html('../capturas/n26_gridsearch_results_table.html')
     ```
  4. Abrir el HTML resultante en un navegador, hacer zoom y capturar PNG (o `df.style.background_gradient(cmap='viridis')` y `plt.savefig`).
* **Justificación:** convierte el espacio de búsqueda de la sección 7.3 en evidencia tangible: el evaluador externo puede ver qué combinaciones probó el `GridSearchCV` y por qué se eligieron los hiperparámetros finales.
* **Hito/sección:** 7.3 — Espacio de búsqueda.

### `n27_model_comparison_metrics.png`
* **Qué debe verse:** gráfico de barras agrupadas (`sns.barplot`) con los tres modelos (RandomForest, XGBoost, MLP) en el eje X y tres barras por modelo: `R² Test`, `MAE (€)` y `RMSE (€)`. Los valores numéricos deben coincidir con la sección 7.4 (R² XGBoost = 71.38 %, MAE = 32.24 €, RMSE = 53.23 €).
* **De dónde se obtiene:**
  1. Mismo cuaderno, tras la evaluación de los tres modelos.
  2. Bloque:
     ```python
     import matplotlib.pyplot as plt, seaborn as sns
     df_metricas = pd.DataFrame({
         'modelo': ['RandomForest','XGBoost','MLP'],
         'R2':    [r2_rf, r2_xgb, r2_mlp],
         'MAE':   [mae_rf, mae_xgb, mae_mlp],
         'RMSE':  [rmse_rf, rmse_xgb, rmse_mlp]
     })
     df_long = df_metricas.melt(id_vars='modelo', var_name='métrica', value_name='valor')
     sns.barplot(data=df_long, x='modelo', y='valor', hue='métrica')
     plt.title('Comparativa de modelos — Hito 2')
     plt.savefig('../capturas/n27_model_comparison_metrics.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** sustenta visualmente la decisión de la sección 7.4 ("XGBoost es el modelo maestro") con una comparativa directa de los tres candidatos.
* **Hito/sección:** 7.4 — Selección del modelo maestro.

### `n28_xgb_feature_importance.png`
* **Qué debe verse:** gráfico de barras horizontales con la **importancia por ganancia (gain)** de las features XGBoost, en el mismo orden y con los mismos porcentajes de la tabla de la sección 8.3 (`room_type_entire home/apt` arriba con ~58.89 %).
* **De dónde se obtiene:**
  1. Mismo cuaderno, tras entrenar el pipeline final.
  2. Bloque:
     ```python
     import xgboost as xgb
     xgb.plot_importance(pipeline_xgb.named_steps['model'], importance_type='gain',
                          max_num_features=13, height=0.5)
     plt.title('Feature Importance (Gain) — XGBoost final')
     plt.tight_layout()
     plt.savefig('../capturas/n28_xgb_feature_importance.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** la tabla de feature importance de la sección 8.3 ya cubre los datos, pero una gráfica los hace más legibles. Especialmente útil en una presentación oral del PFC.
* **Hito/sección:** 8.3 — Interpretabilidad.

### `n29_finetuning_convergence.png`
* **Qué debe verse:** gráfico de barras dobles con dos grupos (Madrid y Madrid-Fine-Tuned-NYC) y dos métricas (`MAE` en azul, `R²` en naranja). Los valores deben coincidir con la tabla de la sección 8.5.2 (`MAE: 91.01 → 64.17`, `R²: −0.129 → 0.146`).
* **De dónde se obtiene:**
  1. Abrir `notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb`.
  2. Localizar la sección 6 (Transfer Learning).
  3. Bloque:
     ```python
     import matplotlib.pyplot as plt, pandas as pd
     df_finetune = pd.DataFrame({
         'versión':   ['Base Madrid en NYC', 'Fine-Tuned NYC'],
         'MAE':       [91.01, 64.17],
         'R2':        [-0.129, 0.146]
     })
     fig, ax = plt.subplots(1, 2, figsize=(12, 4))
     ax[0].bar(df_finetune['versión'], df_finetune['MAE'], color=['#457b9d','#1d3557'])
     ax[0].set_title('MAE antes vs después del Fine-Tuning')
     ax[1].bar(df_finetune['versión'], df_finetune['R2'], color=['#e63946','#2a9d8f'])
     ax[1].set_title('R² antes vs después del Fine-Tuning')
     plt.tight_layout()
     plt.savefig('../capturas/n29_finetuning_convergence.png', dpi=150, bbox_inches='tight')
     ```
* **Justificación:** el Δ MAE = −26.84 $ y Δ R² = +0.275 son los KPIs estrella del experimento. Una gráfica los hace autoexplicativos en la defensa del PFC.
* **Hito/sección:** 8.5 — Transfer Learning Madrid → NYC.

---

## 4. Capturas de Despliegue y MLOps (Hitos 3 y 10)

### `n30_hf_space_dashboard.png`
* **Qué debe verse:** la URL pública del Space (`https://<USER_HF>-<NOME_REPOSITORIO>.hf.space`) con la interfaz Gradio del proyecto renderizada. Debe verse el título "Dashboard Inteligente de Tarificación Dinámica", los sliders del inmueble y, en la parte inferior, una predicción de ejemplo con la franja verde y el precio en euros.
* **De dónde se obtiene:**
  1. Ejecutar las celdas 4.1-4.3 del Hito 3 (CI/CD).
  2. Esperar a que HF termine el build (tarda 2-3 minutos).
  3. Abrir la URL del Space en el navegador.
  4. Llenar el formulario con valores plausibles (Centro, Entire home/apt, accommodates=4, etc.) y pulsar "🚀 Calcular Precio Óptimo".
  5. Capturar la pantalla con la tarifa resultante visible.
* **Justificación:** la captura `mimodelo.png` ya cubre una vista, pero una captura **actualizada tras el último despliegue** asegura que el Space sigue vivo. Requisito indispensable para la defensa.
* **Hito/sección:** 8.2 — Aplicación Gradio + HF Spaces.

### `n31_hf_api_predict_request.png`
* **Qué debe verse:** la consola de Python (o un Jupyter) ejecutando el bloque de la sección 10.5 (`gradio_client.Client.predict(...)`) con la respuesta del API en formato HTML visible (`'<div style=...>Tarifa Sugerida</div>...'`).
* **De dónde se obtiene:**
  1. En un cuaderno o `python -c`, ejecutar:
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
  2. Capturar la consola con el output.
* **Justificación:** prueba que el Space no es solo una UI: es una API SOA consumible remotamente. Aporta la perspectiva "ingenieril" del despliegue, no solo la visual.
* **Hito/sección:** 8.2 / 10.5 — Inferencia remota como API.

### `n32_requirements_txt_proof.png`
* **Qué debe verse:** VS Code con el archivo `app/requirements.txt` abierto y su contenido visible (las ~5 dependencias mínimas del Space: `numpy`, `pandas`, `joblib`, `xgboost`, `gradio`).
* **De dónde se obtiene:**
  1. Abrir `app/requirements.txt` en VS Code.
  2. Capturar la ventana completa.
* **Justificación:** refuerza la sección 10.7 demostrando visualmente el desacoplamiento entre el manifiesto del proyecto completo (`requirements.txt` raíz) y el manifiesto del Space (`app/requirements.txt`).
* **Hito/sección:** 10.7 — Manifiestos de dependencias.

### `n33_env_example_proof.png`
* **Qué debe verse:** VS Code con el archivo `.env.example` abierto. Las variables deben verse **sin valores reales** (es la plantilla, no el `.env` real). Junto al archivo, el panel lateral del explorador debe mostrar que el `.env` real está excluido o que aparece en gris por el `.gitignore`.
* **De dónde se obtiene:**
  1. Abrir `.env.example` en VS Code.
  2. Asegurarse de que el panel Source Control muestra `.env` en la lista de "Untracked" o directamente ignorado.
  3. Capturar.
* **Justificación:** evidencia tangible de la gobernanza de credenciales descrita en la sección 10.1.
* **Hito/sección:** 10.1 — Seguridad y gobierno de credenciales.

### `n34_hito2_metrics_console_output.png`
* **Qué debe verse:** la salida de la última celda del Hito 2 donde se imprimen `MAE`, `RMSE` y `R²` para los tres modelos. Idealmente con la tabla markdown del cuaderno renderizada (`| Modelo | MAE | RMSE | R² |`).
* **De dónde se obtiene:**
  1. Abrir `notebooks/hito_02_creación_entrenamiento_y_validación_del_modelo.ipynb`.
  2. Hacer scroll hasta la última celda de evaluación.
  3. Capturar la sección de output con la tabla de resultados finales.
* **Justificación:** a diferencia de `n27` (gráfico), `n34` es la **evidencia textual** que cita literalmente la memoria. Un evaluador puede leerla y verificar los números exactos.
* **Hito/sección:** 7.4 — Selección del modelo maestro.

---

## 5. Resumen: integración con la sección 12 de `decisiones_tecnicas.md`

Para actualizar la sección 12 del documento principal, basta con añadir una segunda tabla con las 20 capturas nuevas propuestas. La tabla original (las 14 capturas existentes) se mantiene tal cual.

### 5.1 Propuesta de párrafo introductorio adicional

> *«Adicionalmente a las 14 capturas que documentan los hitos principales, se incorpora un segundo bloque de **20 evidencias de refuerzo** organizado por fase del pipeline. Cada una de ellas puede regenerarse siguiendo las instrucciones del documento `docs/capturas_adicionales_propuestas.md` y aporta granularidad adicional a las afirmaciones cuantitativas del documento técnico (métricas concretas, esquemas cloud, pruebas de API, etc.).»*

### 5.2 Tabla adicional propuesta (extracto)

| Captura nueva | Fase | Métrica / dato que respalda |
|---|---|---|
| `n15_docker_kafka_running.png` | Infra local | Kafka 7.5.0 KRaft en `localhost:9092`. |
| `n16_kafka_topic_create_or_describe.png` | Infra local | Tópico `busquedas_tiempo_real` con su `PartitionCount`. |
| `n17_rds_security_group_rule.png` | RDS | Apertura del puerto 5432 tras `open_rds_firewall`. |
| `n18_glue_job_console_running.png` | Glue | Job `pfc_proptech_federated_etl` con `Run status = Succeeded`. |
| `n19_glue_crawler_output.png` | Glue | Esquema del Data Catalog. |
| `n20_athena_query_results.png` | Athena | Query `AVG(price) GROUP BY room_type` con resultados. |
| `n21_s3_bucket_layers_browser.png` | S3 | Capas `raw/`, `curated/`, `scripts/`, `athena-results/`. |
| `n22_eda_matriz_nulos.png` | EDA | 42.152 % de nulos en `price`. |
| `n23_eda_outliers_iqr_boxplot.png` | EDA | 4.13 % de outliers y corte IQR segmentado. |
| `n24_eda_distribucion_precio_log.png` | EDA | Normalidad aproximada tras `log(price)`. |
| `n25_eda_top_neighbourhoods.png` | EDA | Top 15 barrios por Target Encoding. |
| `n26_gridsearch_results_table.png` | ML | Top-10 combinaciones XGBoost ordenadas por `mean_test_score`. |
| `n27_model_comparison_metrics.png` | ML | Comparativa R²/MAE/RMSE RF vs XGB vs MLP. |
| `n28_xgb_feature_importance.png` | ML | Importancia por *gain* del modelo maestro. |
| `n29_finetuning_convergence.png` | Transfer | Δ MAE = −26.84 $, Δ R² = +0.275. |
| `n30_hf_space_dashboard.png` | Despliegue | Space público con predicción reciente. |
| `n31_hf_api_predict_request.png` | Despliegue | Llamada SOA al endpoint `/predict`. |
| `n32_requirements_txt_proof.png` | MLOps | Manifiesto mínimo del Space. |
| `n33_env_example_proof.png` | Seguridad | Plantilla `.env.example` y `.gitignore`. |
| `n34_hito2_metrics_console_output.png` | ML | Métricas textuales finales del Hito 2. |

---

## 6. Notas operativas para la captura

1. **Resolución:** todas las capturas se han generado a 150 dpi (`plt.savefig(..., dpi=150, bbox_inches='tight')`). Mantener esa resolución en las nuevas para homogeneidad.
2. **Tamaño de fuente:** usar `plt.rcParams['font.size'] = 11` y `plt.rcParams['figure.figsize'] = (12, 7)` como en el Hito 1.
3. **Tema visual:** `sns.set_theme(style="whitegrid")` en todos los gráficos de seaborn para mantener coherencia con `mimodelo.png`.
4. **Formato de archivo:** PNG con fondo blanco (no transparente) para incrustar bien en Markdown.
5. **Numeración:** no reutilizar números de las 14 capturas existentes (`insideAIR.png` … `VisualizaciónRDSPrueba.png`). Empezar siempre por `n15_` o `n16_` etc.
6. **Bloque de captura rápida:** si no quieres/ puedes levantar la nube AWS, las capturas `n15`, `n22`, `n23`, `n24`, `n25`, `n26`, `n27`, `n28`, `n29` pueden obtenerse **únicamente con los cuadernos en local**, sin gastar créditos del Learner Lab.
7. **Las capturas cloud** (`n17`, `n18`, `n19`, `n20`, `n21`, `n30`, `n31`) requieren una sesión activa de AWS Learner Lab + Space desplegado. Se recomienda hacerlas todas en una misma sesión para ahorrar tiempo.

---

## 7. Priorización recomendada

Si solo puedes generar **5 capturas** (por tiempo/créditos), el orden de prioridad sería:

1. **`n27_model_comparison_metrics.png`** — comparativa RF/XGB/MLP, impacto visual alto.
2. **`n29_finetuning_convergence.png`** — los KPIs estrella del Transfer Learning.
3. **`n22_eda_matriz_nulos.png`** — documenta el problema de nulos del 42 %.
4. **`n18_glue_job_console_running.png`** — cierra el flujo cloud end-to-end.
5. **`n30_hf_space_dashboard.png`** — evidencia viva de que el modelo está en producción.

Estas cinco cubren: infraestructura cloud (n18), EDA (n22), ML (n27), Transfer Learning (n29) y despliegue (n30), los cinco ejes temáticos del documento.
