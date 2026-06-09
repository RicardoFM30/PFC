# Guía de Obtención de las 14 Capturas Originales de `/capturas`

> **Propósito:** este documento describe paso a paso cómo se obtienen las **14 capturas que ya existen** en la carpeta `capturas/` del proyecto, de modo que cualquiera pueda regenerarlas si se pierden o si necesita rehacerlas para una nueva entrega.
>
> Para cada captura se especifica:
> 1. **Nombre del archivo** (tal y como aparece en `capturas/`).
> 2. **Qué se ve en la imagen** (criterio de aceptación).
> 3. **Origen / flujo de obtención** (URL, comando, celda de cuaderno, script).
> 4. **Requisitos previos** (lo que debe estar levantado/activo antes de capturar).
> 5. **Orden sugerido de captura** (respetar dependencias entre capturas).

---

## 📋 Índice de capturas

| # | Archivo | Tipo de fuente | Hito/Fase | Bloque en este documento |
|---|---|---|---|---|
| 1 | `insideAIR.png` | Web externa | Hito 0 — datos origen | §1 |
| 2 | `rdsAWSPrueba.png` | Consola AWS | 3.1 — RDS creado | §2 |
| 3 | `VisualizaciónRDSPrueba.png` | Cliente SQL / S3 / Jupyter | 3.1 — RDS poblado | §3 |
| 4 | `atlasPrueba.png` | Atlas Web UI | 3.2 — MongoDB poblado | §4 |
| 5 | `kafkaPrueba.png` | Consola Kafka / Docker logs | 4 — Kafka | §5 |
| 6 | `capturaGeneradoraKafka.png` | Terminal local | 4.4 — Simulador | §6 |
| 7 | `eventosKafka.png` | Consola Kafka / cliente | 4.2 — Topic con eventos | §7 |
| 8 | `consolidadoAWSPrueba.png` | Consola AWS S3 / Glue logs | 5.3 — Glue ejecutado | §8 |
| 9 | `contenidoAWSPrueba.png` | Consola Athena / S3 | 5.3 — Athena query | §9 |
| 10 | `modelo11.png` | Navegador web (HF Spaces) | 8.4 — Benchmark | §10 |
| 11 | `modelo12.png` | Navegador web (HF Spaces) | 8.4 — Benchmark | §10 |
| 12 | `modelo21.png` | Navegador web (HF Spaces) | 8.4 — Benchmark | §10 |
| 13 | `modelo22.png` | Navegador web (HF Spaces) | 8.4 — Benchmark | §10 |
| 14 | `mimodelo.png` | Navegador web (HF Space propio) | 8.2 — Despliegue propio | §11 |

> **Convención:** los nombres de archivo se mantienen tal cual existen en el repositorio (respetando la tilde en `VisualizaciónRDSPrueba.png`).

---

## 1. `insideAIR.png` — Portal de datos origen

* **Qué se ve:** la página web del proyecto *Inside Airbnb* ([http://insideairbnb.com/get-the-data/](http://insideairbnb.com/get-the-data/)) con el listado de ciudades y archivos CSV/GeoJSON descargables, idealmente con la sección de **Spain** desplegada mostrando los enlaces a Madrid, Barcelona, Málaga y Sevilla.
* **Requisitos previos:** ninguno (solo conexión a Internet).
* **Origen / flujo:**
  1. Abrir el navegador (Chrome o Edge).
  2. Visitar `http://insideairbnb.com/get-the-data/`.
  3. Hacer scroll hasta la sección **"Get the Data"** o el listado de ciudades por país.
  4. Capturar la pantalla con la mayor cantidad de enlaces a CSVs visibles.
* **Orden sugerido:** esta es la **primera captura** que debe hacerse (no depende de nada del proyecto).
* **Si quieres rehacerla idéntica:** usa una resolución de al menos 1440×900 y captura la pantalla completa (no recortes) para que se vean todos los países.

---

## 2. `rdsAWSPrueba.png` — Consola AWS: instancia RDS PostgreSQL

* **Qué se ve:** la consola web de **Amazon RDS** → **Databases** con la fila correspondiente a la instancia `servidor-pfc-poc` (o el nombre que figure en `RDS_INSTANCE_ID` del `.env`) en estado **Available**, con su **Endpoint**, **Engine** (postgres 15.4), **Class** (`db.t3.micro`) y **Region** visibles.
* **Requisitos previos:**
  * Tener el archivo `.env` configurado con `RDS_INSTANCE_ID` válido.
  * Haber ejecutado `scripts/pipeline_inicio_xauto_Glue_Athena.py` (fase 0.1) o haber lanzado manualmente `python scripts/pipeline_inicio_prototipo_arquitectura.py` (fase 1) hasta ver el mensaje `✔ ¡Instancia RDS creada con éxito!`.
  * Sesión activa del **AWS Learner Lab** con créditos.
* **Origen / flujo:**
  1. Ir a [https://console.aws.amazon.com/rds/home](https://console.aws.amazon.com/rds/home).
  2. Menú lateral → **Databases** → **DB Instances**.
  3. Verificar que la instancia está en estado **Available** (cuadrado verde).
  4. Pulsar sobre el **DB identifier** para abrir el detalle.
  5. Capturar la pestaña **Connectivity & security** (aquí se ve el endpoint y el security group). Es la vista más informativa.
* **Orden sugerido:** segunda captura, tras levantar la infraestructura del Hito 0.5 / pipeline PoC.

---

## 3. `VisualizaciónRDSPrueba.png` — Tabla `listings_master` con datos

* **Qué se ve:** una vista tabular de la tabla `listings_master` de RDS con sus primeras filas (id, property_type, room_type, accommodates, bedrooms, beds, price, etc.) y el conteo total de registros.
* **Requisitos previos:** instancia RDS en estado `Available` Y sembrado de datos completado (fase 0.3 del orquestador industrial, o fase 2 del PoC).
* **Origen / flujo (3 alternativas, usar la que prefieras):**
  * **Opción A — Cliente SQL local (recomendada por simplicidad):**
    1. Abrir **DBeaver**, **pgAdmin** o **TablePlus**.
    2. Conectar con los datos del `.env`:
       ```
       Host: <RDS_ENDPOINT>
       Port: 5432
       Database: proptechdb
       Username: postgres
       Password: <RDS_PASSWORD>
       ```
    3. Ejecutar `SELECT * FROM listings_master LIMIT 20;`
    4. Capturar la vista de resultados.
  * **Opción B — Cuaderno Hito 1 (si no tienes cliente SQL):**
    1. Abrir `notebooks/hito_01_analisis_exploratorio_y_preparación_de_datos.ipynb`.
    2. Localizar la celda que imprime `df_auditoria.head()` tras la ingesta.
    3. Capturar el output de la celda con la tabla renderizada.
  * **Opción C — Consola S3 (post-Glue):**
    1. Descargar uno de los Parquet de `s3://<bucket>/curated/dataset_proptech_master/`.
    2. Abrirlo con DuckDB CLI: `duckdb -c "SELECT * FROM 'part-*.parquet' LIMIT 20"`.
    3. Capturar la salida de la consola.

---

## 4. `atlasPrueba.png` — MongoDB Atlas: colección `reviews_raw`

* **Qué se ve:** la interfaz web de **MongoDB Atlas** con la vista **Collections** mostrando la colección `reviews_raw` (o `reviews_ficticias` en el PoC) con el número de documentos y al menos un documento de ejemplo desplegado (con campos `listing_id`, `comments`, `score_sentimiento`, etc.).
* **Requisitos previos:** clúster Atlas creado y sembrado (fase 0.3 del orquestador, o fase 2 del PoC).
* **Origen / flujo:**
  1. Ir a [https://cloud.mongodb.com/](https://cloud.mongodb.com/).
  2. Menú lateral → **Database** → seleccionar el cluster `pfc_proptech` (o el nombre del PoC).
  3. Click en **Browse Collections**.
  4. Seleccionar la base de datos `pfc_proptech` (o `pfc_poc`).
  5. Click en la colección `reviews_raw` (o `reviews_ficticias`).
  6. Capturar la vista con un documento desplegado.
* **Detalle importante:** Atlas tiene whitelist de IPs. Si tu IP actual no está, añadir `0.0.0.0/0` temporalmente en **Network Access** antes de capturar.

---

## 5. `kafkaPrueba.png` — Consola del broker Kafka local

* **Qué se ve:** la salida de la terminal tras levantar el contenedor Docker de Kafka, idealmente con los logs del KRaft controller mostrando el `Cluster ID = MkU3OEVBNTcwNTJENDM2Qk` y el broker escuchando en `PLAINTEXT://0.0.0.0:9092`.
* **Requisitos previos:** Docker Desktop corriendo.
* **Origen / flujo:**
  1. Abrir PowerShell o terminal.
  2. `cd C:\Users\Ric\Desktop\PFC\docker`
  3. `docker-compose up -d`
  4. `docker logs pfc_kafka_proptech` (mostrará las líneas de arranque del broker KRaft).
  5. Capturar la ventana con los logs.
* **Variante equivalente:** captura de **Docker Desktop** → Containers → click en `pfc_kafka_proptech` → pestaña **Logs**.

---

## 6. `capturaGeneradoraKafka.png` — Simulador ejecutándose

* **Qué se ve:** la terminal con el script `scripts/añadir_eventos_kafka.py` en plena ejecución, mostrando los `logger.info` con líneas como `🚀 [VISITAS] Generando ráfaga de tráfico en el anuncio: 12345` y `   -> [ver_anuncio] registrado para usuario 6789...`.
* **Requisitos previos:** Kafka local levantado (captura anterior #5).
* **Origen / flujo:**
  1. Abrir una terminal.
  2. `cd C:\Users\Ric\Desktop\PFC`
  3. Activar el entorno virtual (`.\venv\Scripts\activate`).
  4. `python scripts/añadir_eventos_kafka.py`
  5. Esperar a que aparezcan al menos 2-3 ráfagas con sus mensajes `INFO`.
  6. Capturar la ventana completa con la salida del simulador.
* **Detalle:** si el script termina muy rápido (modo `carga_masiva`), se puede usar el modo `bucle_simulacion` descomentándolo en el `__main__` del script (línea 136).

---

## 7. `eventosKafka.png` — Topic con eventos

* **Qué se ve:** la herramienta de inspección de Kafka (Conduktor, Offset Explorer, Kafdrop o la propia CLI) mostrando el tópico `busquedas_tiempo_real` con:
  * Listado de particiones.
  * Mensajes consumidos con su `key` (`listing_id`) y su `value` JSON (`event_id`, `action`, `device`, `timestamp`).
* **Requisitos previos:** Kafka local levantado + simulador ejecutado al menos una vez (capturas #5 y #6).
* **Origen / flujo (3 alternativas):**
  * **Opción A — CLI dentro del contenedor:**
    1. `docker exec -it pfc_kafka_proptech bash`
    2. `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic busquedas_tiempo_real --from-beginning --max-messages 5 --property print.key=true --property key.separator=" | "`
    3. Capturar la salida de la consola.
  * **Opción B — Conduktor (gratuito, [https://www.conduktor.io/download/](https://www.conduktor.io/download/)):**
    1. Crear conexión a `localhost:9092`.
    2. Abrir el tópico `busquedas_tiempo_real`.
    3. Click en **Consume**.
    4. Capturar la vista con los mensajes.
  * **Opción C — Kafdrop (UI web):**
    1. Levantar Kafdrop con `docker run -d -p 9000:9000 --network host obsidiandynamics/kafdrop`.
    2. Abrir `http://localhost:9000`.
    3. Click en el tópico `busquedas_tiempo_real`.
    4. Capturar la vista de mensajes.

---

## 8. `consolidadoAWSPrueba.png` — Resultado del Glue Job

* **Qué se ve:** la consola de **Amazon S3** (o la consola de Glue → Runs) mostrando los archivos `part-*.parquet` en la ruta `s3://<bucket>/curated/dataset_proptech_master/`, idealmente con un log de Glue en la parte inferior confirmando que el job terminó con `SUCCEEDED`.
* **Requisitos previos:** Glue Job ejecutado con éxito (fase 2 del orquestador industrial).
* **Origen / flujo:**
  1. AWS Console → S3 → seleccionar el bucket del Data Lake.
  2. Navegar a `curated/dataset_proptech_master/`.
  3. Capturar la vista con los `~20` archivos `part-00000-xxx.snappy.parquet`.
  * **Variante más rica:** AWS Console → AWS Glue → ETL jobs → `pfc_proptech_federated_etl` → pestaña **Runs** → seleccionar el último Run con estado **Succeeded** → capturar la vista con `Execution time` y el enlace a los CloudWatch logs.
* **Tip de consistencia:** si quieres que la captura sea autoexplicativa, abre dos pestañas y captura un *split screen* (S3 a la izquierda, Glue a la derecha).

---

## 9. `contenidoAWSPrueba.png` — Query en Athena sobre el Data Lake

* **Qué se ve:** la consola de **Amazon Athena** con la query `SELECT * FROM proptech_analytics_db.dataset_master LIMIT 20;` ya ejecutada y la tabla de resultados visible.
* **Requisitos previos:**
  * Glue Job ejecutado con éxito (paso #8).
  * Tabla externa en Athena creada (fase 3 del orquestador).
* **Origen / flujo:**
  1. AWS Console → Athena → Query editor.
  2. En el panel **Database**, seleccionar `proptech_analytics_db`.
  3. En el editor, escribir: `SELECT * FROM dataset_master LIMIT 20;`
  4. Pulsar **Run**.
  5. Esperar a que el estado pase a `SUCCEEDED` y aparezca la pestaña **Results**.
  6. Capturar la pantalla con el editor + resultados visibles.
* **Tip:** la captura es más impactante si se añade `ORDER BY price DESC` para ver los alojamientos más caros arriba.

---

## 10. `modelo11.png` · `modelo12.png` · `modelo21.png` · `modelo22.png` — Benchmark de Spaces competidores

* **Qué se ve:** capturas de dos Spaces públicos de Hugging Face usados como referencia en el benchmark de la sección 8.4 del documento de decisiones técnicas:
  * `modelo1*.png` → Space del usuario `ThomasH007` (referencia 1).
  * `modelo2*.png` → Space del usuario `anchit48` (referencia 2).
  * El sufijo `1`/`2` distingue dos pantallas/vistas del mismo Space (formulario vs. resultado, o vista inicial vs. inferencia).
* **Requisitos previos:** ninguno (son URLs públicas).
* **Origen / flujo:**
  1. Ir a [https://huggingface.co/spaces](https://huggingface.co/spaces) y buscar "airbnb price" o "airbnb dynamic pricing".
  2. Identificar los Spaces de `ThomasH007` y `anchit48` con temática similar.
  3. Abrir el Space 1 → capturar la vista inicial del formulario (`modelo11.png`).
  4. Rellenar el formulario con valores plausibles → capturar el resultado de la predicción (`modelo12.png`).
  5. Repetir para el Space 2 (`modelo21.png` y `modelo22.png`).
* **Importante:** los nombres de usuario concretos pueden haber cambiado. Si los Spaces referenciados ya no existen o han sido renombrados, sustituir por otros dos Spaces equivalentes que cubran el mismo dominio (predicción de precios de Airbnb). Actualizar el documento `decisiones_tecnicas.md` (sección 8.4) con los nuevos identificadores.
* **URLs candidatas (a verificar en el momento de rehacer la captura):**
  * `https://huggingface.co/spaces/ThomasH007/airbnb-price-prediction` (verificar disponibilidad).
  * `https://huggingface.co/spaces/anchit48/airbnb-price-prediction` (verificar disponibilidad).
* **Orden sugerido:** estas cuatro capturas se hacen en bloque, una detrás de otra, en una sola sesión de navegación. Reservar 10-15 minutos.
* **Detalle para `modelo12.png` y `modelo22.png`:** en la captura del resultado debe verse claramente la **predicción numérica** y, si es posible, los inputs que la generaron. Esto permite la comparación 1-a-1 con `mimodelo.png` (sección 11).
* **Detalle del benchmark:** el `modelo1*.png` (ThomasH007) suele mostrar campos como `Room Type`, `Accommodates`, `Bathrooms`, `Cancellation Policy` y `Cleaning Fee`. El `modelo2*.png` (anchit48) suele simplificar a `Room Type`, `Accommodates`, `Bathrooms`, `Bedrooms`, `Beds`. Capturar **ambos** formularios con los campos rellenos con los mismos valores que uses en `mimodelo.png` para que la comparativa sea coherente.

---

## 11. `mimodelo.png` — Space propio desplegado

* **Qué se ve:** la URL pública `https://<USER_HF>-<NOME_REPOSITORIO>.hf.space` con la interfaz Gradio del proyecto renderizada, mostrando el título **"Dashboard Inteligente de Tarificación Dinámica"**, los controles (sliders de dormitorios/baños/camas/capacidad, dropdown de tipo de estancia, textbox de barrio, números de noches y total reviews/clicks) y, en la parte inferior, el resultado de una predicción reciente con la franja verde y el precio en euros (formato `XX.XX €`).
* **Requisitos previos:**
  * Haber ejecutado las celdas 4.1-4.3 del Hito 3 (CI/CD hacia Hugging Face).
  * Space `https://<USER_HF>-<NOME_REPOSITORIO>` en estado **Running** (no "Building" ni "Sleeping").
  * Variables `USER_HF`, `NOME_REPOSITORIO`, `TOKEN_HF` correctamente configuradas en el `.env`.
* **Origen / flujo:**
  1. **Desplegar el Space (si no lo está):**
     1. Abrir `notebooks/hito_03_representacion_grafica_y_prueba_de_modelo.ipynb`.
     2. Ejecutar las celdas de la sección 4 (CI/CD) hasta ver el `✔ Space publicado correctamente`.
     3. Esperar 2-3 minutos a que HF termine el build del contenedor.
  2. **Verificar el Space:**
     1. Abrir `https://huggingface.co/spaces/<USER_HF>/<NOME_REPOSITORIO>`.
     2. Verificar que el estado es **Running** (no **Building** ni **Sleeping**).
  3. **Capturar la vista inicial:**
     1. Abrir la URL pública del Space: `https://<USER_HF>-<NOME_REPOSITORIO>.hf.space`.
     2. Capturar la vista por defecto (formulario vacío).
  4. **Capturar la inferencia:**
     1. Rellenar los campos con valores de ejemplo:
        * `Categoría de Estancia` = **Entire home/apt**
        * `Barrio (Eje de Ubicación)` = **Centro** (o el que se quiera)
        * `Dormitorios` = 2
        * `Baños` = 1
        * `Camas` = 2
        * `Capacidad` = 4
        * `Mínimo Noches` = 2
        * `Máximo Noches` = 30
        * `Clicks en Tiempo Real` = 150
        * `Reputación (Total Reviews)` = 42
     2. Pulsar el botón **"🚀 Calcular Precio Óptimo"**.
     3. Esperar 1-2 segundos a que aparezca la franja verde con el precio.
     4. Capturar la pantalla **completa** (no recortes) para que se vean los inputs y el output.
* **Importante para reproducibilidad:** los valores exactos del barrio, capacidad y reviews influyen en la predicción. Si quieres que la captura tenga siempre el mismo precio, anota los valores usados en la primera captura y reutilízalos.

---

## 12. Orden óptimo de captura (resumido)

Para minimizar tiempo y evitar tener que repetir capturas, sigue este orden:

1. **`insideAIR.png`** — primero, sin dependencias.
2. **Levantar toda la infraestructura cloud y local:**
   * Levantar Docker (`kafkaPrueba.png`).
   * Ejecutar `pipeline_inicio_xauto_Glue_Athena.py` o `pipeline_inicio_prototipo_arquitectura.py`.
3. **`rdsAWSPrueba.png`** — instancia en estado Available.
4. **`VisualizaciónRDSPrueba.png`** — una vez la tabla `listings_master` está poblada.
5. **`atlasPrueba.png`** — una vez la colección Atlas está poblada.
6. **`capturaGeneradoraKafka.png`** — arrancando el simulador.
7. **`eventosKafka.png`** — consumiendo el tópico con mensajes.
8. **`consolidadoAWSPrueba.png`** — tras el Glue Job.
9. **`contenidoAWSPrueba.png`** — tras crear la tabla en Athena.
10. **`mimodelo.png`** — tras desplegar el Space.
11. **`modelo11.png` · `modelo12.png` · `modelo21.png` · `modelo22.png`** — en bloque, en una sola sesión de navegación.

**Tiempo total estimado:** 1-2 horas (depende del Learner Lab y de la primera vez que se levanta RDS, que tarda 5-10 min en pasar a `available`).

---

## 13. Convenciones de captura

Para que las 14 capturas tengan una apariencia homogénea:

* **Resolución mínima:** 1440×900. Ideal 1920×1080.
* **Formato:** PNG (no JPG, no WebP).
* **Tema del navegador:** claro, sin modo oscuro, para que las consolas AWS/Atlas/HF se vean legibles.
* **Cursor del ratón:** fuera del área de información (esquina inferior derecha) para que no tape texto.
* **Recortes:** no recortes pequeños; capturar la pantalla completa o, como mínimo, la pestaña/ventana relevante sin la barra de tareas.
* **Datos sensibles:** verificar que las capturas no contienen credenciales. Especialmente:
  * `rdsAWSPrueba.png`: comprobar que el endpoint está visible (es público) pero el **password no**.
  * `atlasPrueba.png`: capturar sin mostrar la cadena de conexión.
  * `mimodelo.png`: el Space público no muestra nada sensible, pero capturar con un barrio de ejemplo, no con datos reales.

---

## 14. Problemas frecuentes y soluciones

| Problema | Causa probable | Solución |
|---|---|---|
| La instancia RDS no aparece en la consola | Learner Lab cerrado o credenciales expiradas | Relanzar el Learner Lab, copiar de nuevo `AWS_ACCESS_KEY_ID` y `AWS_SESSION_TOKEN` al `.env` y refrescar la consola. |
| MongoDB Atlas no carga la colección | IP actual no está en la whitelist | Network Access → Add IP `0.0.0.0/0` temporalmente. |
| El simulador Kafka no inyecta mensajes | El broker no está en `localhost:9092` | Verificar `docker ps` y `KAFKA_BOOTSTRAP_SERVERS` en el `.env`. |
| El Glue Job queda en `Running` más de 15 min | Cluster frío de Spark en Learner Lab | Esperar o cancelar y volver a lanzar. La primera ejecución siempre tarda más. |
| Athena dice "Table not found" | La tabla externa no se creó | Re-ejecutar la fase 3 del orquestador (`fase_3_automatizar_athena`). |
| El Space HF queda en `Building` o `Sleeping` | Primer build largo o inactividad | Refrescar la página. Si sigue en `Sleeping`, ejecutar `gradio_client.Client.predict(...)` para "despertarlo". |
| `modelo11.png` y similares no se corresponden con `ThomasH007`/`anchit48` | Los Spaces han cambiado de nombre o se han borrado | Buscar Spaces equivalentes con el buscador de HF y actualizar la sección 8.4 del documento de decisiones técnicas. |
| Las capturas salen en idioma distinto al español | La consola AWS/Atlas detecta el idioma del navegador | Forzar idioma en la URL: `https://console.aws.amazon.com/?lang=es` o cambiar el idioma en **Settings → Language**. |

---

## 15. Plantilla de checklist para el día de captura

Copia y pega esta checklist en tu `README` personal o en una nota al hacer la sesión de capturas:

```text
[ ] 1. insideAIR.png                  (navegador)
[ ] 2. docker-compose up -d            (PowerShell)
[ ] 3. kafkaPrueba.png                 (docker logs)
[ ] 4. pipeline_inicio_xauto_...       (terminal)
[ ] 5. rdsAWSPrueba.png                (consola AWS)
[ ] 6. VisualizaciónRDSPrueba.png      (cliente SQL / Hito 1)
[ ] 7. atlasPrueba.png                 (Atlas web)
[ ] 8. capturaGeneradoraKafka.png      (terminal)
[ ] 9. eventosKafka.png                (Conduktor / CLI)
[ ]10. consolidadoAWSPrueba.png        (S3 / Glue console)
[ ]11. contenidoAWSPrueba.png          (Athena console)
[ ]12. mimodelo.png                    (HF Space propio)
[ ]13. modelo11.png                    (HF Space ThomasH007)
[ ]14. modelo12.png                    (HF Space ThomasH007)
[ ]15. modelo21.png                    (HF Space anchit48)
[ ]16. modelo22.png                    (HF Space anchit48)
```

Una vez marcadas las 16 casillas, todas las capturas de la sección 12 de `decisiones_tecnicas.md` están regeneradas y son coherentes con el estado actual del proyecto.
