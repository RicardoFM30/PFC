import gradio as gr
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb

# Carga desacoplada de los artefactos locales dentro del contenedor de Hugging Face
transformador_maestro = joblib.load("transformador_maestro.joblib")
modelo_xgb_puro = xgb.XGBRegressor()
modelo_xgb_puro.load_model("modelo_xgboost.json")

def predecir_tarifa_dinamica(
    neighbourhood_cleansed, room_type, accommodates, bedrooms, 
    beds, bathrooms, minimum_nights, maximum_nights, 
    total_reviews_historicas, total_clicks_acumulados
):
    """Motor de inferencia desacoplado con inyección de firmas en la matriz RAM"""
    try:
        # Normalización estricta de texto (Evita congelamiento de precio en Cloud)
        barrio_final = str(neighbourhood_cleansed).strip().lower()
        habitacion_final = str(room_type).strip().lower()

        # Construcción de la estructura temporal
        diccionario_entrada = {
            'neighbourhood_cleansed': barrio_final,
            'room_type': habitacion_final,
            'accommodates': float(accommodates),
            'bedrooms': float(bedrooms),
            'beds': float(beds),
            'bathrooms': float(bathrooms),
            'minimum_nights': float(minimum_nights),
            'maximum_nights': float(maximum_nights),
            'total_reviews_historicas': float(total_reviews_historicas),
            'total_clicks_acumulados': float(total_clicks_acumulados)
        }
        datos_entrada = pd.DataFrame([diccionario_entrada])

        # Sincronización estricta de tipos de datos exigida por Scikit-Learn
        datos_entrada['neighbourhood_cleansed'] = datos_entrada['neighbourhood_cleansed'].astype('category')
        datos_entrada['room_type'] = datos_entrada['room_type'].astype('category')

        columnas_numericas = [
            'accommodates', 'bedrooms', 'beds', 'bathrooms', 
            'minimum_nights', 'maximum_nights', 'total_reviews_historicas', 'total_clicks_acumulados'
        ]
        for col in columnas_numericas:
            datos_entrada[col] = datos_entrada[col].astype('float64')

        # Ordenación física de las columnas según las firmas del Hito 2
        orden_entrenamiento = transformador_maestro.feature_names_in_
        datos_entrada = datos_entrada[orden_entrenamiento]

        # Flujo secuencial: Pasar datos por la aduana del transformador
        datos_transformados = transformador_maestro.transform(datos_entrada)

        # INJECCIÓN MATRICIAL: Forzar los bits de la habitación en la matriz transformada
        datos_transformados[0, 1:5] = 0.0
        if habitacion_final == "entire home/apt":
            datos_transformados[0, 1] = 1.0
        elif habitacion_final == "hotel room":
            datos_transformados[0, 2] = 1.0
        elif habitacion_final == "private room":
            datos_transformados[0, 3] = 1.0
        elif habitacion_final == "shared room":
            datos_transformados[0, 4] = 1.0

        # Inferencia con el modelo XGBoost nativo y reversión logarítmica
        prediccion_log = modelo_xgb_puro.predict(datos_transformados)[0]
        precio_final_euros = np.expm1(prediccion_log)

        return f"<div style='text-align: center; padding: 20px; background-color: #f0fdf4; border-radius: 10px; border: 2px solid #22c55e;'><h2 style='color: #166534; margin: 0;'>Tarifa Sugerida</h2><h1 style='color: #15803d; font-size: 48px; margin: 10px 0;'>{precio_final_euros:.2f} €</h1><p style='color: #166534;'>Precio optimizado por noche (Alineación Secuencial Cloud)</p></div>"
    except Exception as e:
        return f"<div style='color: #991b1b; background-color: #fef2f2; padding: 20px; border-radius: 10px;'>⚠️ Error técnico de alineación Cloud: {str(e)}</div>"

# Construcción de la interfaz gráfica profesional (Blocks Layout)
tema_profesional = gr.themes.Soft(primary_hue="blue", secondary_hue="slate")
with gr.Blocks(theme=tema_profesional, title="Dynamic Pricing Cloud") as app_visual:
    gr.Markdown("# 🏨 Dashboard Inteligente de Tarificación Dinámica\n### Explotación en Tiempo Real (Hugging Face Cloud)\n---")
    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown("### 🏠 Estructura del Inmueble")
            c_room_type = gr.Dropdown(choices=["Entire home/apt", "Private room", "Shared room", "Hotel room"], label="Categoría de Estancia", value="Entire home/apt")
            c_neighbourhood = gr.Textbox(label="Barrio (Eje de Ubicación)", placeholder="Ej: Centro, Salamanca...")
            with gr.Row():
                c_bedrooms = gr.Slider(0, 10, step=1, label="Dormitorios", value=1)
                c_bathrooms = gr.Slider(0, 10, step=1, label="Baños", value=1)
            with gr.Row():
                c_beds = gr.Slider(1, 16, step=1, label="Camas", value=1)
                c_accommodates = gr.Slider(1, 16, step=1, label="Capacidad", value=2)
        with gr.Column(scale=1):
            gr.Markdown("### 📈 Parámetros de Mercado")
            with gr.Row():
                c_min_nights = gr.Number(label="Mínimo Noches", value=1)
                c_max_nights = gr.Number(label="Máximo Noches", value=30)
            gr.Markdown("### ⚡ Datos de Demanda (Kafka)")
            c_clicks = gr.Number(label="Clicks en Tiempo Real", value=100)
            c_reviews = gr.Number(label="Reputación (Total Reviews)", value=25)
            btn_predict = gr.Button("🚀 Calcular Precio Óptimo", variant="primary")
    with gr.Row():
        with gr.Column(scale=1):
            html_output = gr.HTML(label="Resultado de Inferencia")

    # Vinculación del evento respetando escrupulosamente el orden posicional del 1 al 10
    btn_predict.click(
        fn=predecir_tarifa_dinamica,
        inputs=[
            c_neighbourhood, c_room_type, c_accommodates, c_bedrooms, 
            c_beds, c_bathrooms, c_min_nights, c_max_nights, 
            c_reviews, c_clicks
        ],
        outputs=html_output,
        api_name="predict"
    )

app_visual.launch()
