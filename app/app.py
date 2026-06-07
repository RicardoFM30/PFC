import gradio as gr
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from sklearn.pipeline import Pipeline

# Carga de los artefactos locales dentro del contenedor de Hugging Face
transformador = joblib.load("transformador_maestro.joblib")
modelo_xgb = xgb.XGBRegressor()
modelo_xgb.load_model("modelo_xgboost.json")

pipeline_produccion = Pipeline(steps=[
    ('preprocesamiento', transformador),
    ('modelo_xgb', modelo_xgb)
])

def predecir_tarifa_dinamica(
    neighbourhood_cleansed, room_type, accommodates, bedrooms, 
    beds, bathrooms, minimum_nights, maximum_nights, 
    total_reviews_historicas, total_clicks_acumulados
):
    """Motor de inferencia con alineación matemática estricta (1 al 10)"""
    try:
        # Diccionario de traducción para room_type a entero de 64 bits
        mapeo_room_type = {
            "Entire home/apt": 0,
            "Hotel room": 1,
            "Private room": 2,
            "Shared room": 3
        }
        room_type_numerico = np.int64(mapeo_room_type.get(room_type, 0))

        # Construcción del DataFrame en el ORDEN EXACTO exigido por el preprocesamiento
        datos_entrada = pd.DataFrame([{
            'neighbourhood_cleansed': str(neighbourhood_cleansed),
            'room_type': room_type_numerico,
            'accommodates': float(accommodates),
            'bedrooms': float(bedrooms),
            'beds': float(beds),
            'bathrooms': float(bathrooms),
            'minimum_nights': float(minimum_nights),
            'maximum_nights': float(maximum_nights),
            'total_reviews_historicas': float(total_reviews_historicas),
            'total_clicks_acumulados': float(total_clicks_acumulados)
        }])

        # Ejecutar la predicción y revertir la escala logarítmica (expm1)
        prediccion_escalada = pipeline_produccion.predict(datos_entrada)[0]
        precio_final_euros = np.expm1(prediccion_escalada)

        return f"<div style='text-align: center; padding: 20px; background-color: #f0fdf4; border-radius: 10px; border: 2px solid #22c55e;'><h2 style='color: #166534; margin: 0;'>Tarifa Sugerida</h2><h1 style='color: #15803d; font-size: 48px; margin: 10px 0;'>{precio_final_euros:.2f} €</h1><p style='color: #166534;'>Precio optimizado por noche (Alineación SOA)</p></div>"
    except Exception as e:
        return f"<div style='color: #991b1b; background-color: #fef2f2; padding: 20px; border-radius: 10px;'>⚠️ Error técnico de alineación: {str(e)}</div>"

# Construcción de la interfaz gráfica profesional (Blocks Layout)
tema_profesional = gr.themes.Soft(primary_hue="blue", secondary_hue="slate")
with gr.Blocks(theme=tema_profesional, title="Dynamic Pricing Cloud") as app_visual:
    gr.Markdown("# 🏨 Dashboard Inteligente de Tarificación Dinámica\n### Explotación en Tiempo Real (MLOps)\n---")
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

    # Vinculación del evento respetando escrupulosamente el orden del 1 al 10
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
