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
│   └── hito_01_vision_problema.ipynb
├── scripts/
│   └── run_pipeline.py
├── src/
│   ├── aws/
│   ├── kafka/
│   ├── preprocessing/
│   ├── training/
│   └── app/
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

- Hito 1 iniciado y entregable teórico generado en `notebooks/hito_01_vision_problema.ipynb`.
- Justificación técnica de la arquitectura multifuente registrada en `docs/decisiones_tecnicas.md`.
- Script base de pipeline `scripts/run_pipeline.py` implementado con despliegue simulado, ingesta y consolidación hacia S3.

## Historial de acciones realizadas

1. Creación de la estructura de carpetas base del proyecto.
2. Generación del notebook de visión del problema para el Hito 1.
3. Documentación técnica de decisiones arquitectónicas.
4. Implementación del script `run_pipeline.py` con integración de S3, MongoDB y Kafka.
5. Creación del historial de sesión para registrar los avances.

## Próximos pasos

- Desarrollar el documento `docs/arquitectura.md` con el diagrama y flujo de datos.
- Continuar con el contenido de los hitos siguientes en `notebooks/`.
- Implementar componentes de `src/` para AWS, Kafka, preprocessing, training y la app Gradio.

## Contacto

Este proyecto está en desarrollo como parte de un Trabajo de Fin de Curso (PFC) y será mantenido con enfoque de ingeniería de datos de nivel empresarial. Cualquier cambio adicional se documentará en el historial de sesión.
