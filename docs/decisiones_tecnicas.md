# Decisiones Técnicas del Proyecto PropTech: Precio Dinámico y Reputación

## 1. Introducción

Este documento justifica la arquitectura híbrida multifuente propuesta para el proyecto de fin de curso. El diseño combina:

- almacenamiento relacional,
- almacenamiento semiestructurado,
- ingestión en streaming,
- repositorio único de datos (Data Lake),

para soportar un sistema empresarial de precios dinámicos y reputación en alquileres vacacionales.

## 2. Amazon RDS para datos operativos rígidos

### 2.1 Naturaleza del dato

Los datos de los inmuebles —como `property_type`, `accommodates`, `bathrooms`, `bedrooms` y `beds`— son estructurados y forman parte del catálogo operativo.

### 2.2 Razones técnicas

- **Consistencia ACID**: Amazon RDS garantiza integridad transaccional, evitando anomalías en información crítica de propiedades.
- **Modelado relacional**: los datos maestros de los listings se benefician de claves primarias, foráneas, normalización y consultas SQL complejas.
- **Escalabilidad gestionada**: RDS ofrece réplicas de lectura, backups automáticos y conmutación por error, necesario para soluciones empresariales.

## 3. MongoDB Atlas para reseñas y texto libre

### 3.1 Naturaleza del dato

Las reseñas de huéspedes son documentos semiestructurados con campos variables, texto libre y metadata opcional. Esta información no encaja bien en un esquema relacional rígido.

### 3.2 Razones técnicas

- **Flexibilidad de esquema**: MongoDB Atlas permite cambios de estructura sin migraciones costosas.
- **Eficiencia en BSON**: el almacenamiento de reseñas con texto y metadatos es natural en un modelo de documentos.
- **Capacidades de búsqueda de texto**: puede indexar contenido textual y soportar consultas por sentimiento o temas.
- **Baja latencia en lectura/escritura**: ideal para acceder rápidamente a señales de reputación que alimentan el modelo.

## 4. Apache Kafka para ingestión en streaming

### 4.1 Naturaleza del dato

Los eventos de demanda, como `ratio_busquedas_zona`, son métricas dinámicas que cambian en tiempo real. Se requiere un flujo continuo de telemetría para actualizaciones de pricing.

### 4.2 Razones técnicas

- **Buffer de eventos**: Kafka desacopla productores de búsqueda y consumidores analíticos, evitando picos de carga en sistemas destino.
- **Persistencia temporal**: los mensajes quedan almacenados en Kafka, facilitando reprocesos y auditoría.
- **Escalabilidad horizontal**: soporta alto volumen de eventos y múltiples consumidores.
- **Desacoplamiento de sistemas**: Kafka actúa como capa intermedia entre front-end, telemetría y data lake.

## 5. Amazon S3 como repositorio único (Data Lake)

### 5.1 Rol estratégico

Amazon S3 se utiliza como repositorio central donde convergen datos históricos, conjuntos procesados y artefactos de machine learning.

### 5.2 Razones técnicas

- **Costo eficiente**: almacenamiento escalable para grandes volúmenes sin esquema fijo.
- **Durabilidad y disponibilidad**: alta fiabilidad para el data lake de la solución.
- **Compatibilidad analítica**: soporta formatos como Parquet, JSON y CSV para múltiples consumidores.
- **Separación entre producción y análisis**: evita mezclar datos operativos con procesos analíticos.

## 6. AWS Glue y Athena para procesos ELT desacoplados

### 6.1 AWS Glue

- **Catalogación de metadatos**: Glue puede inferir esquemas y crear catálogo para datos en S3.
- **Transformaciones ELT**: facilita limpieza, normalización y enriquecimiento de datos.
- **Ejecución programable**: permite orquestar procesos batch periódicos o bajo demanda.

### 6.2 Amazon Athena

- **Consultas SQL sobre S3**: Athena permite análisis interactivo sin mover los datos.
- **Schema-on-read**: los datos se interpretan en el momento de la consulta, aumentando flexibilidad.
- **Integración con Glue Data Catalog**: centraliza metadatos y facilita el descubrimiento de datasets.

## 7. Sinergia de la arquitectura híbrida

La combinación de estas tecnologías produce una plataforma robusta que aprovecha lo mejor de cada paradigma:

- Amazon RDS para datos transaccionales con garantías de consistencia.
- MongoDB Atlas para reseñas y datos cualitativos con flexibilidad de esquema.
- Apache Kafka para captura y procesamiento de demanda en streaming.
- Amazon S3 como capa central de datos, con AWS Glue/Athena para análisis y gobernanza.

## 8. Conclusión

La arquitectura propuesta es sólida desde una perspectiva técnica y de negocio. Facilita un flujo de datos cohesivo, integra múltiples fuentes y garantiza escalabilidad desde una prueba de concepto hasta una solución empresarial. De este modo, la capa de machine learning se alimenta de datos fiables, flexibles y actualizados.