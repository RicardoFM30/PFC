"""metodos_unir_datasets.py

Módulo para la unificación y consolidación local de los datasets regionales
(Barcelona, Madrid, Málaga, Sevilla) en un único repositorio 'Global'.
Incluye control de existencia previa para optimizar ejecuciones en cascada.
"""

import logging
from pathlib import Path
import pandas as pd

# Configuración de logs integrada
logger = logging.getLogger(__name__)

class DataConsolidationPipeline:
    def __init__(self):
        """
        Inicializa las rutas relativas basándose en la raíz del proyecto.
        Busca los datos dentro del directorio 'datasets/raw'.
        """
        self.ruta_proyecto = Path(__file__).resolve().parent.parent
        self.ruta_base = self.ruta_proyecto / "datasets" / "raw"
        self.ruta_global = self.ruta_base / "Global"
        
        # Garantizar que exista el directorio destino
        self.ruta_global.mkdir(parents=True, exist_ok=True)

    def fusionar_archivo_por_regiones(self, nombre_archivo: str) -> pd.DataFrame:
        """
        Recorre las carpetas regionales, lee el CSV especificado,
        inyecta el metadato de la región y genera un DataFrame unificado.
        """
        datasets_regionales = []
        
        if not self.ruta_base.is_dir():
            raise FileNotFoundError(f"❌ La ruta base de los datos no existe: {self.ruta_base}")
            
        for carpeta_region in self.ruta_base.iterdir():
            if carpeta_region.is_dir() and carpeta_region.name != "Global":
                ruta_csv = carpeta_region / nombre_archivo
                
                if ruta_csv.is_file():
                    logger.info(f"Procesando {nombre_archivo} de la región: {carpeta_region.name}")
                    
                    df = pd.read_csv(ruta_csv, low_memory=False)
                    df['region'] = carpeta_region.name
                    
                    # Normalización de la clave primaria para evitar colisiones estructurales
                    if nombre_archivo == "listings.csv" and 'id' in df.columns:
                        df.rename(columns={'id': 'listing_id'}, inplace=True)
                    
                    datasets_regionales.append(df)
                else:
                    logger.warning(f"⚠️ No se encontró {nombre_archivo} en la carpeta: {carpeta_region.name}")
        
        if not datasets_regionales:
            raise FileNotFoundError(f"❌ No se encontraron archivos '{nombre_archivo}' en las subcarpetas regionales.")
            
        return pd.concat(datasets_regionales, ignore_index=True)

    def guardar_dataset_maestro(self, df: pd.DataFrame, nombre_salida: str):
        """
        Persiste el DataFrame resultante en la carpeta datasets/raw/Global/
        """
        ruta_salida = self.ruta_global / nombre_salida
        logger.info(f"Guardando archivo maestro unificado en: {ruta_salida.relative_to(self.ruta_proyecto)}")
        df.to_csv(ruta_salida, index=False)
        logger.info(f"✔ ¡{nombre_salida} exportado con éxito! Filas totales: {len(df)}")

    def ejecutar_pipeline_fusion_dataset(self, forzar_recalculo: bool = False):
        """
        Orquestador principal. Verifica la existencia de los ficheros 'listings.csv' 
        y 'reviews.csv' en 'Global'. Si ya existen, omite la consolidación 
        a menos que se explicite lo contrario mediante 'forzar_recalculo=True'.
        """
        ruta_listings_final = self.ruta_global / "listings.csv"
        ruta_reviews_final = self.ruta_global / "reviews.csv"

        # Comprobación de la existencia de ambos ficheros maestros
        if ruta_listings_final.is_file() and ruta_reviews_final.is_file() and not forzar_recalculo:
            logger.info("ℹ️ Los datasets consolidados ya existen en la carpeta 'Global'. Se omite la unificación.")
            return

        logger.info("========================================================")
        logger.info("INICIANDO FUSIÓN Y CONSOLIDACIÓN LOCAL HACIA 'GLOBAL'")
        logger.info("========================================================")
        
        # 1. Procesar e integrar listings si no existe o si se fuerza
        if not ruta_listings_final.is_file() or forzar_recalculo:
            try:
                df_listings_master = self.fusionar_archivo_por_regiones("listings.csv")
                self.guardar_dataset_maestro(df_listings_master, "listings.csv")
            except Exception as e:
                logger.error(f"Fallo crítico al procesar los archivos listings: {e}")
        else:
            logger.info("ℹ️ 'listings.csv' ya existe en 'Global'. Saltando etapa.")
            
        logger.info("-" * 56)
        
        # 2. Procesar e integrar reviews si no existe o si se fuerza
        if not ruta_reviews_final.is_file() or forzar_recalculo:
            try:
                df_reviews_master = self.fusionar_archivo_por_regiones("reviews.csv")
                self.guardar_dataset_maestro(df_reviews_master, "reviews.csv")
            except Exception as e:
                logger.error(f"Fallo crítico al procesar los archivos reviews: {e}")
        else:
            logger.info("ℹ️ 'reviews.csv' ya existe en 'Global'. Saltando etapa.")
            
        logger.info("========================================================")
        logger.info("✔ PROCESO CONCLUIDO: Datos verificados en 'Global'")
        logger.info("========================================================")