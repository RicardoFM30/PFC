"""añadir_eventos_kafka.py

Productor/Simulador de streaming para Apache Kafka.
Selecciona entidades reales del dataset global y modela ráfagas estocásticas
de telemetría web para evaluar la resiliencia del pipeline de ingesta.
"""

import json
import time
import random
import logging
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# Configuración de trazabilidad
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class KafkaTelemetrySimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "busquedas_tiempo_real"):
        """
        Inicializa las rutas del Data Lake local y la conexión con el Broker de Kafka.
        """
        self.topic = topic
        self.ruta_proyecto = Path(__file__).resolve().parent.parent
        self.ruta_listings_global = self.ruta_proyecto / "datasets" / "raw" / "Global" / "listings.csv"
        
        logger.info(f"Conectando con el Broker de Apache Kafka en: {bootstrap_servers}...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                # Serializamos tanto el valor como la clave para asegurar el particionamiento determinista
                key_serializer=lambda k: str(k).encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            logger.info("✔ Conexión con Apache Kafka establecida con éxito.")
        except Exception as e:
            logger.error(f"❌ No se pudo conectar al Broker de Kafka. Detalle: {e}")
            raise

    def cargar_ids_reales(self) -> list:
        """
        Lee el archivo listings.csv consolidado en la carpeta Global.
        """
        if not self.ruta_listings_global.is_file():
            raise FileNotFoundError(
                f"❌ No se encuentra el dataset maestro en: {self.ruta_listings_global}. "
                f"Asegúrate de ejecutar primero el script de unificación."
            )
        
        df = pd.read_csv(self.ruta_listings_global, usecols=['listing_id'])
        lista_ids = df['listing_id'].dropna().unique().tolist()
        logger.info(f"✔ Dataset global cargado. Se detectaron {len(lista_ids)} inmuebles únicos.")
        return lista_ids

    def simular_rafaga_trafico(self, lista_ids: list):
        """
        Selecciona una propiedad aleatoria y genera una ráfaga de personas 
        entrando a ver el anuncio con interacciones reales de navegación.
        """
        id_seleccionado = random.choice(lista_ids)
        num_eventos = random.randint(30, 100) # Cuántos clics/acciones ocurren en este pico
        
        logger.info(f"\n🚀 [VISITAS] Generando ráfaga de tráfico en el anuncio: {id_seleccionado}")
        logger.info(f"📊 Inyectando {num_eventos} eventos de navegación web en directo...")

        # Lista de acciones reales de usuarios en un portal inmobiliario
        acciones_disponibles = ["ver_anuncio", "click_galeria", "ver_mapa", "leer_reviews", "clic_contactar"]
        pesos_acciones = [0.50, 0.25, 0.15, 0.08, 0.02] # El 50% solo mira, el 2% llega a contactar
        
        dispositivos = ["mobile_app", "desktop_browser", "mobile_browser", "tablet_app"]

        for i in range(1, num_eventos + 1):
            # Simulamos que hay varios usuarios concurrentes (creamos IDs de usuario del 1000 al 9999)
            usuario_anonimo = random.randint(1000, 9999)
            
            # Elegimos una acción basada en la probabilidad real de navegación
            accion_usuario = random.choices(acciones_disponibles, weights=pesos_acciones, k=1)[0]
            
            payload = {
                "event_id": f"evt_{random.randint(100000, 999999)}",
                "listing_id": int(id_seleccionado),
                "user_id": f"usr_{usuario_anonimo}",
                "action": accion_usuario,
                "device": random.choice(dispositivos),
                "timestamp": time.time()
            }
            
            # Seguimos enviando con el listing_id como clave para que Kafka los ordene por casa
            self.producer.send(self.topic, key=id_seleccionado, value=payload)
            
            if i % 25 == 0 or i == num_eventos:
                logger.info(f"   -> [{accion_usuario}] registrado para usuario {usuario_anonimo}...")
            
            time.sleep(0.01) # Simulación fluida de ráfaga concurrente
        
        self.producer.flush()
        logger.info(f"✔ Ráfaga completada. {num_eventos} eventos de clickstream en el tópico '{self.topic}'.")

    def ejecutar_bucle_simulacion(self, intervalos_segundos: int = 4):
        """
        Modo Continuo: Mantiene el simulador activo generando tráfico intermitente.
        """
        try:
            ids_disponibles = self.cargar_ids_reales()
            logger.info("Iniciando bucle de simulación continua. Pulsar Ctrl+C para detener.")
            while True:
                self.simular_rafaga_trafico(ids_disponibles)
                logger.info(f"💤 Esperando {intervalos_segundos} segundos...")
                time.sleep(intervalos_segundos)
        except KeyboardInterrupt:
            logger.info("\n🛑 Simulación detenida por el usuario.")
        finally:
            self.producer.close()

    def ejecutar_carga_masiva_local(self, total_rafagas: int = 50):
        """
        Modo Estrés: Genera miles de eventos seguidos sin esperas 
        para construir el dataset del modelo analítico rápidamente.
        """
        try:
            ids_disponibles = self.cargar_ids_reales()
            logger.info(f"🔥 Iniciando carga masiva local de {total_rafagas} ráfagas continuas...")
            for _ in range(total_rafagas):
                self.simular_rafaga_trafico(ids_disponibles)
            logger.info("✔ ¡Proceso masivo completado! Buffer de Kafka saturado con éxito.")
        finally:
            self.producer.close()

if __name__ == "__main__":
    simulador = KafkaTelemetrySimulator()
    
    # OPCIÓN A: Ejecutar en bucle infinito (Descomenta para usar)
    simulador.ejecutar_bucle_simulacion(intervalos_segundos=4)
    
    # OPCIÓN B: Carga masiva rápida para testear el orquestador (Activada por defecto)
    # simulador.ejecutar_carga_masiva_local(total_rafagas=30)