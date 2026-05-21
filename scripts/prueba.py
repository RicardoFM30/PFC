import os
from dotenv import load_dotenv

# Forzamos la carga del archivo .env que está en la misma carpeta
load_dotenv()

print("=========================================")
print("🕵️‍♂️ COMPROBACIÓN DE VARIABLES DE ENTORNO")
print("=========================================")

# Intentamos leer la contraseña
password = os.getenv("RDS_PASSWORD")

if password:
    print(f"✅ ¡Éxito! dotenv funciona correctamente.")
    print(f"🔑 Tu contraseña cargada es: {password}")
else:
    print("❌ Error: No se pudo encontrar 'RDS_PASSWORD'.")
    print("👉 Revisa que el archivo se llame exactamente '.env' (sin .txt al final) y esté en esta misma carpeta.")
print("=========================================")