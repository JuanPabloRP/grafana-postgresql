import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client
from sqlalchemy import create_engine, text
import os
import time


# ============================
# 1️⃣ CONFIGURACIÓN SUPABASE <-------- Versión vieja - NO USAR ✖️
# ============================
SUPABASE_URL = "https://vxhmujfzqmvecjrksvus.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ4aG11amZ6cW12ZWNqcmtzdnVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI1NTQ3MDYsImV4cCI6MjA3ODEzMDcwNn0.sNR2qV9AU815TdS9TuARj-utYht4j1tF4Tv8VW-61N4"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================
# 1️⃣ CONFIGURACIÓN LOCAL <-------- Versión NUEVA  ✅🥵👍 
# ============================
DB_URL = "https://vxhmujfzqmvecjrksvus.supabase.co"
DB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ4aG11amZ6cW12ZWNqcmtzdnVzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI1NTQ3MDYsImV4cCI6MjA3ODEzMDcwNn0.sNR2qV9AU815TdS9TuARj-utYht4j1tF4Tv8VW-61N4"
db_key: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


EXCEL_PATH = "SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm"

# ============================
# 2️⃣ LECTURA Y LIMPIEZA
# ============================

print("📂 Leyendo archivo Excel...")
df = pd.read_excel(EXCEL_PATH,sheet_name="Base De Datos", skiprows=8)

print("🧾 Columnas detectadas:")
print(list(df.columns))
df.info()


# Normalizar nombres de columnas (quitar saltos y espacios)
df.columns = df.columns.str.replace("\n", " ").str.strip()

# Quitar filas vacías
df = df.dropna(subset=["Fecha", "Maquina"])

# Convertir tipos básicos
df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
df["Mes"] = df["Mes"].astype(str)
df["Año"] = df["Año"].astype(int)

# Reemplazar NaN por None
df = df.replace({np.nan: None})

print(f"✅ Datos cargados ({len(df)} filas, {len(df.columns)} columnas)\n")

# ============================
# 3️⃣ FUNCIONES AUXILIARES
# ============================

def obtener_o_insertar(tabla, campo_nombre, valor):
    """Verifica si un valor ya existe en Supabase; si no, lo inserta."""
    if not valor:
        return None

    id_campo = "id_" + tabla[:-1].lower()  # ej: Maquinas -> id_maquina

    # Buscar si ya existe
    response = supabase.table(tabla).select("*").eq("nombre", valor).execute()

    if response.data:
        return response.data[0][id_campo]
    else:
        # Insertar si no existe
        insert_resp = supabase.table(tabla).insert({"nombre": valor}).execute()
        if insert_resp.data:
            return insert_resp.data[0][id_campo]
        else:
            print(f"⚠️ No se pudo insertar {valor} en {tabla}")
            return None


# ============================
# 4️⃣ TABLAS DIMENSIONALES
# ============================

print("🧱 Procesando dimensiones únicas...")

maquina_map, operario_map, referencia_map = {}, {}, {}

for _, row in df.iterrows():
    maq = row["Maquina"]
    ope = row["Operario"]
    ref = row["Referencia"]

    if maq and maq not in maquina_map:
        maquina_map[maq] = obtener_o_insertar("maquinas", "nombre", maq)
    if ope and ope not in operario_map:
        operario_map[ope] = obtener_o_insertar("operarios", "nombre", ope)
    if ref and ref not in referencia_map:
        referencia_map[ref] = obtener_o_insertar("referencias", "nombre", ref)

print(f"✅ Dimensiones procesadas: {len(maquina_map)} máquinas, {len(operario_map)} operarios, {len(referencia_map)} referencias\n")

# ============================
# 5️⃣ INSERTAR REGISTROS DE PRODUCCIÓN
# ============================

registros_insertados = []

print("📦 Insertando registros de producción...")

for i, row in df.iterrows():
    registro = {
        "fecha": row["Fecha"].date().isoformat() if row["Fecha"] else None,
        "mes": row["Mes"],
        "año": row["Año"],
        "maquina_id": maquina_map.get(row["Maquina"]),
        "operario_id": operario_map.get(row["Operario"]),
        "referencia_id": referencia_map.get(row["Referencia"]),
        "pacas_producidas": row["Pacas producidas"],
        "horas_trabajadas": row["Horas trabajadas"],
        "horas_no_trabajadas": row["Horas no trabajadas"],
        "turno": row["Turno"],
        "tiempo_total_paros": row["Tiempo de Paro"],
        "observaciones": row["Observaciones"],
    }

    try:
        response = supabase.table("registrosproduccion").insert(registro).execute()
        if response.data:
            id_registro = response.data[0]["id_registro"]
            registros_insertados.append((i, id_registro))
    except Exception as e:
        print(f"⚠️ Error en fila {i}: {e}")

print(f"✅ Se insertaron {len(registros_insertados)} registros en RegistrosProduccion\n")

# ============================
# 6️⃣ INSERTAR DETALLE DE PAROS
# ============================

print("⚙️ Insertando detalles de paros...")

for i, id_registro in registros_insertados:
    row = df.iloc[i]

    for n in range(1, 19):  # Hasta 18 códigos de paro
        codigo_col = f"Codigo de paro {n}"
        horas_col = f"Codigo {n} en horas"
        sub_col = f"Sub Codigo de paro {n}" if f"Sub Codigo de paro {n}" in df.columns else None

        if row.get(codigo_col):
            detalle = {
                "registro_id": id_registro,
                "codigo_paro": str(row[codigo_col]),
                "subcodigo": row.get(sub_col),
                "tipo_paro": row.get("Tipo de paro"),
                "horas_paro": row.get(horas_col),
                "area_involucrada": row.get("Área involucrada en subcodigo 5"),
                "personal_involucrado": row.get("Personal involucrado"),
                "observaciones_paro": row.get("Observaciones de paro"),
            }

            try:
                supabase.table("detalleparosproduccion").insert(detalle).execute()
            except Exception as e:
                print(f"⚠️ Error insertando detalle fila {i}: {e}")

print("✅ Detalle de paros insertado correctamente.")
print("🎯 ETL finalizado con éxito.")
