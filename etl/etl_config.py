import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
import os

# ============================
# 🔧 CONFIGURACIÓN BASE LOCAL
# ============================
DB_USER = "postgres"
DB_PASSWORD = "test"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "produccion_db"

# Cadena de conexión SQLAlchemy
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

EXCEL_PATH = "data/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm"

# ============================
# 🧾 LECTURA Y LIMPIEZA
# ============================

print("📂 Leyendo archivo Excel...")
df = pd.read_excel(EXCEL_PATH, sheet_name="Base De Datos", skiprows=8)

df.columns = df.columns.str.replace("\n", " ").str.strip()
df = df.dropna(subset=["Fecha", "Maquina"])
df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
df["Mes"] = df["Mes"].astype(str)
df["Año"] = df["Año"].astype(int)
df = df.replace({np.nan: None})

print(f"✅ Datos cargados ({len(df)} filas)\n")

# ============================
# 🧱 CARGA DE DIMENSIONES
# ============================

def obtener_o_insertar(tabla, campo_nombre, valor):
    if not valor:
        return None

    id_campo = f"id_{tabla[:-1].lower()}"
    with engine.begin() as conn:
        res = conn.execute(text(f"SELECT {id_campo} FROM {tabla} WHERE nombre = :v"), {"v": valor}).fetchone()
        if res:
            return res[0]
        else:
            conn.execute(text(f"INSERT INTO {tabla}(nombre) VALUES(:v)"), {"v": valor})
            res = conn.execute(text(f"SELECT {id_campo} FROM {tabla} WHERE nombre = :v"), {"v": valor}).fetchone()
            return res[0] if res else None

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

print(f"✅ Dimensiones cargadas ({len(maquina_map)} máquinas, {len(operario_map)} operarios, {len(referencia_map)} referencias)\n")

# ============================
# 📦 INSERTAR REGISTROS
# ============================

print("📦 Insertando registros de producción...")
with engine.begin() as conn:
    for _, row in df.iterrows():
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

        # Inserta el registro
        conn.execute(text("""
            INSERT INTO registrosproduccion (
                fecha, mes, año, maquina_id, operario_id, referencia_id,
                pacas_producidas, horas_trabajadas, horas_no_trabajadas,
                turno, tiempo_total_paros, observaciones
            ) VALUES (
                :fecha, :mes, :año, :maquina_id, :operario_id, :referencia_id,
                :pacas_producidas, :horas_trabajadas, :horas_no_trabajadas,
                :turno, :tiempo_total_paros, :observaciones
            )
        """), registro)

print("✅ Registros insertados correctamente en registrosproduccion.")

print("🎯 ETL finalizado con éxito.")
