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

EXCEL_PATH = "data/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm"
engine = create_engine(DATABASE_URL)


def limpiar_tablas(engine):
    """Elimina datos de las tablas si existen."""
    with engine.begin() as conn:
        print("🧹 Verificando tablas existentes para limpiar registros...")
        tablas = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)).fetchall()

        if not tablas:
            print("⚠️ No existen tablas. Se crearán nuevas.")
            return False

        for (tabla,) in tablas:
            conn.execute(text(f"TRUNCATE TABLE {tabla} RESTART IDENTITY CASCADE;"))
        print("✅ Tablas vaciadas correctamente.\n")

        return True

limpiar_tablas(engine)

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
registros_insertados = []  # Guardará (índice, id_registro)

with engine.begin() as conn:
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

        result = conn.execute(text("""
            INSERT INTO registrosproduccion (
                fecha, mes, año, maquina_id, operario_id, referencia_id,
                pacas_producidas, horas_trabajadas, horas_no_trabajadas,
                turno, tiempo_total_paros, observaciones
            ) VALUES (
                :fecha, :mes, :año, :maquina_id, :operario_id, :referencia_id,
                :pacas_producidas, :horas_trabajadas, :horas_no_trabajadas,
                :turno, :tiempo_total_paros, :observaciones
            )
            RETURNING id_registro
        """), registro)

        id_registro = result.scalar()
        registros_insertados.append((i, id_registro))

print("✅ Registros insertados correctamente en registrosproduccion.\n")

# ============================
# ⚙️ INSERTAR DETALLE DE PAROS
# ============================

print("⚙️ Insertando detalles de paros...")

with engine.begin() as conn:
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
                    conn.execute(text("""
                        INSERT INTO detalleparosproduccion (
                            registro_id, codigo_paro, subcodigo, tipo_paro,
                            horas_paro, area_involucrada, personal_involucrado, observaciones_paro
                        ) VALUES (
                            :registro_id, :codigo_paro, :subcodigo, :tipo_paro,
                            :horas_paro, :area_involucrada, :personal_involucrado, :observaciones_paro
                        )
                    """), detalle)
                except Exception as e:
                    print(f"⚠️ Error insertando detalle fila {i}: {e}")

print("✅ Detalle de paros insertado correctamente.")
print("🎯 ETL finalizado con éxito.")
