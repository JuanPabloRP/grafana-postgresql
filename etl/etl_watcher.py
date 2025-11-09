import time
import subprocess
import os
from threading import Lock
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuración
EXCEL_PATH = os.path.abspath("data/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm")
ETL_SCRIPT = os.path.abspath("etl/etl_config.py")
COOLDOWN_SECONDS = 5  # evita relanzar si el archivo cambia varias veces seguidas

last_run_time = 0
etl_lock = Lock()


class ExcelChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == EXCEL_PATH:
            trigger_etl("modificado")

    def on_created(self, event):
        if event.src_path == EXCEL_PATH:
            trigger_etl("creado o reemplazado")


def trigger_etl(reason):
    global last_run_time

    now = time.time()
    if now - last_run_time < COOLDOWN_SECONDS:
        print(f"⚠️ Cambio detectado ({reason}) pero dentro del cooldown, se ignora.")
        return

    if etl_lock.locked():
        print(f"⚙️ ETL ya en ejecución, se omite este evento ({reason}).")
        return

    last_run_time = now
    with etl_lock:
        run_etl(reason)


def run_etl(reason):
    print(f"\n📊 Archivo Excel {reason}. Ejecutando ETL...\n")
    try:
        subprocess.run(["python", ETL_SCRIPT], check=True)
        print("✅ ETL ejecutado correctamente.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al ejecutar el ETL: {e}\n")


def watch_excel():
    folder_to_watch = os.path.dirname(EXCEL_PATH)
    event_handler = ExcelChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_watch, recursive=False)
    observer.start()
    print(f"👀 Vigilando cambios en: {EXCEL_PATH}\nPresiona Ctrl+C para detener.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\n🛑 Vigilancia detenida.")
    observer.join()


if __name__ == "__main__":
    watch_excel()
