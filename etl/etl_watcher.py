import time
import subprocess
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Ruta al archivo Excel que quieres vigilar
EXCEL_PATH = os.path.abspath("data/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm")

# Ruta del script ETL que debe ejecutarse cuando cambie el Excel
ETL_SCRIPT = os.path.abspath("etl/etl_config.py")


class ExcelChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == EXCEL_PATH:
            print("\n📊 Archivo Excel modificado. Ejecutando ETL...\n")
            run_etl()

    def on_created(self, event):
        if event.src_path == EXCEL_PATH:
            print("\n📊 Archivo Excel creado o reemplazado. Ejecutando ETL...\n")
            run_etl()


def run_etl():
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
