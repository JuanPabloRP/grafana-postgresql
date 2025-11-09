# Proyecto ETL Grafana PostgreSQL

Este proyecto está diseñado para extraer datos de un archivo de Excel, transformarlos y cargarlos en una base de datos PostgreSQL. Luego, los datos se visualizan usando Grafana. El proyecto incluye un proceso ETL (Extraer, Transformar, Cargar) que se activa automáticamente cuando se modifica el archivo de Excel.

## Empezando

Estas instrucciones le permitirán obtener una copia del proyecto en funcionamiento en su máquina local para fines de desarrollo y prueba.

### Prerrequisitos

* [Docker](https://www.docker.com/get-started)
* [Python 3.10+](https://www.python.org/downloads/)

### Instalación

1. **Clona el repositorio:**

   ```bash
   git clone https://github.com/jpsalado/grafana_postgresql.git
   cd grafana_postgresql
   ```

2. **Instala las dependencias de Python:**

   Crea un entorno virtual e instala los paquetes requeridos de `requirements.txt`.

   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows usa `venv\Scripts\activate`
   pip install -r requirements.txt
   ```

3. **Configura el entorno:**

   El proyecto utiliza un archivo `docker-compose.yml` para configurar los servicios necesarios (PostgreSQL y Grafana).

   ```bash
   docker compose up -d
   ```

   Este comando iniciará los siguientes servicios:
    - **PostgreSQL:** una instancia de base de datos PostgreSQL.
    - **Grafana:** una instancia de Grafana para la visualización de datos.

## Uso

Para ejecutar el proceso ETL, ejecuta el siguiente comando:

```bash
python etl/etl_watcher.py
```

Este script observará los cambios en el archivo `data/SEGUIMIENTO TEMPERAS Y VINILOS Actividad.xlsm`. Cuando se modifique el archivo, el proceso ETL se activará automáticamente.

El proceso ETL realiza los siguientes pasos:
1. Lee los datos del archivo de Excel.
2. Limpia y transforma los datos.
3. Carga los datos en la base de datos de PostgreSQL.

## Esquema de la base de datos

El esquema de la base de datos se define en el archivo `initdb/01_schema.sql`. Consiste en las siguientes tablas:

- `maquinas`: Almacena los nombres de las máquinas.
- `operarios`: Almacena los nombres de los operarios.
- `referencias`: Almacena los nombres de las referencias.
- `registrosproduccion`: Almacena los registros de producción.

## Proceso ETL

El proceso ETL se implementa en los archivos `etl/etl_config.py` y `etl/etl_watcher.py`.

- `etl_config.py`: este script contiene la lógica ETL principal. Lee los datos del archivo de Excel, los limpia y los inserta en la base de datos de PostgreSQL.
- `etl_watcher.py`: este script supervisa los cambios en el archivo de Excel y activa el proceso ETL cuando se modifica el archivo.

El proceso ETL está diseñado para ser idempotente, lo que significa que se puede ejecutar varias veces sin crear entradas duplicadas en la base de datos.
