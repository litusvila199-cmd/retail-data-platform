# Retail Data Platform

Pipeline de Data Engineering de extremo a extremo construido con Python, PostgreSQL, Pandas, PySpark, Apache Airflow y AWS S3.

El proyecto extrae datos de una base de datos PostgreSQL, los procesa y valida con Apache Spark, genera datasets analíticos curados y almacena las diferentes capas de datos tanto localmente como en Amazon S3.

## Arquitectura

```text
                    PostgreSQL
                        │
                        ▼
                 Apache Airflow
                        │
                        ▼
                     Extract
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
      data/landing/        AWS S3 / landing
          CSV files
             │
             ▼
        Apache Spark
         Processing
             │
             ├───────────────────┐
             ▼                   ▼
      data/processed/     AWS S3 / processed
          Parquet
             │
             ▼
        Data Generation
       ┌───────┴────────┐
       ▼                ▼
     Sales       Customer Analytics
       │                │
       └───────┬────────┘
               ▼
          data/curated/
             Parquet
                │
                ▼
          AWS S3 / curated
```

## Pipeline

El pipeline completo está orquestado con Apache Airflow:

```text
extract
   ↓
process
   ↓
sales
   ↓
customer_analytics
```

### 1. Extract

Los datos se extraen de PostgreSQL utilizando Python y Pandas.

Las tablas extraídas se guardan como archivos CSV en:

```text
data/landing/
```

Los archivos CSV también se suben a Amazon S3:

```text
s3://<bucket>/retail/landing/
```

### 2. Process

Apache Spark lee los archivos CSV de la capa landing y aplica los esquemas correspondientes.

La etapa de procesamiento realiza varias comprobaciones de calidad de datos:

- Comprueba si los datasets están vacíos.
- Detecta filas duplicadas.
- Elimina filas duplicadas.
- Detecta valores NULL.
- Escribe los datos procesados en formato Parquet.

Los datasets procesados se almacenan localmente en:

```text
data/processed/
```

y se suben a:

```text
s3://<bucket>/retail/processed/
```

### 3. Sales Dataset

El pipeline combina los siguientes datasets:

```text
orders
order_items
products
```

para crear un dataset curado de ventas.

El dataset resultante contiene información como:

- pedido
- cliente
- producto
- categoría
- cantidad
- precio unitario
- importe total

El dataset se almacena en:

```text
data/curated/sales.parquet/
```

y se sube a:

```text
s3://<bucket>/retail/curated/
```

### 4. Customer Analytics

El pipeline genera datos analíticos a nivel de cliente utilizando:

```text
customers
orders
sales
```

El dataset resultante contiene métricas como:

- número de pedidos
- fecha del último pedido
- importe total gastado

El dataset se almacena en:

```text
data/curated/customer_analytics.parquet/
```

y se sube a Amazon S3.

## Tecnologías

- Python
- PostgreSQL
- Pandas
- PySpark
- Apache Spark
- Apache Airflow
- AWS S3
- Boto3
- Git
- GitHub
- python-dotenv

## Estructura del proyecto

```text
retail-data-platform/
│
├── airflow/
│   └── dags/
│       └── retail_pipeline.py
│
├── data/
│   ├── landing/
│   ├── processed/
│   └── curated/
│
├── spark/
│   └── session.py
│
├── src/
│   ├── aws/
│   │   ├── __init__.py
│   │   └── s3.py
│   │
│   ├── config/
│   │   ├── database.py
│   │   ├── schemas.py
│   │   └── tables.py
│   │
│   ├── generators/
│   │   ├── customer_analytics.py
│   │   └── sales.py
│   │
│   ├── ingestion/
│   │   └── extract.py
│   │
│   ├── transforms/
│   │   └── process.py
│   │
│   └── utils/
│       └── logger.py
│
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Capas de datos

El proyecto utiliza tres capas lógicas de datos.

### Landing

Contiene los datos extraídos de PostgreSQL en formato CSV.

```text
data/landing/
```

y:

```text
S3: retail/landing/
```

### Processed

Contiene los datos procesados y validados con Apache Spark y almacenados en formato Parquet.

```text
data/processed/
```

y:

```text
S3: retail/processed/
```

### Curated

Contiene los datasets analíticos generados a partir de los datos procesados.

```text
data/curated/
```

y:

```text
S3: retail/curated/
```

## Estructura de AWS S3

El bucket de S3 está organizado siguiendo las mismas capas lógicas:

```text
retail/
│
├── landing/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── products.csv
│   ├── payments.csv
│   └── shipments.csv
│
├── processed/
│   ├── customers.parquet/
│   ├── orders.parquet/
│   ├── order_items.parquet/
│   └── ...
│
└── curated/
    ├── sales.parquet/
    └── customer_analytics.parquet/
```

## Configuración

La configuración sensible se almacena mediante variables de entorno y no se incluye en el repositorio.

Ejemplo:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail
DB_USER=your_user
DB_PASSWORD=your_password

S3_BUCKET_NAME=your-bucket
AWS_REGION=eu-west-1
```

Las credenciales de AWS se gestionan mediante AWS CLI en lugar de almacenarse directamente en el proyecto.

## Instalación

Crea y activa un entorno virtual:

```bash
python -m venv .venv
source .venv/bin/activate
```

Instala las dependencias del proyecto:

```bash
pip install -r requirements.txt
```

Configura las variables de entorno necesarias utilizando `.env`.

Asegúrate de que PostgreSQL y las credenciales de AWS estén disponibles.

## Ejecución del Pipeline

Inicia Apache Airflow:

```bash
airflow standalone
```

Abre la interfaz web de Airflow y ejecuta:

```text
retail_pipeline
```

El DAG ejecuta las siguientes tareas de forma secuencial:

```text
extract
   ↓
process
   ↓
sales
   ↓
customer_analytics
```

Cada tarea está configurada con reintentos para gestionar errores temporales.

## Logging y gestión de errores

El proyecto incluye logging a lo largo de todo el pipeline.

El pipeline gestiona situaciones como:

- Errores durante la extracción desde PostgreSQL.
- Datasets vacíos.
- Registros duplicados.
- Valores NULL.
- Errores durante el procesamiento con Spark.
- Errores durante la subida a S3.

Airflow vuelve a ejecutar las tareas que fallan de acuerdo con la política de reintentos configurada.

## Integración con AWS

AWS S3 está integrado mediante Boto3.

El proyecto utiliza dos funciones de subida:

```python
upload_file_to_s3()
```

para archivos individuales como los archivos CSV, y:

```python
upload_directory_to_s3()
```

para los directorios generados por Spark al escribir datasets en formato Parquet.

Esto permite que el mismo pipeline mantenga las capas de datos localmente y, al mismo tiempo, almacene los datos en una estructura de data lake en AWS S3.

## Objetivo

Este proyecto se ha creado como un proyecto práctico de Data Engineering para portfolio, demostrando la construcción de un pipeline de datos de extremo a extremo.

El proyecto cubre:

- Extracción de datos desde PostgreSQL.
- Procesamiento ETL.
- Comprobaciones de calidad de datos.
- Transformaciones con PySpark.
- Formato Parquet.
- Capas de datos.
- Generación de datasets analíticos.
- Orquestación con Apache Airflow.
- Integración con AWS S3.
- Conceptos de data lake en la nube.
- Logging y gestión de errores.
- Git y GitHub.