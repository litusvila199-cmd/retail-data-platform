import logging

from spark.session import spark
from pyspark.sql.functions import col


from src.config.schemas import SCHEMAS
from src.config.tables import TABLES
from src.utils import logger


logger = logging.getLogger(__name__)




def process_table(table_name):

    logger.info(f"Processing table: {table_name}")

    try:

        # Leer el CSV y aplicar el schema correspondiente
        df = (
            spark.read
            .option("header", True)
            .schema(SCHEMAS[table_name])
            .csv(f"data/landing/{table_name}.csv")
        )

        # Contar las filas recibidas
        total_rows = df.count()

        logger.info(f"Rows read: {total_rows}")

        # Si la tabla está completamente vacía,
        # detenemos el procesamiento.
        if total_rows == 0:
            logger.error(f"{table_name} is empty")
            raise ValueError(
                f"{table_name} contains no data"
            )

        # Contar las filas después de eliminar duplicados.
        unique_rows = df.dropDuplicates().count()

        # Si hay menos filas únicas que filas originales,
        # significa que existen duplicados.
        if total_rows != unique_rows:

            logger.warning(
                f"{table_name} contains "
                f"{total_rows - unique_rows} duplicate rows."
            )

            # Eliminamos los duplicados.
            df = df.dropDuplicates()

            logger.info(
                f"Duplicate rows removed from {table_name}."
            )

        # Diccionario donde guardaremos los nulos
        # encontrados en cada columna.
        null_counts = {}

        # Comprobamos todas las columnas de la tabla.
        for column in df.columns:

            # Contamos cuántos valores NULL tiene
            # la columna actual.
            null_count = (
                df.filter(col(column).isNull()).count()
            )

            # Si encontramos algún NULL, lo guardamos.
            if null_count > 0:
                null_counts[column] = null_count

        # Los NULL no detienen el pipeline.
        # Simplemente dejamos constancia en el log.
        if null_counts:

            logger.warning(
                f"{table_name} contains null values: "
                f"{null_counts}"
            )

        # Guardamos los datos procesados como Parquet.
        df.write.mode("overwrite").parquet(
            f"data/processed/{table_name}.parquet"
        )

        logger.info(
            f"{table_name} saved as Parquet"
        )

    except Exception:

        logger.exception(
            f"Error processing table: {table_name}"
        )

        # Los errores técnicos sí hacen fallar
        # el procesamiento.
        raise


if __name__ == "__main__":

    for table in TABLES:
        process_table(table)