from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

# Configura tu SparkSession
spark = SparkSession.builder.appName("process_and_load_to_bq").getOrCreate()

# Evita que se genere _success
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

years = ["2020"]
months = ["01", "02"]

for year in years:
    for month in months:
        # Define la ruta del archivo Parquet en Google Cloud Storage
        gcs_path_input = f"gs://raw-files/parquet/fhvhv_tripdata_{year}-{month}.parquet"
        
        try:
            # Lee el archivo Parquet en un DataFrame de Spark
            df = spark.read.parquet(gcs_path_input)

            # Elimina duplicados basados en todas las columnas
            df = df.dropDuplicates()

            # Selecciona las columnas específicas
            selected_columns = [
                "hvfhs_license_num",
                "request_datetime",
                "pickup_datetime",
                "dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "trip_miles",
                "trip_time",
                "base_passenger_fare"
            ]

            df = df.select(*selected_columns)

            # Elimina valores nulos
            df = df.na.drop()

            # Configura las opciones para BigQuery
            bigquery_project = "proyecto-de-prueba-23"
            bigquery_dataset = "greenMiles"
            bigquery_table = "tlc"

            # Escribe el DataFrame en BigQuery
            df.write.format("bigquery") \
                .option("temporaryGcsBucket", "tmpr_files") \
                .option("table", f"{bigquery_project}:{bigquery_dataset}.{bigquery_table}") \
                .mode("append") \
                .save()

        except Exception as e:
            print(f"Error procesando el archivo {gcs_path_input}: {str(e)}")

# Detén la sesión de Spark
spark.stop()
