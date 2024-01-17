from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from datetime import datetime

# Tiempo de Ejecución
start_time = datetime.now()

# Configura tu SparkSession
spark = SparkSession.builder.appName("process_and_load_to_bq").getOrCreate()

# Evita que se genere _success
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

years = ["2023"]
months = ["01", "02"]

# Registro
file_name = "fhvhv_tripdata"
file_format = "parquet"
initial_record_count = 0
null_values_count = []
duplicate_records_count = 0

for year in years:
    for month in months:
        # Define la ruta del archivo Parquet en Google Cloud Storage
        gcs_path_input = f"gs://raw-files/parquet/fhvhv_tripdata_{year}-{month}.parquet"
        
        try:
            # Lee el archivo Parquet en un DataFrame de Spark
            df = spark.read.parquet(gcs_path_input)

            # Registro
            initial_record_count += df.count()
            initial_column_count = len(df.columns)
            null_values_count += list(df.select([F.sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict().values())
            duplicate_records_count += df.count() - df.dropDuplicates().count()

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

            # Registro
            final_record_count = df.count()
            final_column_count = len(df.columns)

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
            
            end_time = datetime.now()
            execution_time = end_time - start_time
            execution_time_seconds = execution_time.total_seconds()
            execution_date = start_time.date()

            registro = [file_name, file_format, initial_record_count, final_record_count,
            initial_column_count, final_column_count, sum(null_values_count),
             duplicate_records_count, execution_time_seconds, execution_date ]
            
            # Crear DataFrame de Spark a partir de la lista registro
            registro_df = spark.createDataFrame([Row(*registro)], ["file_name", "file_format", "initial_record_count", 
                                                      "final_record_count", "initial_column_count", 
                                                      "final_column_count", "null_values_sum", 
                                                      "duplicate_records_count", "execution_time_seconds", 
                                                      "execution_date"])
            
            # Cambiar los tipos de datos en el DataFrame de registro
            registro_df = registro_df.withColumn("null_values_sum", col("null_values_sum").cast("integer"))

            # Tabla de Auditoria
            bigquery_project = "proyecto-de-prueba-23"
            bigquery_dataset = "greenMiles"
            bigquery_table = "Auditoria"

            registro_df.write.format("bigquery") \
            .option("temporaryGcsBucket", "tmpr_files") \
            .option("table", f"{bigquery_project}:{bigquery_dataset}.{bigquery_table}") \
            .mode("append") \
            .save()

        except Exception as e:
            print(f"Error procesando el archivo {gcs_path_input}: {str(e)}")

# Detén la sesión de Spark
spark.stop()
