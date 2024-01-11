from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_unixtime, to_timestamp, hour
from google.cloud import storage

# Crear una sesión de Spark
spark = SparkSession.builder.appName("airPollution").getOrCreate()

# Evita que se genere _success
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Ruta del archivo JSON
gcs_path_input = f"gs://raw-files/json/airPollution.json"


# Verifica si el archivo existe antes de intentar leerlo con Spark
client = storage.Client()
bucket_name = "raw-files"  
blob_name = f"json/airPollution.json"
blob = client.bucket(bucket_name).get_blob(blob_name)

if blob is not None and blob.exists():
    # Leer el archivo JSON en un DataFrame de PySpark
    df = spark.read.json(gcs_path_input)

    # Seleccionar la columna 'list' y aplicar la función explode para descomponer la lista en filas
    df = df.select(explode(df['list']).alias('list'))

    # Seleccionar y convertir el campo 'dt' de timestamp a datetime
    df = df.withColumn("datetime", to_timestamp(from_unixtime("list.dt")))

    # Extraer la fecha y la hora directamente usando la función hour
    df = df.withColumn("date", df["datetime"].cast("date"))
    df = df.withColumn("hour_of_day", hour(df["datetime"]))

    # Seleccionar las columnas deseadas del DataFrame resultante
    df = df.select("date", "hour_of_day", "list.main.aqi", "list.components.co", "list.components.no", 
                "list.components.no2", "list.components.pm2_5", "list.components.pm10")
    
    # Configura las opciones para BigQuery
    bigquery_project = "proyecto-de-prueba-23"
    bigquery_dataset = "greenMiles"
    bigquery_table = "airPollution"

    # Escribe el DataFrame en BigQuery
    df.write.format("bigquery") \
    .option("temporaryGcsBucket", "tmpr_files") \
    .option("table", f"{bigquery_project}:{bigquery_dataset}.{bigquery_table}") \
    .mode("overwrite") \
    .save()
    
else: 
    print(f"El archivo {gcs_path_input} no existe.")

# Detener la sesión de Spark
spark.stop()