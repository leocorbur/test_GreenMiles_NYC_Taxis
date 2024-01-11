# Comandos para Shell de GCP

Este repositorio contiene scripts de shell para facilitar el proceso de transferencia de datos y scripts de Pyspark a Google Cloud Platform (GCP). A continuación, se detallan los comandos necesarios para ejecutar estos scripts.

### 1. Transferencia de datos brutos a Cloud Storage
```bash
curl -O https://raw.githubusercontent.com/leocorbur/testGreenMiles_NYC_Taxis/main/GCP/1_rawData_to_cloudStorage.sh
chmod +x a_rawData_to_cloudStorage.sh
./a_rawData_to_cloudStorage.sh
```

### 2. Transferencia de scripts Pyspark a Cloud Storage
```bash
curl -O https://raw.githubusercontent.com/leocorbur/testGreenMiles_NYC_Taxis/main/GCP/2_pyScripts_to_cloudStorage.sh
chmod +x b_pyScripts_to_cloudStorage.sh
./b_pyScripts_to_cloudStorage.sh
```

### 3. Transferencia de script DAG a la carpeta dags
```bash
curl -O https://raw.githubusercontent.com/leocorbur/test_GreenMiles_NYC_Taxis/main/GCP/dag_greenMiles.py
gsutil cp dag_greenMiles.py gs://us-central1-greenmiles-c4cc86ec-bucket/dags
```
