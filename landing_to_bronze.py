import os

import requests
from pyspark.sql import SparkSession
# import urllib.request

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("Landing to Bronze") \
    .getOrCreate()

tablets = ["athlete_bio", "athlete_event_results"]
# Створення папки, якщо не існує
os.makedirs("bronze", exist_ok=True)


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"\nDownloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"    File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

for table_name in tablets:
    download_data(table_name)

    # Зчитування CSV за допомогою Spark
    print(f"        Reading {table_name} into Spark DataFrame...")
    df = spark.read.option("header", "true").csv(table_name + ".csv")

    # Запис у форматі Parquet у папку bronze/{table_name}
    output_path = f"bronze/{table_name}"
    print(f"        Writing {table_name} to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)

print("All tables successfully processed into Bronze layer.")
spark.stop()
