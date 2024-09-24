import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta


def calculate_weekly_aggregate(input_date):
    spark = SparkSession.builder \
        .appName('MyApp2') \
        .getOrCreate()

    # Преобразование строки в объект datetime
    date = datetime.strptime(input_date, '%Y-%m-%d')
    
    # Создаем список для всех файлов за неделю
    date_list = [(date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 8)]
    file_paths = [f"/home/al.vorontsova/daily_res/{d}" for d in date_list]

    # Чтение и объединение всех файлов в один DataFrame
    df = spark.read.option("header", "true").csv(file_paths)
    df = df.groupBy('email').agg(
        sum('create_count').alias('create_count'), 
        sum('read_count').alias('read_count'), 
        sum('update_count').alias('update_count'), 
        sum('delete_count').alias('delete_count')
    )

    # Сохранение результата
    output_path = f"/home/al.vorontsova/output/{input_date}"
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    input_date = sys.argv[1] 
    calculate_weekly_aggregate(input_date)