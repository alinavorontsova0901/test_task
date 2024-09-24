import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def calculate_daily_aggregate(date):
    spark = SparkSession.builder \
        .appName('MyApp') \
        .getOrCreate()

    path = f"/home/al.vorontsova/input/{date}.csv"
    df = spark.read.csv(path, inferSchema = True, sep=",")

    # Выполнение необходимых преобразований
    df = df.toDF('email', 'action', 'dt')

    pivot_df = df.groupBy('email').pivot('action').agg(
    count('action')
    )

    agg_df = pivot_df.withColumnRenamed("CREATE", "create_count")\
            .withColumnRenamed("READ", 'read_count')\
            .withColumnRenamed("UPDATE", 'update_count')\
            .withColumnRenamed("DELETE", 'delete_count')

    # Сохранение промежуточных результатов
    output_path = f"/home/al.vorontsova/daily_res/{date}"
    agg_df.write.csv(output_path, header=True, mode="overwrite")

    spark.stop()

if __name__ == '__main__':
    date = sys.argv[1] 
    calculate_daily_aggregate(date)