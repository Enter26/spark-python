from pyspark.sql.functions import stddev as _stddev, col
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
import datetime

if __name__ == "__main__":

        #ustawienie środowiska
        sc = SparkContext("local[1]", "average")
        sc.setLogLevel("Error")
        spark = SparkSession(sc)
        #pobranie czasu rozpoczęcia ekstrakcji danych
        start_time = datetime.datetime.now()
        #ładowanie danych z pliku CSV do zmiennej
        df = spark.read.option("header",True).csv("data1.csv")
        #sprawdzenie poprawności załadowania pliku poprzez wyświetlenie nagłówka i pierszych 5 wierszy
        df.show(5)
        #modyfikacja struktury danych oraz wyświetlenie zmodyfikowanego schematu
        df2 = df.withColumn("max_temp", df["max_temp"].cast(FloatType()))
        #pobranie czasu zakończenia ładowania daych
        end_load_time = datetime.datetime.now()
        #wykonanie zadania
        df2.filter((df2['latitude'] >= 41) & (df2['latitude'] <= 45 ) & (df2['longitude'] >= (-110) ) & (df2['longitude'] <= (-104) ) & (df2['year'] >= 1970)).groupBy("year").mean('max_temp').sort('year').show(50)
        #odchylenie standardowe
        df2.filter((df2['latitude'] >= 41) & (df2['latitude'] <= 45 ) & (df2['longitude'] >= (-110) ) & (df2['longitude'] <= (-104) ) & (df2['year'] >= 1970)).groupBy("year").mean('max_temp').sort('year').select(_stddev(col('avg(max_temp)')).alias('std')).show(1)

        #pobranie czasu zakończenia wykonania programu/zadania
        end_time = datetime.datetime.now()
        sc.stop()
        #utworzenie zmiennych wynikowych
        time_load_data = end_load_time - start_time
        time_of_execution = end_time - end_load_time
        total_time = end_time - start_time
        #wyświetlenie wyników
        print("Time load data:")
        print(time_load_data)
        print("Time of execution:")
        print(time_of_execution)
        print("Total time:")
        print(total_time)
