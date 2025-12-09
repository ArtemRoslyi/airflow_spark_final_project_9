from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, ShortType, DateType, BooleanType
import spark.sql.functions as f
import logging

logger = logging.getLogger(__name__)

def spark_job(file_path):

    try:
        spark = (
            SparkSession
            .builder
            .appName('test_9')
            .master('local[*]')
            .getOrCreate()
        )

        airlines_raw_df = (
            spark
            .read
            .options('header', True)
            .csv('/airflow_spark_data/airlines.csv')
        )

        airports_raw_df = (
            spark
            .read
            .options('header', True)            
            .csv('/airflow_spark_data/airports.csv')
        )

        flights_pak_raw_df = (
            spark
            .read
            .options('header', True)            
            .csv('/airflow_spark_data/flights_pak.csv')
        )

        logger.info(f'Количество строк в airlines_raw_df: {airlines_raw_df.count()}')
        logger.info('Первые 3 строки в airlines_raw_df:')
        airlines_raw_df.show(3, truncate=False)
        logger.info('Схема данных в airlines_raw_df:')
        airlines_raw_df.printSchema()       
        
        logger.info(f'Количество строк в airports_row_df: {airports_raw_df.count()}')
        logger.info('Первые 3 строки в airports_row_df:')
        airports_raw_df.show(3, truncate=False)
        logger.info('Схема данных в airports_row_df:')
        airports_raw_df.printSchema()

        logger.info(f'Количество строк в flights_pak_row_df: {flights_pak_raw_df.count()}')
        logger.info('Первые 3 строки в flights_pak_row_df:')
        flights_pak_raw_df.show(3, truncate=False)
        logger.info('Схема данных в flights_pak_row_df:')
        flights_pak_raw_df.printSchema()

        airports_stage_df = (
            airports_raw_df
            .withColumn('Latitude', f.col('Latitude').cast('double'))
            .withColumn('Longitude', f.col('Longitude').cast('double'))
        )

        flights_pak_stage_df = (
            flights_pak_raw_df
            .withColumn('DATE', f.col('DATE').cast(DateType()))
            .withColumn('FLIGHT_NUMBER', f.col('FLIGHT_NUMBER').cast(IntegerType()))
            .withColumn('DEPARTURE_DELAY', f.col('DEPARTURE_DELAY').cast(IntegerType()))
            .withColumn('DISTANCE', f.col('DISTANCE').cast(DoubleType()))    
            .withColumn('ARRIVAL_DELAY', f.col('ARRIVAL_DELAY').cast(IntegerType()))
            .withColumn('DIVERTED', f.col('DIVERTED').cast(BooleanType()))     
            .withColumn('CANCELLED', f.col('CANCELLED').cast(BooleanType()))             
            .withColumn('AIR_SYSTEM_DELAY', f.col('AIR_SYSTEM_DELAY').cast(IntegerType()))
            .withColumn('SECURITY_DELAY', f.col('SECURITY_DELAY').cast(IntegerType()))
            .withColumn('AIRLINE_DELAY', f.col('AIRLINE_DELAY').cast(IntegerType()))                   
            .withColumn('LATE_AIRCRAFT_DELAY', f.col('LATE_AIRCRAFT_DELAY').cast(IntegerType()))
            .withColumn('WEATHER_DELAY', f.col('WEATHER_DELAY').cast(IntegerType()))
            .withColumn('DEPARTURE_HOUR', f.col('DEPARTURE_HOUR').cast(ShortType()))
            .withColumn('ARRIVAL_HOUR', f.col('ARRIVAL_HOUR').cast(ShortType()))                                
        )


    except Exception as e:
        logger.error(f'Ошибка pyspark задачи: {e}', exc_info=True)
    finally:
        if spark:
            spark.stop()