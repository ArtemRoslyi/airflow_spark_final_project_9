from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, ShortType, DateType, BooleanType
import pyspark.sql.functions as f
import logging

logger = logging.getLogger(__name__)

# 9. Настройте соединение с PostgreSQL из кода, но из PySpark. 
# (обязательно сделать это нужно в Airflow)

def conection_postgre(spark=None, df=None, table_name='test', db_name='test'):
    try:
        url = f'jdbc:postgresql://postgres_user:5432/{db_name}'
        properties = {
            'user': 'user',
            'password': 'password',
            'driver': 'org.postgresql.Driver',
        }

        if df is None:
            df = spark.read.jdbc(
            url=url,
            table=table_name,
            properties=properties
            )
            return df
    
        df.write.jdbc(
            url=url,
            table=table_name,
            mode='overwrite',
            properties=properties
        )
        
    except Exception as e:
        logger.exception(f'Ошибка соединения с postgre: {e}')
        raise

@task
def task_spark(file_path_1, file_path_2, file_path_3):

    try:
        # 1. Загрузите файл данных в DataFrame PySpark. 
        # Обязательно выведите количество строк.
        spark = (
            SparkSession
            .builder
            .appName('test_9')
            .master('local[*]')
            .config('spark.jars', '/opt/spark/external_jars/postgresql-42.7.3.jar')
            .getOrCreate()
        )

        airlines_raw_df = (
            spark
            .read
            .option('header', True)
            .csv(file_path_1)
        )

        airports_raw_df = (
            spark
            .read
            .option('header', True)            
            .csv(file_path_2)
        )

        flights_pak_raw_df = (
            spark
            .read
            .option('header', True)            
            .csv(file_path_3)
        )

        logger.info(f'Количество строк в airlines_raw_df: {airlines_raw_df.count()}')   
        logger.info(f'Количество строк в airports_row_df: {airports_raw_df.count()}')   
        logger.info(f'Количество строк в flights_pak_row_df: {flights_pak_raw_df.count()}')

        # 2. Убедитесь, что данные корректно прочитаны 
        # (правильный формат, отсутствие пустых строк).
        logger.info('Первые 2 строки в airlines_raw_df:\n' + airlines_raw_df._jdf.showString(2, 0, False))
        logger.info('Схема данных в airlines_raw_df:\n' + airlines_raw_df._jdf.schema().treeString()) 
        
        logger.info('Первые 2 строки в airports_row_df:\n' + airports_raw_df._jdf.showString(2, 0, False))
        logger.info('Схема данных в airports_row_df:\n' + airports_raw_df._jdf.schema().treeString())

        logger.info('Первые 2 строки в flights_pak_row_df:\n' + flights_pak_raw_df._jdf.showString(2, 0, False))
        logger.info('Схема данных в flights_pak_row_df:\n' + flights_pak_raw_df._jdf.schema().treeString())

        # 3. Преобразуйте текстовые и числовые поля в 
        # соответствующие типы данных (например, дата, число).
        airlines_df = airlines_raw_df.withColumnRenamed('IATA CODE', 'IATA_CODE')

        airports_df = (
            airports_raw_df
            .withColumnRenamed('IATA CODE', 'IATA_CODE')
            .withColumn('Latitude', f.col('Latitude').cast('double'))
            .withColumn('Longitude', f.col('Longitude').cast('double'))
        )

        flights_pak_df = (
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

        # 4. Найдите топ-5 авиалиний с наибольшей средней задержкой.
        max_avg_delay_airlines_df = (
            flights_pak_df
            .filter(f.col('ARRIVAL_DELAY') > 0)
            .groupBy('AIRLINE')
            .agg(
                f.round(f.avg(f.col('ARRIVAL_DELAY')), 2).alias('AVG_ARRIVAL_DELAY_MINUTES')
                ) 
            .alias('1_df').join(
                airlines_df.alias('2_df'),
                on=f.col('1_df.AIRLINE') == f.col('2_df.IATA_CODE'),
                how='inner'
            )
            .select('2_df.AIRLINE', '1_df.AVG_ARRIVAL_DELAY_MINUTES')
            .orderBy(f.desc('1_df.AVG_ARRIVAL_DELAY_MINUTES'))
            .limit(5)
        )

        conection_postgre(
            df=max_avg_delay_airlines_df, 
            table_name='max_avg_delay_airlines_df'
        )

        # 5. Вычислите процент отмененных рейсов для каждого аэропорта.
        percent_cancelld_flights_airport_df = (
            flights_pak_df
            .groupBy('ORIGIN_AIRPORT')
            .agg(
                f.count('*').alias('COUNT_FLIGHTS'), 
                f.count(f.when(f.col('CANCELLED') == True, 1)).alias('COUNT_CANCELLED_FLIGHTS')
            )
            .withColumn(
                'PERCENT_CANCELLED_FLIGHTS', 
                f.when(
                    f.col('COUNT_FLIGHTS') > 0, 
                    f.round((f.col('COUNT_CANCELLED_FLIGHTS') / f.col('COUNT_FLIGHTS')) * 100, 2)
                ).otherwise(0.0)
            )
            .alias('1_df').join(
                airports_df.alias('2_df'), 
                on=f.col('1_df.ORIGIN_AIRPORT') == f.col('2_df.IATA_CODE'),
                how='inner'
            )
            .orderBy(f.desc('1_df.PERCENT_CANCELLED_FLIGHTS'))
            .select('2_df.Airport', '2_df.City', '1_df.PERCENT_CANCELLED_FLIGHTS')
        )

        conection_postgre(
            df=percent_cancelld_flights_airport_df, 
            table_name='percent_cancelld_flights_airport_df'
        )

        # 6. Определите, какое время суток (утро, день, вечер, ночь) 
        # чаще всего связано с задержками рейсов.
        delay_times_of_day_df =(
            flights_pak_df
            .filter(f.col('ARRIVAL_DELAY') > 0)
            .withColumn('TIMES_OF_DAY', 
                f.when(f.col('ARRIVAL_HOUR').between(6, 11), 'Утро: 6-12')
                 .when(f.col('ARRIVAL_HOUR').between(12, 17), 'День: 12-18')
                 .when(f.col('ARRIVAL_HOUR').between(18, 21), 'Вечер: 18-22')
                 .otherwise('Ночь: 22-6')
            )
            .groupBy('TIMES_OF_DAY').agg(f.count('*').alias('delay_flights_count'))
            .orderBy(f.desc('delay_flights_count'))
            .limit(1)
        )

        conection_postgre(
            df=delay_times_of_day_df, 
            table_name='delay_times_of_day_df'
        )
            
        # 7. Добавьте в данные о полетах новые столбцы, рассчитанные 
        # на основе существующих данных:
            # IS_LONG_HAUL: Флаг, указывающий, является ли рейс 
            # дальнемагистральным (если расстояние больше 1000 миль).
            # DAY_PART: Определите, в какое время суток 
            # происходит вылет (утро, день, вечер, ночь).

        flights_pak_add_column_df = (
            flights_pak_df
            .withColumn('IS_LONG_HAUL', f.when(f.col('DISTANCE') > 1000, True).otherwise(False))
            .withColumn('TIMES_OF_DAY', 
                f.when(f.col('ARRIVAL_HOUR').between(6, 11), 'Утро: 6-12')
                 .when(f.col('ARRIVAL_HOUR').between(12, 17), 'День: 12-18')
                 .when(f.col('ARRIVAL_HOUR').between(18, 21), 'Вечер: 18-22')
                 .otherwise('Ночь: 22-6')
            )
            .orderBy('DATE')
            .limit(10000)
        )

        # 10. Загрузите только 10.000 строк из DataFrame в таблицу в PostgreSQL. 
        # (обязательно сделать это нужно в Airflow)
        conection_postgre(
            df=flights_pak_add_column_df, 
            table_name='flights_pak_add_column_df'
        )

        # 11. Выполните SQL скрипт в Python-PySpark скрипте, 
        # который выведет компанию - общее время полетов ее самолетов.
        pg_flights_pak_add_column_df = conection_postgre(
            spark=spark, 
            table_name='flights_pak_add_column_df'
        )
        pg_flights_pak_add_column_df.createOrReplaceTempView('pg_flights_pak_add_column_view')
        airlines_df.createOrReplaceTempView('airlines_view')
        result_sql_df = spark.sql(
            '''
            select 
                view_2.AIRLINE, 
                sum(
                    case 
                        when view_1.ARRIVAL_HOUR >= view_1.DEPARTURE_HOUR
                        then view_1.ARRIVAL_HOUR - view_1.DEPARTURE_HOUR
                        else view_1.ARRIVAL_HOUR - view_1.DEPARTURE_HOUR + 24
                    end
                ) as TOTAL_FLIGHT_TIME_HOURS
            from pg_flights_pak_add_column_view as view_1 
            join airlines_view as view_2 
            on view_1.AIRLINE = view_2.IATA_CODE

            group by view_2.AIRLINE
            order by TOTAL_FLIGHT_TIME_HOURS desc
            '''
        )

        logger.info('Первые 5 строк SQL-запроса:\n' + result_sql_df._jdf.showString(5, 0, False))

    except Exception as e:
        logger.exception(f'Ошибка pyspark задачи: {e}')
        raise
    finally:
        if spark:
            spark.stop()

@task
def task_start():
    logger.info('Start')

@task
def task_end():
    logger.info('end')

with DAG(
    dag_id='main',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
    },
    description='main',
    schedule_interval=None,
) as dag:
    
    start_task = task_start()
    end_task = task_end()

    spark_task = task_spark(
        file_path_1='/airflow_spark_data/airlines.csv',                
        file_path_2='/airflow_spark_data/airports.csv', 
        file_path_3='/airflow_spark_data/flights_pak.csv'
    )

    (
        start_task 
        >> spark_task
        >> end_task
    )
