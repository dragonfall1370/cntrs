from os import truncate
import findspark
from pyspark.sql.dataframe import DataFrame
from traitlets.traitlets import Integer
findspark.init()

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from common import *
import datetime
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pyodbc
import tabulate

import fd

appName = "HTTL Log Analysis"
master = "local[*]"

conf_local = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) \
        .set("spark.executor.memory", "26g")
        # .set("spark.worker.exrtaClassPath", "C:\Spark\sqljdbc_9.2\enu\jre8connect\mssql-jdbc-9.2.1.jre8.jar: C:\Spark\sqljdbc_9.2\enu\jre8connect\mssql-jdbc_auth-9.2.1.x64.dll")

sc_local = SparkContext.getOrCreate(conf=conf_local)
sc_local.setLogLevel("ERROR")


spark_local = SparkSession(sc_local)

def flatten_json(df: DataFrame) -> DataFrame :
    df = df.withColumn(
                "gender_name",
                when(get_json_object(col("personal_properties"), '$.customer_gender').isNotNull(),
                    get_json_object(col("personal_properties"), '$.customer_gender'))
                .when(get_json_object(col("personal_properties"), '$.gender').isNotNull(),
                    get_json_object(col("personal_properties"), '$.gender'))
                .otherwise(lit(None))
            ).withColumn(
                "race_name",
                get_json_object(col("personal_properties"), '$.race')
            ).withColumn(
                "primary_cluster",
                get_json_object(col("personal_properties"),
                                '$.primary_cluster_comb')
            ).withColumn(
                "primary_cluster_state",
                get_json_object(col("personal_properties"),
                                '$.primary_cluster_state')
            ).withColumn(
                "converge_flag",
                get_json_object(col("personal_properties"), '$.converge_flag')
            ).withColumn(
                "info_5",
                get_json_object(col("personal_properties"), '$.info_5')
            ).withColumn(
                "birth_year",
                when(get_json_object(col("personal_properties"), '$.customer_date_of_birth').isNotNull(),
                    substring(
                        when(
                            get_json_object(col("personal_properties"),
                                            '$.customer_date_of_birth') == " ", "0"
                        )
                        .otherwise(
                            get_json_object(col("personal_properties"),
                                            '$.customer_date_of_birth')
                        ),
                        1,
                        4
                ))
                .when(get_json_object(col("personal_properties"), '$.birth_year').isNotNull(),
                    substring(
                        get_json_object(
                            col("personal_properties"), '$.birth_year'),
                        1,
                        4
                ))
                .when(get_json_object(col("personal_properties"), '$.birthday').isNotNull(),
                    substring(
                        get_json_object(
                            col("personal_properties"), '$.birthday'),
                        -4,
                        4
                ))
                .otherwise(lit(None)).cast(IntegerType())
            ).withColumn(
                "converge_flag",
                get_json_object(col("personal_properties"), '$.converge_flag')
            ).withColumn(
                "user_tier",
                get_json_object(col("personal_properties"), '$.tier')
            ).withColumn(
                "pp_addr1",
                get_json_object(col("personal_properties"), '$.addr1')
            ).withColumn(
                "pp_city",
                get_json_object(col("personal_properties"), '$.city')
            ).withColumn(
                "pp_home",
                get_json_object(col("personal_properties"), '$.home')
            )
            # .withColumn(
            #     "personal_properties",
            #     substring(col("personal_properties"), 1, 0)
            # )
    return df


data2 = [(1,'{"email": null, "phone": "60174107825", "gender": "female", "address": null, "birthday": "1987-02-03", "joined_at": "2019-08-01T00:00:00.000Z"}'),
    (2,'{"email": null, "phone": "60123074202", "gender": null, "address": null, "birthday": null, "joined_at": null}'),
    (3,'{"email": "engkukhairi@gmail.com", "phone": "601126000575", "gender": "male", "address": null, "birthday": "1994-10-12", "joined_at": "2018-06-26T00:00:00.000Z"}'),
    (4,'{"email": "roslansster@gmail.com", "phone": "60193825797", "gender": "male", "address": null, "birthday": "2001-01-01", "joined_at": "2019-10-25T00:00:00.000Z"}'),
    (5,'{"email": "khaizeins@yahoo.com", "phone": "60133737320", "gender": "male", "address": null, "birthday": "1978-10-30", "joined_at": "2020-07-21T00:00:00.000Z"}')
  ]




schema = StructType([
                        StructField("Id", IntegerType(), True)
                        , StructField("personal_properties",StringType() , True)
                    ])

df = spark_local.createDataFrame(data=data2,schema=schema)

# df_new = flatten_json(df)

df_new = df.withColumn(
                "personal_properties_new",
                substring(col("personal_properties"), 1, 0)
            )


df_new.printSchema()

df_new.show()