from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


import boto3
import psycopg2
import logging
import json








def get_connection(conn_name: str) -> dict:
    client = boto3.client('glue', region_name="ap-southeast-1")
    response = client.get_connection(Name=conn_name)

    connection_properties = response['Connection']['ConnectionProperties']
    URL = connection_properties['JDBC_CONNECTION_URL']
    url_list = URL.split("/")

    host = "{}".format(url_list[-2][:-5])
    port = url_list[-2][-4:]
    database = "{}".format(url_list[-1])
    user = "{}".format(connection_properties['USERNAME'])
    pwd = "{}".format(connection_properties['PASSWORD'])

    conn = {
        "url": URL,
        "user": user,
        "pwd": pwd,
        "dbname": database,
        "host": host,
        "port": port
    }

    # Reurn connection credentials to be used by spark.read()
    return conn



def get_dynamic_frame(glueContext: GlueContext, schema: str, table: str, bookmark_keys: list = ["updated_at"]) -> DataFrame:
    # Get AWS Glue Dynamic Frame
    df = glueContext.create_dynamic_frame.from_catalog(
        database=schema,
        table_name="platform_production_{}_{}".format(schema, table),
        transformation_ctx="datasource-{}-{}".format(schema, table),
        additional_options={
            "jobBookmarkKeys": bookmark_keys,
            "jobBookmarksKeysSortOrder": "asc",
        }
    )

    # Return AWS Dynamic Frame
    return df


def get_data_frame(conn: dict, sqlContext: SQLContext, query: str, driver: str="org.postgresql.Driver") -> DataFrame:
    df = sqlContext.read \
        .format("jdbc") \
        .option("url", conn["url"]) \
        .option("user", conn["user"]) \
        .option("password", conn["pwd"]) \
        .option("query", query) \
        .option("fetchsize", "5000") \
        .option("driver", driver) \
        .load()

    # Return Spark Data Frame
    return df




# Clients, connections


dynamodb = boto3.client('dynamodb')


#   .option("driver", "org.postgresql.Driver") \

def dynamodb_get_item(table: str, attr_name: str, pk_name: str = "table_name",) -> str:
    item = dynamodb.get_item(
        TableName=table,
        Key={pk_name: {"S": attr_name}}
    )
    return item


def dynamodb_put_item(table: str, partition_key: str, attr_name: str, attr_value: str, pk_name: str = "table_name",):
    dynamodb.put_item(
        TableName=table,
        Item={pk_name: {"S": partition_key}, attr_name: {"S": attr_value}}
    )

def dynamodb_get_module_bookmark(dynamodb_table_name: str, module_name: str, bookmark_table: str) -> dict:
    dynamodb_table = boto3.resource("dynamodb").Table(dynamodb_table_name)

    to_update = dynamodb_table.query(
        KeyConditionExpression="module_name = :module_name",
        FilterExpression="table_name = :table_name AND to_update = :to_update",
        ExpressionAttributeValues = {
            ":module_name": module_name,
            ":table_name": bookmark_table,
            ":to_update": True
        },
    )
    return to_update

def dynamodb_put_module_bookmark(dynamodb_table: str, schema_name: str, module_name: str, table_name: str, bookmark_value: str):
    dynamodb.put_item(
        TableName=dynamodb_table,
        Item={
            "module_name": {"S": module_name},
            "schema_name": {"S": schema_name},
            "table_name": {"S": table_name},
            "bookmark": {"S": bookmark_value}
        }
    )


def get_filtered_ids(df: DataFrame, id_column: str) -> str:

    ids = df.select(id_column).rdd.flatMap(lambda x: x).collect()
    filtered = set(filter(None, ids))

    if len(filtered) > 0:
        # Return (x, y, z) string. Ready to inject to WHERE clause
        return "({})".format(','.join(map(str, filtered)))
    else:
        return ""






def copy_raw_to_redshift(df: DataFrame, glueContext: GlueContext, schema: str, table: str, args: dict):

    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(df, glueContext, "nested"),
        catalog_connection="redshift-playground-local",
        connection_options={
            "dbtable": "public.etl_all_tables_staging".format(schema, table),
            "database": "oltp_transactions",
            "extracopyoptions": "ACCEPTANYDATE TRUNCATECOLUMNS ACCEPTINVCHARS MAXERROR 5"
        },
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="",
    )

def copy_to_redshift(df: DataFrame, glueContext: GlueContext, schema: str, table: str, args: dict, postactions: str ="", preactions: str =""):

    postactions_fn = "BEGIN TRANSACTION; select 1 as check; "+ postactions+" "+ " END TRANSACTION;"
    preactions_fn = "BEGIN TRANSACTION; select 1 as check; "+ preactions+" "+ " END TRANSACTION;"

    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(df, glueContext, "nested"),
        catalog_connection="redshift-playground-local",
        connection_options={
            "dbtable": "public.etl_all_tables_staging",
            "database": "oltp_transactions",
            "preactions": preactions_fn,
            "postactions": postactions_fn,
            "extracopyoptions": "ACCEPTANYDATE TRUNCATECOLUMNS ACCEPTINVCHARS MAXERROR 5"
        },
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="",
    )




def delete_from_redshift(df: DataFrame, glueContext: GlueContext, schema: str, table: str, args: dict):
    preactions = """CREATE SCHEMA IF NOT EXISTS {schema};TRUNCATE TABLE {schema}.{table}_etl_deletes;""".format(
        schema=schema, table=table)

    postactions = """BEGIN TRANSACTION;DELETE FROM {schema}.{table} USING {schema}.{table}_etl_deletes WHERE {schema}.{table}.id = {schema}.{table}_etl_deletes.id;END TRANSACTION;""".format(
        schema=schema, table=table)

    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(df, glueContext, "nested"),
        catalog_connection="redshift-playground-local",
        connection_options={
            "dbtable": "{}.{}_etl_deletes".format(schema, table),
            "database": "oltp_transactions",
            "preactions": preactions,
            "postactions": postactions,
            "extracopyoptions": "ACCEPTANYDATE TRUNCATECOLUMNS"
        },
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="",
    )

def whens_for_json_mappings(mappings: dict, column: str, json_column: str):
    whens = None
    for mapping_key in mappings.keys():
        if whens is None:
            whens = when(get_json_object(col(column), f'$.{json_column}') == mapping_key, mappings[mapping_key])
        else:
            whens = whens.when(get_json_object(col(column), f'$.{json_column}') == mapping_key, mappings[mapping_key])
    return whens

def handle_setting(setting_name: str, column_name: str, df: DataFrame, user_account_mappings_setting: dict, personal_properties_name: str) -> DataFrame:
    if user_account_mappings_setting is None:
        return df.withColumn(column_name, lit(''))
    data = json.loads(user_account_mappings_setting.json_value)
    if setting_name in data.keys() and 'field_path' in data[setting_name].keys():
        setting = data[setting_name]
        column = setting['field_path']['column']
        if column == 'personal_properties':
            column = personal_properties_name
        json_column = setting['field_path']['json_column']
        whens = None
        if 'field_converter' in setting.keys():
            if setting['field_converter']['type'] == 'value_mapping':
                mappings = setting['field_converter']['mappings']
                whens = whens_for_json_mappings(mappings, column, json_column).otherwise('')
            if setting['field_converter']['type'] == 'birthmonth':
                date_format = setting['field_converter']['date_format']
                date_format=date_format.replace("YYYY", "yyyy")
                whens = to_timestamp(get_json_object(col(column), f'$.{json_column}'), date_format)
        else:
            whens = get_json_object(col(column), f'$.{json_column}')
        return df.withColumn(column_name, whens)
    else:
        return df.withColumn(column_name, lit(''))

def user_account_mappings_setting(schema, rds, sqlContext):
    setting_rows = get_data_frame(rds, sqlContext, f"""
            SELECT
            *
            FROM {schema}.settings
            WHERE key = 'user_account_personal_properties_field_mappings' 
            """
    ).collect()

    user_account_mappings_setting = None
    if len(setting_rows) == 1:
        user_account_mappings_setting = setting_rows[0]
    return user_account_mappings_setting

def user_account_colummns_sql(user_account_mappings_setting: dict):
    return f"""
                    ua.identifier                           AS user_account_identifier,
                    TRIM(COALESCE(
                        (ua.personal_properties ->> 'customer_gender'),
                        (ua.personal_properties ->> 'gender'),
                        'unknown'))::varchar(256)                    AS demo_gender,
                    TRIM(COALESCE(
                        (ua.personal_properties ->> 'race'),
                        'unknown'))::varchar(256)                    AS demo_race,
                    TRIM(COALESCE(
                        CASE WHEN lower(ua.personal_properties ->> 'primary_cluster_comb') = 'unknown-unknown' THEN 'unknown' ELSE (ua.personal_properties ->> 'primary_cluster_comb') END,
                        'unknown'))::varchar(256)                    AS demo_municipal,
                    TRIM(COALESCE(
                        CASE WHEN lower(ua.personal_properties ->> 'primary_cluster_state') = 'unknown-unknown' THEN 'unknown' ELSE (ua.personal_properties ->> 'primary_cluster_state') END,
                        'unknown'))::varchar(256)                    AS demo_district,
                    TRIM(COALESCE(
                        (ua.personal_properties ->> 'tier'),
                        'unknown'))::varchar(256)                    AS demo_loyalty,
                    EXTRACT(YEAR FROM AGE(current_timestamp , COALESCE(
                        TO_DATE((ua.personal_properties ->> 'customer_date_of_birth'), 'YYYYMMDD'),
                        CASE
                            WHEN (ua.personal_properties ->> 'birthday') = '' THEN NULL
                            WHEN (ua.personal_properties ->> 'birthday') ~ '^((19|20)\d{{2}})(([0])([1-9]{{1}})|(1{{1}}[012])|([1-9]{{1}}))(([0])([1-9]{{1}})|([123]{{1}})([0-9]){{1}})$' THEN TO_DATE((ua.personal_properties ->> 'birthday'), 'YYYYMMDD')
                            WHEN (ua.personal_properties ->> 'birthday') ~ '^(([0])([1-9]{{1}})|(1{{1}}[012])|([1-9]{{1}}))(\/|-|\.)(?:(?:1[6-9]|[2-9]\d)?\d{{2}})$' THEN TO_DATE((ua.personal_properties ->> 'birthday'), 'MM/YYYY')
                            WHEN (ua.personal_properties ->> 'birthday') ~ '^(([1-3]{{1}}[0-9])|(?:0?[1-9]))(\/|-|\.)(([0])([1-9]{{1}})|(1{{1}}[012])|([1-9]{{1}}))(\/|-|\.)(?:(?:1[6-9]|[2-9]\d)?\d{{2}})$' THEN TO_DATE((ua.personal_properties ->> 'birthday'), 'DD/MM/YYYY')
                            WHEN (ua.personal_properties ->> 'birthday') ~ '^(?:(?:1[6-9]|[2-9]\d)?\d{{2}})(\/|-|\.)(([0])([1-9]{{1}})|(1{{1}}[012])|([1-9]{{1}}))(\/|-|\.)(([1-3]{{1}}[0-9])|(?:0?[1-9]))$' THEN TO_DATE((ua.personal_properties ->> 'birthday'), 'YYYY/MM/DD')
                            WHEN (ua.personal_properties ->> 'birth_year') ~ '^(19|20)\d{{2}}$' THEN TO_DATE((ua.personal_properties ->> 'birth_month') || '/' || (ua.personal_properties ->> 'birth_year'), 'MM/YYYY')
                            ELSE NULL
                        END,
                        NULL)))::INT                                 AS demo_age,
                    ua.personal_properties AS personal_properties2,
"""

def add_demo_mapping_columns(df: DataFrame, user_account_mappings_setting: dict) -> DataFrame:
    df = handle_setting('gender', 'demo_gender2', df, user_account_mappings_setting, 'personal_properties2')
    df = handle_setting('race', 'demo_race2', df, user_account_mappings_setting, 'personal_properties2')
    df = handle_setting('birthmonth', 'demo_birthday2', df, user_account_mappings_setting, 'personal_properties2')
    df = df.withColumn("demo_birthday2",col("demo_birthday2").cast('timestamp'))

    # Personal Properties is only needed for above
    df = df.drop('personal_properties2')

    return df

def flatten_json(df: DataFrame, glueContext: GlueContext, schema: str, table: str, args: dict, user_account_mappings_setting: dict) -> DataFrame:
    # User Accounts
    if table == "user_accounts":
        df = handle_setting('gender', 'gender_label', df, user_account_mappings_setting, 'personal_properties')
        df = handle_setting('race', 'race_label', df, user_account_mappings_setting, 'personal_properties')
        df = handle_setting('birthmonth', 'birthmonth_label', df, user_account_mappings_setting, 'personal_properties')
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
        ).withColumn(
            "personal_properties",
            substring(col("personal_properties"), 1, 0)
        )

    # Stored Value Accounts
    if table == "stored_value_accounts":
        df = df.withColumn(
            "pp_mapped",
            get_json_object(col("personal_properties"), '$.mapped')
        ).withColumn(
            "pp_lastname",
            get_json_object(col("personal_properties"), '$.lastname')
        ).withColumn(
            "pp_mobileno",
            get_json_object(col("personal_properties"), '$.mobileno')
        ).withColumn(
            "pp_digital_cardnumber",
            get_json_object(col("personal_properties"), '$.digital_cardnumber')
        ).withColumn(
            "pp_barangay",
            get_json_object(col("personal_properties"), '$.barangay')
        ).withColumn(
            "pp_cardnumber",
            get_json_object(col("personal_properties"), '$.cardnumber')
        ).withColumn(
            "personal_properties",
            substring(col("personal_properties"), 1, 0)
        )

    # Rule Outcomes
    if table == "rule_outcomes":
        df = df.withColumn(
            "additional_info_ids",
            get_json_object(col("additional_info"), '$.ids')
        ).withColumn(
            "additional_info_type",
            get_json_object(col("additional_info"), '$.type')
        ).withColumn(
            "additional_info_status",
            get_json_object(col("additional_info"), '$.status')
        ).withColumn(
            "additional_info_error_code",
            get_json_object(col("additional_info"), '$.error')
        ).withColumn(
            "additional_info",
            substring(col("additional_info"), 1, 0)
        )

    # Reward Transactions
    if table == "reward_transactions":
        df = df.withColumn(
            "p_referral_code",
            get_json_object(col("properties"), '$.referral_code')
        ).withColumn(
            "p_referral_user_account_id",
            get_json_object(col("properties"), '$.referral_user_account_id')
        ).withColumn(
            "p_redemption_pin",
            get_json_object(col("properties"), '$.pin')
        ).withColumn(
            "p_source",
            get_json_object(col("properties"), '$.source')
        ).withColumn(
            "p_reward_price",
            get_json_object(col("properties"), '$.reward_price.price')
        )

    # Campaigns
    if table == "campaigns":
        df = df.withColumn(
            "recurring_day",
            concat_ws(",", col("recurring_day"))
        )

    # Categories
    if table == "categories":
        df = df.withColumn(
            "usage",
            concat_ws(",", col("usage"))
        )

    # Labels
    if table == "labels":
        df = df.withColumn(
            "usage",
            concat_ws(",", col("usage"))
        )

    # Merchant accounts
    if table == "merchant_accounts":
        df = df.withColumn(
            "partner_type",
            concat_ws(",", col("partner_type"))
        )

    # Record Mappings
    if table == "record_mappings":
        df = df.withColumn(
            "acl_global_ids",
            concat_ws(",", col("acl_global_ids"))
        )

    # Reward Campaigns
    if table == "reward_campaigns":
        df = df.withColumn(
            "active_days",
            concat_ws(",", col("active_days"))
        )

    # Stored Value Campaigns
    if table == "stored_value_campaigns":
        df = df.withColumn(
            "review_cycle_months",
            concat_ws(",", col("review_cycle_months"))
        )

    # Abenson - extract mobile numbers
    if schema == "abenson" and table == "file_imports":
        fi = df.select(
            "id",
            "created_at",
            "updated_at",
            "glue_timestamp",
            explode(
                split(
                    regexp_replace(
                        get_json_object(
                            col("information_to_display"),
                            "$.new_mobile_numbers"
                        ), "(\[)|(\])|(\")", ""
                    ), ",\s*"
                )
            ).alias("mobile_number")
        )

        copy_to_redshift(fi, glueContext, schema,
                         "mobile_numbers_flatten", args)
    # File Imports
    # truncate all JSONB
    if table == "file_imports":
        df = df.withColumn(
            "processed_rows",
            substring(col("processed_rows"), 1, 0)
        ).withColumn(
            "non_row_errors",
            substring(col("non_row_errors"), 1, 0)
        ).withColumn(
            "row_counts",
            substring(col("row_counts"), 1, 0)
        ).withColumn(
            "information_to_display",
            substring(col("information_to_display"), 1, 0)
        ).withColumn(
            "error_details",
            substring(col("error_details"), 1, 0)
        )

    return df


def get_data_struct(x:str) -> str:
    if x in '_':
        y = 'VARCHAR(65535) ENCODE LZO'
    elif x in 'int4':
        y = 'INT ENCODE AZ64'
    elif x in 'int8':
        y = 'BIGINT ENCODE AZ64'
    elif x in 'serial':
        y = 'BIGINT ENCODE AZ64'
    elif x in 'varchar':
        y = 'VARCHAR(65535) ENCODE LZO'
    elif x in 'timestamp':
        y = 'TIMESTAMP ENCODE AZ64'
    elif x in 'float8':
        y = 'FLOAT'
    elif x in 'decimal':
        y = 'DECIMAL ENCODE AZ64'
    elif x in 'bool':
        y = 'BOOLEAN ENCODE ZSTD'
    else:
        y = 'VARCHAR(65535) ENCODE LZO' 
    return y
# def get_data_frame_redshift_prod(sqlContext: SQLContext, query: str) -> DataFrame:
#     df = sqlContext.read \
#         .format("jdbc") \
#         .option("url", redshift_connection_prod["url"]) \
#         .option("user", redshift_connection_prod["user"]) \
#         .option("password", redshift_connection_prod["pwd"]) \
#         .option("query", query) \
#         .option("driver", "org.postgresql.Driver") \
#         .option("fetchsize", "5000") \
#         .load()

#     # Return Spark Data Frame
#     return df