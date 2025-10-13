import time
from urllib.parse import urlparse
import boto3
from awsglue.dynamicframe import DynamicFrame
from source.glue_job import GlueJob
from source.utils.s3 import S3Client
from source.utils.spark import SparkClient
from pyspark.sql.functions import when, col, trim, regexp_replace, lit
from pyspark.sql.types import StringType


class S3ToS3SQLTransform(GlueJob):
    def __init__(self):
        super().__init__(self.init_params, self.run)
    
    def init_params(self):
        self.spark_client = SparkClient(self.context, self.logger)
        self.glueContext = self.context
        self.spark = self.glueContext.spark_session
        self.s3_utils = S3Client()
        self.split_s3_uri = self.s3_utils.split_s3_uri

        print("Spark Configuration:")
        for item in self.spark.sparkContext.getConf().getAll():
            print(f"{item[0]} = {item[1]}")

        self.s3_client = boto3.client("s3")
        self.glue_client = boto3.client("glue")
        self.log_tag = "GLUE DeltaTransform"
        self.job_params = self.config["job_execution_params"]
        self.file_type = self.job_params["file_type"]
        self.condition = self.job_params["condition"]
        self.custom_sql = self.job_params.get("custom_sql", False)
        self.truncate_and_load_sql = self.job_params.get("truncate_and_load", False)
        self.additional_options = {}
        self.source_catalog_database = self.job_params["source_catalog_database"]
        self.target_catalog_database = self.job_params["target_catalog_database"]
        self.target_iceberg_location = self.job_params["target_iceberg_location"]
        self.source_table = self.job_params["source_table"]
        self.target_table = self.job_params["target_table"]

    def check_table_exists(self, database, table):
        table_names = [tbl.name for tbl in self.spark.catalog.listTables(database)]
        return table in table_names

    def create_glue_table(self, table, database, location, exists):
        df = self.rename_columns(self.spark.read.parquet(location))
        glue_schema = self.get_glue_schema(df)

        table_input = {
            "Name": table,
            "StorageDescriptor": {
                "Columns": glue_schema,
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "Name": "parquet",
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
        }

        if exists:
            self.glue_client.delete_table(DatabaseName=database, Name=table)
            time.sleep(5)
            self.glue_client.create_table(DatabaseName=database, TableInput=table_input)
        else:
            self.glue_client.create_table(DatabaseName=database, TableInput=table_input)

        self.logger.info(f"{self.log_tag} Landing table created")
    
    def create_iceberg_table(self, df):
        self.logger.info(f"{self.log_tag} Creating new Iceberg table: glue_catalog.{self.target_catalog_database}.{self.target_table}")

        df.writeTo(f"glue_catalog.{self.target_catalog_database}.{self.target_table}") \
            .tableProperty("format-version", "2") \
            .tableProperty("location", self.target_iceberg_location) \
            .tableProperty("write.parquet.compression-codec", "gzip") \
            .tableProperty("write.target-file-size-bytes", "134217728") \
            .tableProperty("write.metadata.previous-versions-max", "3") \
            .option("iceberg.write.target-file-size-bytes", "134217728") \
            .option("write.spark.accept-any-schema", "true") \
            .options(**self.additional_options) \
            .create()

    def deltaTransformation(self):
        if self.file_type == "parquet_to_iceberg":
            self.logger.info(f"{self.log_tag} Processing Parquet to Iceberg")
            source_table_ddl = f'{self.source_table}_ddl'
            data_type_changes_sql = self.job_params["data_type_changes_sql"]

            if "source_table_location" in self.job_params:
                source_table_location = self.job_params["source_table_location"]
                source_table_exists = self.check_table_exists(self.source_catalog_database, self.source_table)
                self.logger.info(
                    f"{self.log_tag} Source Table - {self.source_table} | Catalog DB - {self.source_catalog_database} | Location - {source_table_location} | Exists -  {source_table_exists}"
                )
                self.create_glue_table(self.source_table, self.source_catalog_database, source_table_location, source_table_exists)
                self.logger.info(f"{self.log_tag} {self.source_catalog_database}.{self.source_table} created.")

            sourceDDF = self.glueContext.create_dynamic_frame.from_catalog(
                database=self.source_catalog_database,
                table_name=self.source_table,
                transformation_ctx=f"{self.source_table}DF",
            )

            self.logger.info(f'{self.log_tag} Checking if table is already available in RAW database')
            table_exists = self.check_table_exists(self.target_catalog_database, self.target_table)
            self.logger.info(f"{self.log_tag} Table exists in RAW database - {table_exists}")

            sourceDF = self.rename_columns(sourceDDF.toDF())
            sourceDF = self.handlingNulls(sourceDF)
            sourceDF.createOrReplaceTempView(self.source_table)
            self.logger.info(f'{self.log_tag} In memory table created from landing source table')

            self.logger.info(f"{self.log_tag} Provided SQL for data type changes - {data_type_changes_sql}")
            sourceDDLChangesDF = self.rename_columns(self.spark.sql(data_type_changes_sql))
            sourceDDLChangesDF = self.handlingNulls(sourceDDLChangesDF)
            sourceDDLChangesDF = sourceDDLChangesDF.dropDuplicates()
            sourceDDLChangesDF.createOrReplaceTempView(source_table_ddl)
            self.logger.info(f'{self.log_tag} In memory table created from base landing table using a source_query specified in params')

            if table_exists is False:
                sourceDDLChangesDF = sourceDDLChangesDF.limit(0)
                self.create_iceberg_table(sourceDDLChangesDF)

            if self.truncate_and_load_sql is False and self.custom_sql is False:
                target_tbl = f"glue_catalog.{self.target_catalog_database}.{self.target_table}"
                source_tbl = source_table_ddl
                self.update_existing_table(target_tbl, source_tbl)

            elif self.truncate_and_load_sql is True:
                target_tbl = f"glue_catalog.{self.target_catalog_database}.{self.target_table}"
                source_tbl = source_table_ddl
                self.truncate_and_load(target_tbl, source_tbl)

            elif self.custom_sql is True:
                queries = self.get_sql_queries()
                self.run_sql_queries(queries)

            self.archive_files()

        elif self.file_type == "iceberg_to_iceberg":
            self.logger.info(f"{self.log_tag} Processing Iceberg to Iceberg")
            sourceDF = self.glueContext.create_data_frame.from_catalog(
                database=self.source_catalog_database,
                table_name=self.source_table,
                transformation_ctx=f"{self.source_table}DF",
            )

            sourceDF = self.rename_columns(sourceDF)
            sourceDF = self.handlingNulls(sourceDF)
            sourceDF = sourceDF.dropDuplicates()
            sourceDF.createOrReplaceTempView(self.source_table)

            table_exists = self.check_table_exists(self.target_catalog_database, self.target_table)
            self.logger.info(f"{self.log_tag} Source Table: {self.target_table} | Catalog DB: {self.target_catalog_database} | Exists: {table_exists}")

            if table_exists is False:
                self.create_iceberg_table(sourceDF)

            if self.truncate_and_load_sql is False and self.custom_sql is False:
                source_tbl = f"glue_catalog.{self.source_catalog_database}.{self.source_table}"
                target_tbl = f"glue_catalog.{self.target_catalog_database}.{self.target_table}"
                self.update_existing_table(target_tbl, source_tbl)

            elif self.truncate_and_load_sql is True:
                source_tbl = f"glue_catalog.{self.source_catalog_database}.{self.source_table}"
                target_tbl = f"glue_catalog.{self.target_catalog_database}.{self.target_table}"
                self.truncate_and_load(target_tbl, source_tbl)

            elif self.custom_sql is True:
                queries = self.get_sql_queries()
                self.run_sql_queries(queries)

        self.logger.info(f"{self.log_tag} deltaTransformation() execution completed")

    def rename_columns(self, df):
        return df.toDF(*[c.strip().replace(" ", "_").replace("-", "_").lower() for c in df.columns])

    def handlingNulls(self, df):
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(
                    field.name,
                    when(
                        col(field.name).isNull() | trim(col(field.name)).isin("", "null"),
                        lit(None)
                    ).otherwise(regexp_replace(col(field.name), r'[\r\n\t]+', ' '))
                )
        return df

    def archive_files(self):
        staging_prefix = self.job_params["source_table_location"]

        if "zone=staging" not in staging_prefix:
            raise ValueError(f"Expected 'zone=staging' in path: {staging_prefix}")

        processed_prefix = staging_prefix.replace("zone=staging", "zone=processed")
        bucket_name, _ = self.split_s3_uri(staging_prefix)

        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=self.split_s3_uri(staging_prefix)[1]):
            for obj in page.get("Contents", []):
                source_key = obj["Key"]
                target_key = source_key.replace("zone=staging", "zone=processed")

                try:
                    self.s3_client.copy_object(
                        CopySource={"Bucket": bucket_name, "Key": source_key},
                        Bucket=bucket_name,
                        Key=target_key
                    )
                    self.s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    self.logger.info(f"{self.log_tag} Archived: s3://{bucket_name}/{source_key} → s3://{bucket_name}/{target_key}")
                except Exception as e:
                    self.logger.info(f"{self.log_tag} Error archiving {source_key} {e}")

    def get_glue_schema(self, df):
        glue_types = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "bigint",
            "DoubleType": "double",
            "FloatType": "float",
            "BooleanType": "boolean",
            "TimestampType": "timestamp",
            "DateType": "date",
            "DecimalType": "decimal",
            "BinaryType": "binary",
            "ShortType": "smallint",
            "ByteType": "tinyint"
        }

        schema = []
        spark_schema = []
        for field in df.schema.fields:
            spark_type = field.dataType.__class__.__name__
            spark_schema.append({"Name": field.name, "Type": spark_type})

            glue_type = glue_types.get(spark_type)
            if not glue_type:
                raise ValueError(f"Unsupported Spark type: {spark_type} for column: {field.name}")
            schema.append({"Name": field.name, "Type": glue_type})

        self.logger.info(f"{self.log_tag} Spark Schema {spark_schema}")
        self.logger.info(f"{self.log_tag} Glue Schema {schema}")
        return schema

    def update_existing_table(self, target_tbl, source_tbl):
        self.logger.info(f"{self.log_tag} Existing table found: {target_tbl}.{source_tbl}. Proceeding with update.")
        key_columns = self.config["job_execution_params"]["key_columns"]
        df_target = self.spark.table(target_tbl)
        df_source = self.spark.table(source_tbl)
        target_columns = set(c.lower() for c in df_target.columns)
        source_columns = set(c.lower() for c in df_source.columns)
        new_columns = source_columns - target_columns

        for col_name in new_columns:
            original_col_name = next(c for c in df_source.columns if c.lower() == col_name.lower())
            col_type = next(f.dataType.simpleString() for f in df_source.schema.fields if f.name == original_col_name)
            alter_statement = f"""ALTER TABLE {target_tbl} ADD COLUMN {col_name} {col_type}"""
            self.logger.info(f"{self.log_tag} {alter_statement}")
            self.spark.sql(alter_statement)

        source_table_cols = df_source.columns
        target_table_cols = df_target.columns

        self.logger.info(f"{self.log_tag} Table - {source_tbl} - Columns - , {source_table_cols}")
        self.logger.info(f"{self.log_tag} Table - {target_tbl} - Columns - , {target_table_cols}")

        common_columns = [c for c in source_table_cols if c in target_table_cols]
        new_columns = [c for c in source_table_cols if c not in target_table_cols]
        missing_columns = [c for c in target_table_cols if c not in source_table_cols]

        self.logger.info(f"{self.log_tag} additional_columns - {new_columns}")
        self.logger.info(f"{self.log_tag} missing_columns - {missing_columns}")

        update_set_clause = ",\n    ".join([f"old.{c} = new.{c}" for c in common_columns + new_columns])
        insert_columns = ", ".join(common_columns + new_columns + missing_columns)
        self.logger.info(f"{self.log_tag} insert_columns - {insert_columns}")

        insert_values = ", ".join([f"new.{c}" for c in common_columns + new_columns] + ["NULL"] * len(missing_columns))
        self.logger.info(f"{self.log_tag} insert_values - {insert_values}")

        merge_statement = f"""
            MERGE INTO {target_tbl} AS old
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_columns} ORDER BY {key_columns}) AS rn
                    FROM {source_tbl}
                ) filtered WHERE rn = 1
            ) new
            ON {self.condition}
            WHEN MATCHED THEN UPDATE SET
                {update_set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
            ;
        """

        self.logger.info(f"{self.log_tag} Executing MERGE statement\n{merge_statement.strip()}")
        self.spark.sql(merge_statement.strip())

    def truncate_and_load(self, target_tbl, source_tbl):
        self.logger.info(f"{self.log_tag} Proceeding with TRUNCATE and LOAD - {target_tbl}. ")

        df_target = self.spark.table(target_tbl)
        df_source = self.spark.table(source_tbl)

        self.logger.info(f"{self.log_tag} - {source_tbl} - {target_tbl}")

        target_col_set = {c.lower() for c in df_target.columns}
        source_columns = [c for c in df_source.columns if c.lower() in target_col_set]

        truncate_statement = f"TRUNCATE TABLE {target_tbl}"
        self.logger.info(f"{self.log_tag} - Truncate statement\n{truncate_statement}")
        self.spark.sql(truncate_statement)

        self.logger.info(f"{self.log_tag} Table - {source_tbl} - Columns - {source_columns}")
        self.logger.info(f"{self.log_tag} Table - {target_tbl} - Columns - {df_target.columns}")

        target_col_str = ", ".join(source_columns)
        source_col_str = ", ".join(source_columns)

        insert_statement = f"""
            INSERT INTO {target_tbl}
            ({target_col_str})
            SELECT {source_col_str} FROM {source_tbl}
        """

        self.logger.info(f"{self.log_tag} Executing INSERT statement\n{insert_statement.strip()}")
        self.spark.sql(insert_statement.strip())

    def get_sql_queries(self):
        self.logger.info("GETTING SQL QUERIES FROM S3")
        response = self.s3_client.get_object(
            Bucket=self.config["job_execution_params"]["sql_s3_bucket"],
            Key=self.config["job_execution_params"]["sql_s3_key"]
        )
        sql_file = response["Body"].read().decode("utf-8")
        sql_queries = list(filter(None, sql_file.strip().split(";")))
        self.logger.info(f'SQL queries uploaded on S3 - \n{sql_queries}')
        return sql_queries

    def run_sql_queries(self, sql_queries):
        try:
            self.logger.info(f"running {len(sql_queries)} queries from S3 file")
            self.spark_client.spark_run_sql(sql_queries)
            self.logger.info("Queries from S3 path ran successfully.")
            return True
        except Exception as e:
            self.logger.info("Error occurred while running query.")
            params = {"error": str(e)}
            self.logger.info(params)
            raise Exception(f"ERROR - Running SQL query - {e}")

    def run(self):
        try:
            self.deltaTransformation()
            self.logger.info(f"{self.log_tag} Job completed successfully.")
        except Exception as e:
            self.logger.info(f"{self.log_tag} Error occurred while executing run(self) function.")
            raise Exception(f"ERROR: {e}")


if __name__ == "__main__":
    S3ToS3SQLTransform().run_job()
