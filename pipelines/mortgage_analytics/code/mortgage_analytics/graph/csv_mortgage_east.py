from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def csv_mortgage_east(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("PropID", StringType(), True), StructField("City", StringType(), True), StructField("State", StringType(), True), StructField("PropertyType", StringType(), True), StructField("Servicer", StringType(), True), StructField("ServicerType", StringType(), True), StructField("LoanType", StringType(), True), StructField("UnPaidBalance", DoubleType(), True), StructField("CurrentPropValue", DoubleType(), True), StructField("LoanStatus", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/mortgage/MortgageEast.csv")
