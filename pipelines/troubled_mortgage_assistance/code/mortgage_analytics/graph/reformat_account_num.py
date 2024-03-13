from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def reformat_account_num(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(lit("ACCT_"), col("`Cust ID`")).alias("PropID"), 
        col("First_Name"), 
        col("Last_Name"), 
        col("Gender"), 
        col("Age"), 
        col("Income"), 
        col("`Phone Number`"), 
        col("`Email Address`"), 
        col("`Zip Code`"), 
        col("`Credit Rating`")
    )
