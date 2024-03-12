from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def calculations(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PropID"), 
        col("City"), 
        initcap(col("State")).alias("State"), 
        col("PropertyType"), 
        col("Servicer"), 
        col("ServicerType"), 
        col("LoanType"), 
        col("UnPaidBalance"), 
        col("CurrentPropValue"), 
        col("LoanStatus"), 
        (col("UnPaidBalance") / col("CurrentPropValue")).alias("UPB_to_Value_Ratio")
    )
