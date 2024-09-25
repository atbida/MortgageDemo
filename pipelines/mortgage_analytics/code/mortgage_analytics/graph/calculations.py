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
        lit("Property Type").alias("PropertyType"), 
        col("Servicer"), 
        col("Servicer").alias("Type").alias("ServicerType"), 
        lit("Loan Type").alias("LoanType"), 
        lit("UnPaid Balance").alias("UnPaidBalance"), 
        lit("Current Prop Value").alias("CurrentPropValue"), 
        lit("Loan Status").alias("LoanStatus"), 
        (col("`UnPaid Balance`") / col("`Current Prop Value`")).alias("UPBtoValueRatio")
    )
