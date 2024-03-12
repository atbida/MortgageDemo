from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def clean_up_mortgage_west(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Account").alias("PropID"), 
        col("City"), 
        initcap(col("State")).alias("State"), 
        col("`Property Type`").alias("PropertyType"), 
        col("Servicer"), 
        col("`Servicer Type`").alias("ServicerType"), 
        col("`Loan Type`").alias("LoanType"), 
        col("`UnPaid Balance`").alias("UnPaidBalance"), 
        col("`Current Prop Value`").alias("CurrentPropValue"), 
        when((col("`0`") > lit(0)), lit("0"))\
          .when((col("`30`") > lit(0)), lit("30"))\
          .when((col("`60`") > lit(0)), lit("60"))\
          .when((col("`90`") > lit(0)), lit("90"))\
          .when((col("REO") > lit(0)), lit("REO"))\
          .alias("LoanStatus")
    )
