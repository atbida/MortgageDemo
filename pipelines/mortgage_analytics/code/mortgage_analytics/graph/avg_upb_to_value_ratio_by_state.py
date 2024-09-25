from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def avg_upb_to_value_ratio_by_state(spark: SparkSession, troubled_mortgages: DataFrame) -> DataFrame:
    df1 = troubled_mortgages.groupBy(col("State"))

    return df1.agg(count(col("PropID")).alias("prop_count"), avg(col("UPBtoValueRatio")).alias("UPB_to_Value_Ratio"))
