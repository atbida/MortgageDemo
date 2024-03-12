from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def sort_by_state(spark: SparkSession, avg_upb_to_value_ratio_by_state: DataFrame) -> DataFrame:
    return avg_upb_to_value_ratio_by_state.orderBy(col("prop_count").desc())
