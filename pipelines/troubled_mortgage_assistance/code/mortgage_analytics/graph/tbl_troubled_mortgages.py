from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def tbl_troubled_mortgages(spark: SparkSession) -> DataFrame:
    return spark.read.table("`pipelinehub`.`anya`.`troubled_mortgages_loandemo`")
