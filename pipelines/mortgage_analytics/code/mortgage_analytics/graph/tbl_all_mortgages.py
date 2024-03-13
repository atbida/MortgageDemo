from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def tbl_all_mortgages(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`bobwelshmer`.`mortgage_demo`.`mortgages_all`")
