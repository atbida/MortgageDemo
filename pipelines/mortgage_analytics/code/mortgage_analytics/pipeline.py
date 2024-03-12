from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *
from prophecy.utils import *
from mortgage_analytics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_mortgage_west = mortgage_west(spark)
    df_clean_up_mortgage_west = clean_up_mortgage_west(spark, df_mortgage_west)
    df_mortgage_east = mortgage_east(spark)
    df_union_east_west = union_east_west(spark, df_mortgage_east, df_clean_up_mortgage_west)
    df_calculations = calculations(spark, df_union_east_west)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/mortgage_analytics")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/mortgage_analytics", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/mortgage_analytics")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
