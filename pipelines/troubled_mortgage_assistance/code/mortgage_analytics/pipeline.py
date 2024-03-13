from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *
from prophecy.utils import *
from mortgage_analytics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_tar_troubled_mortgages = tar_troubled_mortgages(spark)
    df_nv_customers = nv_customers(spark)
    df_filter_by_state_nv = filter_by_state_nv(spark, df_tar_troubled_mortgages)
    df_reformat_account_num = reformat_account_num(spark, df_nv_customers)
    df_mtg_asst_program = mtg_asst_program(spark, df_filter_by_state_nv, df_reformat_account_num)
    mtg_assist_prog_HAF_csv(spark, df_mtg_asst_program)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/troubled_mortgage_assistance")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/troubled_mortgage_assistance", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/troubled_mortgage_assistance")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
