from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *
from prophecy.utils import *
from mortgage_analytics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_mortgage_east = mortgage_east(spark)
    df_mortgage_west = mortgage_west(spark)
    df_clean_up_mortgage_west = clean_up_mortgage_west(spark, df_mortgage_west)
    df_union_east_west = union_east_west(spark, df_mortgage_east, df_clean_up_mortgage_west)
    df_calculations = calculations(spark, df_union_east_west)
    df_troubled_mortgages = troubled_mortgages(spark, df_calculations)
    df_avg_upb_to_value_ratio_by_state = avg_upb_to_value_ratio_by_state(spark, df_troubled_mortgages)
    df_sort_by_state = sort_by_state(spark, df_avg_upb_to_value_ratio_by_state)
    df_customers_nv = customers_nv(spark)
    all_mortgages_calc(spark, df_calculations)
    df_filter_by_state_nv = filter_by_state_nv(spark, df_troubled_mortgages)
    df_reformat_account_num = reformat_account_num(spark, df_customers_nv)
    df_mtg_asst_program = mtg_asst_program(spark, df_filter_by_state_nv, df_reformat_account_num)
    by_state_analysis(spark, df_sort_by_state)
    mtg_assist_prog_csv(spark, df_mtg_asst_program)

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
