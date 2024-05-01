from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *
from prophecy.utils import *
from mortgage_analytics.graph import *

def pipeline(spark: SparkSession) -> None:
    df_csv_mortgage_east = csv_mortgage_east(spark)
    df_csv_mortgage_west = csv_mortgage_west(spark)
    df_clean_up_mortgage_west = clean_up_mortgage_west(spark, df_csv_mortgage_west)
    df_union_east_west = union_east_west(spark, df_csv_mortgage_east, df_clean_up_mortgage_west)
    df_calculations = calculations(spark, df_union_east_west)
    df_troubled_mortgages = troubled_mortgages(spark, df_calculations)
    df_avg_upb_to_value_ratio_by_state = avg_upb_to_value_ratio_by_state(spark, df_troubled_mortgages)
    df_sort_by_state = sort_by_state(spark, df_avg_upb_to_value_ratio_by_state)
    tbl_all_mortgages(spark, df_calculations)
    tbl_troubled_mortgages(spark, df_troubled_mortgages)
    csv_by_state_analysis(spark, df_sort_by_state)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/mortgage_analytics")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/mortgage_analytics", config = Config)(pipeline)

if __name__ == "__main__":
    main()
