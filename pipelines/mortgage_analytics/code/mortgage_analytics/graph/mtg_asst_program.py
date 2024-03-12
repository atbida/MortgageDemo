from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def mtg_asst_program(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.PropID") == col("in1.PropID")), "inner")\
        .select(col("in0.PropID").alias("PropID"), col("in1.First_Name").alias("First_Name"), col("in1.Last_Name").alias("Last_Name"), col("in1.Gender").alias("Gender"), col("in1.Age").alias("Age"), col("in1.Income").alias("Income"), col("in1.`Phone Number`").alias("Phone Number"), col("in1.`Email Address`").alias("Email Address"), col("in0.City").alias("City"), col("in0.State").alias("State"), col("in1.`Zip Code`").alias("Zip Code"), col("in1.`Credit Rating`").alias("Credit Rating"), col("in0.PropertyType").alias("PropertyType"), col("in0.Servicer").alias("Servicer"), col("in0.ServicerType").alias("ServicerType"), col("in0.LoanType").alias("LoanType"), col("in0.UnPaidBalance").alias("UnPaidBalance"), col("in0.CurrentPropValue").alias("CurrentPropValue"), col("in0.LoanStatus").alias("LoanStatus"), col("in0.UPB_to_Value_Ratio").alias("UPB_to_Value_Ratio"))
