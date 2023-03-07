from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import DecisionTreeClassificationModel


def predict(wind_p_9_am, wind_f_9am, wind_p_9_pm, wind_f_9_pm, p_9am, p_9pm, h_9am, h_9pm, t_9am, t_9pm):
    
    # Create a Spark Session
    spark = SparkSession\
        .builder\
        .appName("WeatherModel")\
        .getOrCreate()

    # Load Spark model from HDFS
    model_loaded = DecisionTreeClassificationModel.load("hdfs://user/francois/model_spark")

    columns = ['wind_provenance_9_am' ,  'wind_force_9_am', "wind_provenance_9_pm", "wind_force_9_pm", "pressure_9_am", "pressure_9_pm","humidity_9_am", "humidity_9_pm", "temperature_9_am", "temperature_9_pm"]
    data = [(wind_p_9_am, wind_f_9am, wind_p_9_pm, wind_f_9_pm, p_9am, p_9pm, h_9am, h_9pm, t_9am, t_9pm)]
    rdd = spark.sparkContext.parallelize(data)
    df = spark.createDataFrame(rdd).toDF(*columns)

    # Index wind_provenance columns (to transform string to int)
    df_index_wind_9am = StringIndexer(inputCol="wind_provenance_9_am", outputCol="wind_provenance_9_am_index").fit(df).transform(df)
    df_index_wind_9pm = StringIndexer(inputCol="wind_provenance_9_pm", outputCol="wind_provenance_9_pm_index").fit(df_index_wind_9am).transform(df_index_wind_9am)

    # Create features Vector
    vecAssembler = VectorAssembler(outputCol="features")
    vecAssembler.setInputCols(
      ["wind_force_9_am", "wind_force_9_pm", "pressure_9_am", "pressure_9_pm",
       "humidity_9_am", "humidity_9_pm", "temperature_9_am", "temperature_9_pm",
      "wind_provenance_9_pm_index", "wind_provenance_9_am_index"])
    vectorized = vecAssembler.transform(df_index_wind_9pm)


    # Test the model
    predictions = model_loaded.transform(vectorized)

    # Return result
    raw_result = predictions.select("prediction").take(1)[0]['prediction']
    if raw_result == 0:
      return "It will not rain"
    else:
      return "it will rain"
