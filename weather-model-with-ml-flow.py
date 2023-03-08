import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import DecisionTreeClassifier
import mlflow

# Let's prepare ML Flow
mlflow.set_experiment("my_first_exp")

# Create a Spark Session
spark = SparkSession\
    .builder\
    .appName("WeatherModel")\
    .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.2.1")\
    .getOrCreate()

# Read Weather Files Generated by datagen
df = spark.read.parquet("/user/datagen/hdfs/publicservice/weather/")
df.printSchema()
df.show(1)

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

# Label the column 
def fromBooleanToInt(s):
  if s == 'true':
    return 1
  elif s =='false':
    return 0
  else:
    return None

fromBooleanToInt_udf = udf(lambda x: fromBooleanToInt(x), IntegerType())
df_labeled = vectorized.withColumn("label", fromBooleanToInt_udf(vectorized["rain"]))

# Prepare data for training and testing
splittedData = df_labeled.randomSplit((0.8,0.2),228)

train_data = splittedData[0]
test_data = splittedData[1]


# Let's make a Linear Regression first:
mlflow.start_run()
mlflow.log_param("model_type", "linear_regression")

lr = LinearRegression()
model = lr.fit(train_data)

# Predict ! 
predictions = model.transform(test_data)

number_of_tested_rows = test_data.count()
number_of_rain_predicted_but_no_rain = predictions.select("*").where("label = 0 and prediction > 0.5").count()
number_of_no_rain_predicted_but_rain = predictions.select("*").where("label = 1 and prediction < 0.5").count()
success_rate = 1-(number_of_rain_predicted_but_no_rain+number_of_no_rain_predicted_but_rain)/number_of_tested_rows


# End first ML flow
mlflow.log_param("number_of_tested_rows", number_of_tested_rows)

mlflow.log_metric("success_rate", success_rate)
mlflow.log_metric("number_of_rain_predicted_but_no_rain", number_of_rain_predicted_but_no_rain)
mlflow.log_metric("number_of_no_rain_predicted_but_rain", number_of_no_rain_predicted_but_rain)

mlflow.end_run()


#Now, Let's make a Decision Tree:
mlflow.start_run()
mlflow.log_param("model_type", "decision_tree")

dt = DecisionTreeClassifier(maxDepth=6)
model = dt.fit(train_data)

# Predict ! 
predictions = model.transform(test_data)

number_of_tested_rows = test_data.count()
number_of_rain_predicted_but_no_rain = predictions.select("*").where("label = 0 and prediction > 0.5").count()
number_of_no_rain_predicted_but_rain = predictions.select("*").where("label = 1 and prediction < 0.5").count()
success_rate = 1-(number_of_rain_predicted_but_no_rain+number_of_no_rain_predicted_but_rain)/number_of_tested_rows


# End second ML flow
mlflow.log_param("number_of_tested_rows", number_of_tested_rows)

mlflow.log_metric("success_rate", success_rate)
mlflow.log_metric("number_of_rain_predicted_but_no_rain", number_of_rain_predicted_but_no_rain)
mlflow.log_metric("number_of_no_rain_predicted_but_rain", number_of_no_rain_predicted_but_rain)

#mlflow.end_run()


# Save model as ML flow locally
import mlflow.spark
mlflow.spark.save_model(model, "model-spark-mlflow")