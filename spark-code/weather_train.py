import sys
# assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main(inputs, model_file):
  tmax_schema = types.StructType([
      types.StructField('station', types.StringType()),
      types.StructField('date', types.DateType()),
      types.StructField('latitude', types.FloatType()),
      types.StructField('longitude', types.FloatType()),
      types.StructField('elevation', types.FloatType()),
      types.StructField('tmax', types.FloatType()),
  ])  
  data = spark.read.csv(inputs, schema=tmax_schema)
  train, validation = data.randomSplit([0.75, 0.25])
  train = train.cache()
  validation = validation.cache()
  
  # TODO: create a pipeline to predict tmax value
  statement = "SELECT latitude, longitude, elevation, dayofyear(date) AS day_of_year, tmax FROM __THIS__"
  # statement = "SELECT today.latitude, today.longitude, today.elevation, dayofyear(today.date) AS day, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
  sqlTrans = SQLTransformer(statement=statement)
  features_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day_of_year"], outputCol="features")
  # features_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "yesterday_tmax"], outputCol="features")
  regressor = RandomForestRegressor(featuresCol='features', labelCol='tmax', predictionCol="prediction", numTrees=3, maxDepth=4, seed=15)
  temperature_pipeline = Pipeline(stages=[sqlTrans, features_assembler, regressor])
  temperature_model = temperature_pipeline.fit(train)
  temperature_model.write().overwrite().save(model_file)
