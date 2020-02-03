# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
import uuid
import sys
from datetime import datetime
from os import listdir
from os.path import isfile, join
import pandas as pd

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='datascience_workflow',
    default_args=args,
    schedule_interval=None,
)


# [START datascience_workflow]
def start_workflow(ds, **kwargs):
    return 'START'


step_start = PythonOperator(
    task_id='start_workflow',
    provide_context=True, # ds
    python_callable=start_workflow,
    dag=dag,
)

# Task 1
def task_1(input_path, output_path):
    column_names = ['station', 'date', 'latitude', 'longitude', 'elevation', 'tmax']
    df = pd.concat([pd.read_csv(join(input_path, f), names=column_names, \
        header=None, compression='gzip') for f in listdir(input_path)], \
        ignore_index = True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    print(df.head())
    df.to_csv(output_path)
    return "TASK_1"


step_1 = PythonOperator(
    task_id='step_1',
    python_callable=task_1,
    op_kwargs={'input_path': 'tmax-train', \
                'output_path': 'tmax-preprocess/data.csv', \
        },
    dag=dag,
)

# Task 2 with SSHOperator
# Hook to spark_default connection
# sparkSSHHook = SSHHook(ssh_conn_id="spark_default")
# step_2 = SSHOperator(
#     task_id="step_2",
#     remote_host="spark-master",
#     username="root",
#     command="/spark/bin/spark-submit --conf spark.pyspark.python=python3 /workspace/spark-code/weather_train.py /workspace/tmax-preprocess /workspace/tmax-model-1",
#     ssh_hook=sparkSSHHook,
#     dag=dag)

# # Task 2 with PythonOperator
# def task_2(inputs, model_path):
#     from pyspark.sql import SparkSession, functions, types
#     spark = SparkSession.builder.appName('weather prediction').master("spark://spark-master:7077").getOrCreate()
#     spark.sparkContext.setLogLevel('WARN')
#     assert spark.version >= '2.4' # make sure we have Spark 2.4+

#     from pyspark.ml import Pipeline
#     from pyspark.ml.feature import VectorAssembler, SQLTransformer
#     from pyspark.ml.classification import MultilayerPerceptronClassifier
#     from pyspark.ml.regression import RandomForestRegressor
#     from pyspark.ml.evaluation import RegressionEvaluator

#     tmax_schema = types.StructType([
#         types.StructField('station', types.StringType()),
#         types.StructField('date', types.DateType()),
#         types.StructField('latitude', types.FloatType()),
#         types.StructField('longitude', types.FloatType()),
#         types.StructField('elevation', types.FloatType()),
#         types.StructField('tmax', types.FloatType()),
#     ])  
#     data = spark.read.csv(inputs, schema=tmax_schema)
#     train, validation = data.randomSplit([0.75, 0.25])
#     train = train.cache()
#     validation = validation.cache()
    
#     # TODO: create a pipeline to predict tmax value
#     statement = "SELECT latitude, longitude, elevation, dayofyear(date) AS day_of_year, tmax FROM __THIS__"
#     # statement = "SELECT today.latitude, today.longitude, today.elevation, dayofyear(today.date) AS day, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
#     sqlTrans = SQLTransformer(statement=statement)
#     features_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day_of_year"], outputCol="features")
#     # features_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "yesterday_tmax"], outputCol="features")
#     regressor = RandomForestRegressor(featuresCol='features', labelCol='tmax', predictionCol="prediction", numTrees=3, maxDepth=4, seed=15)
#     temperature_pipeline = Pipeline(stages=[sqlTrans, features_assembler, regressor])
#     temperature_model = temperature_pipeline.fit(train)
#     temperature_model.write().overwrite().save(model_path)
#     return "TASK_2"

# step_2 = PythonOperator(
#     task_id='step_2',
#     python_callable=task_2,
#     op_kwargs={'inputs': '/workspace/tmax-preprocess', \
#                 'model_path': '/workspace/tmax-model-1', \
#         },
#     dag=dag,
# )

# Task 2 with SparkSubmitOperator
step_2 =  SparkSubmitOperator(
    task_id='step_2',
    application='/usr/local/airflow/spark-code/weather_train.py',
    application_args=['/workspace/tmax-preprocess','/workspace/tmax-model-1'],
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-spark-step-2',
    verbose=False,
    driver_memory='1g',
    conf={'master':'spark://spark-master:7077'},
    dag=dag,
)

step_start >> step_1
step_1 >> step_2

# [END datascience_workflow]
