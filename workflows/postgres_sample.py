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

create_table = PostgresOperator(
    task_id='create_table',
    sql='''CREATE TABLE new_table(
        custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
        );''',
    dag=dag,
)

insert_row = PostgresOperator(
    task_id='insert_row',
    sql='INSERT INTO new_table VALUES(%s, %s, %s)',
    trigger_rule=TriggerRule.ALL_DONE,
    parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10]),
    dag=dag,
)

create_table >> insert_row

# Task 2
def task_2():
    pass


step_start >> step_1

# [END datascience_workflow]
