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
from airflow.utils.trigger_rule import TriggerRule

import uuid
import sys
from datetime import datetime
from os import listdir
from os.path import isfile, join
import pandas as pd
from sklearn.datasets import fetch_openml
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.utils import check_random_state

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='mnist_workflow',
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
def task_1(output_path_X, output_path_y):
    
    # COLLECT DATA AND DO PERMUTATION AND SAVE WHOLE DATA 
    X, y = fetch_openml('mnist_784', version=1, return_X_y=True)
    random_state = check_random_state(0)
    permutation = random_state.permutation(X.shape[0])
    X = X[permutation]
    y = y[permutation]
    X = X.reshape((X.shape[0], -1))

    pd.DataFrame(X).to_csv(output_path_X, index=False)
    pd.DataFrame(y).to_csv(output_path_y, index=False)

    return "TASK_1"


step_1 = PythonOperator(
    task_id='step_1',
    python_callable=task_1,
    op_kwargs={'output_path_X': 'mnist-preprocess/X_data.csv', \
               'output_path_y': 'mnist-preprocess/y_data.csv',
        },
    dag=dag,
)


# SPLIT DATA INTO TRAINING AND TESTING SETS
# SAVE TO CSV FROM PANDAS
def task_2(input_path_X, input_path_y, \
           output_path_Xtrain, output_path_ytrain, \
           output_path_Xtest, output_path_ytest):

    # df_X = pd.read_csv(input_path_X)
    # df_y = pd.read_csv(input_path_y)
    X = pd.read_csv(input_path_X).to_numpy()
    y = pd.read_csv(input_path_y).to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(
    X, y, train_size=train_samples, test_size=10000)

    pd.DataFrame(X_train).to_csv(output_path_Xtrain)
    pd.DataFrame(y_train).to_csv(output_path_ytrain)
    pd.DataFrame(X_test).to_csv(output_path_Xtest)
    pd.DataFrame(y_test).to_csv(output_path_ytest)

    return "TASK_2"


step_2 = PythonOperator(
    task_id='step_2',
    python_callable=task_2,
    op_kwargs={'input_path_X': 'mnist-preprocess/X_data.csv', \
               'input_path_y': 'mnist-preprocess/y_data.csv', \
               'output_path_Xtrain': 'mnist-preprocess/Xtrain_data.csv', \
               'output_path_ytrain': 'mnist-preprocess/ytrain_data.csv', \
               'output_path_Xtest': 'mnist-preprocess/Xtest_data.csv', \
               'output_path_ytest': 'mnist-preprocess/ytest_data.csv', 
        },
    dag=dag,
)


# APPLY MODEL 1 TO TRAINING DATA
def task_3(input_path_Xtrain, input_path_ytrain, model_path):

    X_train = pd.read_csv(input_path_Xtrain).to_numpy()
    y_train = pd.read_csv(input_path_ytrain).to_numpy()

    scaler = StandardScaler()

    X_train = scaler.fit_transform(X_train)
    clf = LogisticRegression(C=50. / train_samples, penalty='l1', solver='saga', tol=0.1)
    clf.fit(X_train, y_train)

    with open(model_path, 'wb') as f:
        pickle.dump(clf, f)

    return "TASK_3"

step_3 = PythonOperator(
    task_id='step_3',
    python_callable=task_3,
    op_kwargs={'input_path_Xtrain': 'mnist-preprocess/Xtrain_data.csv', \
               'input_path_ytrain': 'mnist-preprocess/ytrain_data.csv', \
               'model_path': 'mnist-model/model1',
        },
    dag=dag,
)


# APPLY MODEL 2 TO TRAINING DATA
def task_4(input_path_Xtrain, input_path_ytrain, model_path):
    X_train = pd.read_csv(input_path_Xtrain).to_numpy()
    y_train = pd.read_csv(input_path_ytrain).to_numpy()

    scaler = StandardScaler()

    X_train = scaler.fit_transform(X_train)
    # CHANGE THE MODEL
    clf = SGDClassifier(C=50. / train_samples, penalty='l1', solver='saga', tol=0.1)
    clf.fit(X_train, y_train)

    with open(model_path, 'wb') as f:
        pickle.dump(clf, f)

    return "TASK_4"

step_4 = PythonOperator(
    task_id='step_4',
    python_callable=task_4,
    op_kwargs={'input_path_Xtrain': 'mnist-preprocess/Xtrain_data.csv', \
               'input_path_ytrain': 'mnist-preprocess/ytrain_data.csv', \
               'model_path': 'mnist-model/model2',
        },
    dag=dag,
)


# PREDICT ON THE TEST DATA USING 2 MODELS
def task_5(input_path_Xtest, input_path_ytest, \
           model1_path, model2_path):

    X_test = pd.read_csv(input_path_Xtest).to_numpy()
    y_test = pd.read_csv(input_path_ytest).to_numpy()

    with open(model1_path, 'rb') as f:
        clf1 = pickle.load(f)

    with open(model2_path, 'rb') as f:
        clf2 = pickle.load(f)

    scaler = StandardScaler()
    X_test = scaler.transform(X_test)

    print("MODEL1 DETAILS")
    sparsity = np.mean(clf1.coef_ == 0) * 100
    score = clf1.score(X_test, y_test)
    # print('Best C % .4f' % clf.C_)
    print("Sparsity with L1 penalty: %.2f%%" % sparsity)
    print("Test score with L1 penalty: %.4f" % score)

    print("--------------------")
    print("MODEL2 DETAILS")
    sparsity = np.mean(clf2.coef_ == 0) * 100
    score = clf2.score(X_test, y_test)
    # print('Best C % .4f' % clf.C_)
    print("Sparsity with L1 penalty: %.2f%%" % sparsity)
    print("Test score with L1 penalty: %.4f" % score)

    return "TASK_5"

step_5 = PythonOperator(
    task_id='step_5',
    python_callable=task_5,
    op_kwargs={'input_path_Xtest': 'mnist-preprocess/Xtest_data.csv', \
               'input_path_ytest': 'mnist-preprocess/ytest_data.csv', \
               'model1_path': 'mnist-model/model1', \
               'model2_path': 'mnist-model/model2',
        },
    dag=dag,
)



step_start >> step_1
step_1 >> step_2
step_2 >> step_3
step_2 >> step_4
[step_3, step_4] >> step_5

# [END datascience_workflow]
