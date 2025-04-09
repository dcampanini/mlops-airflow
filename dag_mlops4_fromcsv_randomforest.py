from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import pandas as pd
from sklearn import preprocessing
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, precision_recall_curve,
                                 roc_auc_score)
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
from google.cloud import aiplatform
import subprocess

import sklearn
import sys



YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
GCS_BUCKET_NAME = "mlops_airflow_data"  # Your GCS bucket

#Define default arguments
default_args = {
    "owner": "DiegoCampanini",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=50),
    "start_date": YESTERDAY,
}

# Instantiate your DAG
tags = ['uai', 'mlops']
dag = DAG(
        dag_id = 'mlops_dag_csv1', 
        tags = tags,
        default_args=default_args, 
        schedule_interval= '0 8 * * wed'
    )

# Define tasks
def read_and_preprocessing():
    df = pd.read_csv('gs://mlops_airflow_data/pima-indians-diabetes.csv')
    scaler = preprocessing.StandardScaler().fit(df)
    # split data into X and y
    X = df.iloc[:,0:8] # features columns
    Y = df.iloc[:,8] # label column
    # split data into train and test sets
    seed = 7
    test_size = 0.2
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size, random_state=seed)
    X_train.to_csv('gs://mlops_airflow_data/X_train.csv', index=False)
    X_test.to_csv('gs://mlops_airflow_data/X_test.csv', index=False)
    y_train.to_csv('gs://mlops_airflow_data/y_train.csv', index=False)
    y_test.to_csv('gs://mlops_airflow_data/y_test.csv', index=False)

    print ("Prerpocessing Task Completed")


def train(model_name:str):
    X_train = pd.read_csv('gs://mlops_airflow_data/X_train.csv')
    y_train = pd.read_csv('gs://mlops_airflow_data/y_train.csv')
    X_test = pd.read_csv('gs://mlops_airflow_data/X_test.csv')
    y_test = pd.read_csv('gs://mlops_airflow_data/y_test.csv')


    clf = RandomForestClassifier(max_depth=2, random_state=0)
    clf.fit(X_train, y_train)
    predictions = clf.predict(X_test)
    score = accuracy_score(y_test, predictions)
    auc = roc_auc_score(y_test, predictions)
    print('accuracy:   %0.3f' % score)
    print('auc:   %0.3f' % auc)
    # Save Model
    # Save locally first
    local_path = f"/tmp/{model_name}.joblib"  # Use a valid local directory
    joblib.dump(clf, local_path, compress=3, protocol=4)
    print(f"Model saved locally at {local_path}")

    print("Python Version:", sys.version)
    print("Joblib Version:", joblib.__version__)
    print("Scikit-learn Version:", sklearn.__version__)

    # Upload to GCS using Airflow's GoogleCloudStorageHook
    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=f"models/{model_name}/model.joblib",  # Path in GCS
        filename=local_path
    )
    
    print(f"Model uploaded to gs://{GCS_BUCKET_NAME}/models/{model_name}/{model_name}.joblib")
    print ("Training Task Completed")
    

def deploy_model_to_vertex(MODEL_NAME: str):
    # Get your Google Cloud project ID from gcloud
    if not os.getenv("IS_TESTING"):
        shell_output = subprocess.check_output(
                "gcloud config list --format 'value(core.project)' 2>/dev/null",
                shell=True, text=True).strip()
        PROJECT_ID = shell_output
        print("Project ID: ", PROJECT_ID)

    ENDPOINT_NAME = "mlops-endpoint"  # endpoint name that will be displayed in Vertex
    MODEL_URI = f"gs://{GCS_BUCKET_NAME}/models/{MODEL_NAME}/"
    # ✅ Initialize Vertex AI client
    aiplatform.init(project=PROJECT_ID, location="us-central1")

    # ✅ Upload model to Vertex AI
    model = aiplatform.Model.upload(
        display_name=MODEL_NAME,
        artifact_uri=MODEL_URI,  # GCS path where model is stored, not the full to the .joblib
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",  # Update if using TensorFlow/PyTorch
    )
    print(f"Model: {model}")
    print(f"Model uploaded: {model.resource_name}")
    
    endpoint = aiplatform.Endpoint.create(display_name=ENDPOINT_NAME)
    # ✅ Deploy model to endpoint
    deployed_model = model.deploy(endpoint=endpoint, machine_type="n1-standard-2")  # Adjust based on your needs)
    print(f"Model deployed to endpoint: {deployed_model.resource_name}")

    print('Model deployed to a Vertex endpoint')
    return True


prepro = PythonOperator(
 task_id='preprocessing',
 python_callable=read_and_preprocessing,
 dag=dag,
)
training1 = PythonOperator(
 task_id='train1',
 python_callable=train,
 op_kwargs={"model_name": "model1"},  
 dag=dag,
)
training2 = PythonOperator(
 task_id='train2',
 python_callable=train,
 op_kwargs={"model_name": "model2"},  
 dag=dag,
)
deploy = PythonOperator(
 task_id='deploy_model',
 python_callable=deploy_model_to_vertex,
 op_kwargs={"MODEL_NAME": "model2"}, 
 dag=dag,
)

# Set task dependencies
prepro >> [training1, training2] >> deploy