from os import environ
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.kubernetes.secret import Secret

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

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
    dag_id = "MLOPS_UAI",
    tags = tags,
    default_args = default_args,
    schedule = "0 8 * * 1",
)


tag = "10"  
proyect_uri = "https://github.com/ml_code_uai"
image_uri = f"registry.{proyect_uri.split('//')[1].split('.gi')[0]}:{tag}"

# Este secret contiene todas las variables de entorno necesarias para 
# la ejecucion del DAG
secret = Secret("env", None, "secret-secrets-mlops-template")

cmds = ["python", "main.py"]

IMAGE_PULL_SECRET = environ["IMAGE_PULL_SECRET"]
IMAGE_PULL_POLICY = environ["IMAGE_PULL_POLICY"]
NODE_SELECTOR = environ["NODE_SELECTOR"]
NODE = environ["NODE"]
NAMESPACE = environ["NAMESPACE"]
SERVICE_ACCOUNT = environ["SERVICE_ACCOUNT"]

kwargs_k8s = {
    "namespace": NAMESPACE,
    "image_pull_secrets": IMAGE_PULL_SECRET,
    "image_pull_policy": "IfNotPresent",
    "startup_timeout_seconds": 300,
    "execution_timeout": timedelta(minutes=60),
    "is_delete_operator_pod": True,
    "in_cluster": True,
    "get_logs": True,
    "pool": "default_pool",
    "node_selector": {NODE_SELECTOR: NODE},
    "dag": dag,
    "service_account_name": SERVICE_ACCOUNT,
    "image": image_uri,
    "cmds": cmds,
}



preprocess = KubernetesPodOperator(
    **kwargs_k8s,
    name="preprocesado",
    task_id="preprocesado",
    arguments=["preproceso"],
    secrets=[secret],
)


node_selector={"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"}

entrenamiento  = KubernetesPodOperator(
    **kwargs_k8s,
    name="entrenamiento",
    task_id="entrenamiento",
    arguments=["entrenamiento", "batch_01"],
    secrets=[secret],
    node_selector=node_selector,
)

prediccion = KubernetesPodOperator(
    **kwargs_k8s,
    name="prediccion",
    task_id="prediccion",
    arguments=["prediccion"],
    secrets=[secret],
)

bigquery = KubernetesPodOperator(
    **kwargs_k8s,
    name="bigquery",
    task_id="predicciones_bigquery",
    arguments=["bigquery", "propuesta"],
    secrets=[secret],
)


(
    preprocess
    >> entrenamiento
    >> prediccion
    >> bigquery
)
