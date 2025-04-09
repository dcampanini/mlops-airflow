from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime

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
        dag_id = 'mlops_dag2', 
        tags = tags,
        default_args=default_args, 
        schedule_interval= '0 8 * * wed'
    )

# Define tasks
def preprocessing():
    from sklearn import preprocessing
    import numpy as np
    X_train = np.array([[ 1., -1.,  2.],
                         [ 2.,  0.,  0.],
                         [ 0.,  1., -1.]])
    scaler = preprocessing.StandardScaler().fit(X_train)
    print ("Executing prerpocessing Task")


def train():
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.datasets import make_classification
    X, y = make_classification(n_samples=1000, n_features=4,
                            n_informative=2, n_redundant=0,
                            random_state=0, shuffle=False)
    clf = RandomForestClassifier(max_depth=2, random_state=0)
    clf.fit(X, y)
    print('class predicted = ', clf.predict([[0, 0, 0, 0]]))
    

def deploy_model():
    print('deploying model to an endpoint')
    return True


prepro = PythonOperator(
 task_id='preprocessing',
 python_callable=preprocessing,
 dag=dag,
)
training1 = PythonOperator(
 task_id='train1',
 python_callable=train,
 dag=dag,
)
training2 = PythonOperator(
 task_id='train2',
 python_callable=train,
 dag=dag,
)
deploy = PythonOperator(
 task_id='deploy_model',
 python_callable=deploy_model,
 dag=dag,
)

# Set task dependencies
prepro >> [training1, training2] >> deploy