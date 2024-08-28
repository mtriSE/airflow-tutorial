# First step, import module
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import airflow.utils
import airflow.utils.dates

# Second step, define default and DAG-specific arguments
defalt_args = {
    'owner': 'mqtri',
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(2024,12,30),
    # true -> current task will dependent on previous task, if previous task failed, today task trigger.
    'depends_on_past': False,
    'email': ['mtri0162@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Third step, instantiate a DAG
dag = DAG(
    'mqtri_dag',
    default_args=defalt_args,
    description='A simple DAG was created by me',
    # Continue to run DAG once per day
    # how often a dag is triggered = '@dally'
    schedule_interval=timedelta(days=1)
)

# Forth step, lay out task
t1 = BashOperator(
    task_id='print_date',
    bash_command='date && pwd',
    dag=dag
)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
    echo "{{ params.other_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': "Hello world", "other_param": "Mai Quoc Tri"},
    dag=dag
)

# Fifth step, setting up Dependencies

# t2 will depend on t1
t1.set_downstream(t2) # t1 >> t2

# t3 will depend on t1
t3.set_upstream(t1) #