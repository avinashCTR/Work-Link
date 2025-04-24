from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta
import textwrap

default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries':0,
    'config_group':"config_group_jiomart"
}

modelPath = "/data1/searchengine/EmbeddingTrainingDataCuration/ajio/TrainedModels/mpnet_v3_ep3"
testDataPath = "/data1/archive/avinash/ModelTestingData2"
outputPrefix = "MpnetOutputs"


dag = DAG('search_engine_embeddings_compare', default_args=default_args,schedule_interval=None)

env_command = "conda activate /data/archita/searchengine_nlp"

################################################ SpellCheck Testing ##############################################################
"""
In this tasks the inputs are 
1. Model
2. SpellCheck and WordBreak data merged into one , here we have three columns
   query,mistake_queries,category

the output will be a csv report with the metrics on simmilarity score the model produces on the two query columns
"""
bash_command = textwrap.dedent(
    f"""
    {env_command} && sh /app/notebooks/avinash/ModelTestingDag/EmbeddingsCompare.sh \
    --model_path "{modelPath}" \
    --query_file_path "{testDataPath}/inputs/spellCheckAndWordBreak.csv" \
    --report_csv_path "{testDataPath}/outputs/{outputPrefix}/spellCheckAndWordBreakReport.csv" \
    --query_col1 "query" \
    --query_col2 "mistake_queries" \
    --n 20000 \
    --model_name "mpnet_v3_ep3" \
    --batch_size 1024 \
    --step_size 100000 \
    --multi_gpu True
    """
)
SpellCheck = SSHOperator(
    task_id="SpellCheck",
    ssh_conn_id="AIRFLOW_CONN_SSH_SERVER",
    command=f"""ssh -p 8509 jioapp@10.166.181.219 "{bash_command}" """,
    dag=dag
)

############################################### SearchStability ####################################################################
"""
In this tasks the inputs are 
1. Model
2. UKUS, Hinglish and Shuffled queries data merged into one , here we have three columns
   query,mistake_queries,category

the output will be a csv report with the metrics on simmilarity score the model produces on the two query columns
"""
bash_command = textwrap.dedent(
    f"""
    {env_command} && sh /app/notebooks/avinash/ModelTestingDag/EmbeddingsCompare.sh \
    --model_path "{modelPath}" \
    --query_file_path "{testDataPath}/inputs/SearchStability.parquet" \
    --report_csv_path "{testDataPath}/outputs/{outputPrefix}/SearchStabilityReport.csv" \
    --query_col1 "query" \
    --query_col2 "mistake_queries" \
    --n 200000 \
    --model_name "mpnet_v3_ep3" \
    --batch_size 1024 \
    --step_size 100000 \
    --multi_gpu True
    """
)
SearchStabality = SSHOperator(
    task_id="SearchStability",
    ssh_conn_id="AIRFLOW_CONN_SSH_SERVER",
    command=f"""ssh -p 8509 jioapp@10.166.181.219 "{bash_command}" """,
    dag=dag
)

SearchStabality.set_upstream(SpellCheck)

########################################### Non-Model analysis #####################################################################
