import json
import textwrap
import pytz

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from CoutureSpark3Plugin import CoutureSpark3Operator as CoutureSparkOperator
from CouturePythonDockerPlugin import CouturePythonDockerOperator

env = "prod" # possible values: prod, sit, replica
assert env in ["prod", "sit", "replica"]

opposite = lambda x: ("true" if x=="false" else "false")

# ========================================== Configuration ==========================================
conf = {
    "embedding_model_path": "/data/models/stella_checkpoint_3ep_new_desc_bin",
    "embedding_model_name": "latest-stable",
    "embedding_model_batch_size": None,
    "embedding_model_step_size": None,
    "hdfs_connection_available_to_remote": "False",
    "remote_base_dir": "/data/hdfs",
    "rwc_current_base_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current",
    "rwc_archive_base_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/archive",
    "rwc_spell_corrected_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/spell_corrected_queries",
    "rwc_embeddings_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/query_embeddings",
    "rwc_embeddings_cache_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/query_embeddings_cache",
    "rwc_guardrails_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/guardrails",
    "rwc_guardrails_existing_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/guardrails",
    "rwc_qdrant_response_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/qdrant_response",
    "rwc_qdrant_facets_response_output_path": f"/data1/searchengine/genai_search/ajio/rwc/{env}/current/qdrant_facets_response",
    "rwc_num_queries_to_generate_embeddings": None,
    "rwc_tasks_run_flags": {}
}

# ========================================== scala jars ==========================================
genai_etl_jar = "couture-search-engine-etl-2.0.0-genai-prod.jar"

# ========================================== python eggs and volumes ==========================================
python_image = "couture/python-search-image:1.1.2"
python_image_qdrant = "couture/qdrant-client:1.0"
nltk_volume = "nltk_volume:/home/jovyan/nltk_data:Z"
searchEngineVolume = "searchEngineVolume:/home/pythondockervolume:Z"
python_egg = "couture_search_genai_prod-2.0.0-py3.9.egg"

# ========================================== gpu environment commands ==========================================
source_env_cmd = "conda activate /data/envs/searchengine_nlp"

# ========================================== embedding model ==========================================
embedding_model_path = conf.get("embedding_model_path", "/data/models/stella_checkpoint_3ep_new_desc_bin")
embedding_model_name = conf.get("embedding_model_name", "latest-stable")
batch_size = conf.get("embedding_model_batch_size")
step_size = conf.get("embedding_model_step_size")

# ========================================== Remote processing arguments ==========================================
hdfs_connection_available_to_remote = eval(conf.get("hdfs_connection_available_to_remote", "False"))
remote_base_dir = conf.get("remote_base_dir", "/data/hdfs")

# ========================================== DAG Arguments ==========================================
default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'class_path': "ai.couture.obelisk.search.MainClass",
    'code_artifact': genai_etl_jar,
    "config_group": "config_group_jiomart_large",
    "priority_weight": 1
}

schedule = None
Dag = DAG(
    'search_engine_genai_data_update_prod_avinash',
    default_args=default_args,
    schedule_interval=schedule,
    tags=["search-engine"],
    catchup=False,
    max_active_runs=1
)

def create_remote_processing_task_group(input_files, output_files, bash_command, task_name,
                                        is_hdfs_connection_available=hdfs_connection_available_to_remote,
                                        remote_server="jioapp@10.172.148.69", skip_task="false"):
    with TaskGroup(f"RemoteProcessing.{task_name}", dag=Dag) as tg:
        if remote_server == "jioapp@10.166.181.219":
            ssh_cmd="ssh -p 8509"
        else:
            ssh_cmd="ssh"

        if is_hdfs_connection_available:
            _ = SSHOperator(
                task_id=f"Execute.{task_name}",
                ssh_conn_id="DL2_CONN_SSH_SERVER",
                command=f"""{skip_task} || {ssh_cmd} {remote_server} \"{bash_command}\" """,
                dag=Dag
            )
        else:
            _transfer_data_to_local = SSHOperator(
                task_id="TransferInputToLocal",
                ssh_conn_id="DL2_CONN_SSH_SERVER",
                command=f"{skip_task} || bash /mysqldata/searchengine/scripts/transfer_hdfs_data.sh {remote_server} " + " ".join(
                    map(lambda file: f"{file}:{remote_base_dir + file}", input_files)),
                dag=Dag
            )

            _run_task = SSHOperator(
                task_id=f"Execute.{task_name}",
                ssh_conn_id="DL2_CONN_SSH_SERVER",
                command=f"""{skip_task} || {ssh_cmd} {remote_server} \"{bash_command}\" """,
                dag=Dag
            )

            _transfer_output_to_hdfs = SSHOperator(
                task_id="TransferOutputToHdfs",
                ssh_conn_id="DL2_CONN_SSH_SERVER",
                command=f"{skip_task} || bash /mysqldata/searchengine/scripts/transfer_to_hdfs.sh {remote_server} " + " ".join(
                    map(lambda file: f"{remote_base_dir + file}:{file}", output_files)),
                dag=Dag
            )
            _transfer_data_to_local >> _run_task >> _transfer_output_to_hdfs
    return tg

with TaskGroup("EmbeddingTasks", dag=Dag) as embedding_tasks:
    _generate_query_embeddings = create_remote_processing_task_group(
        input_files=[conf.get("rwc_spell_corrected_output_path")],
        output_files=[conf.get("rwc_embeddings_output_path")],
        bash_command=textwrap.dedent(f"""
          {source_env_cmd} && python {"/app/notebooks/genai_automation_scripts/ajio/generate_query_embeddings.py"} \
              --model_path {embedding_model_path} \
              --descriptions_path {conf.get("rwc_spell_corrected_output_path")} \
              --output_path_embeddings {conf.get("rwc_embeddings_output_path")} \
              --product_desc_col {"corrected_query"} \
              --model_name {embedding_model_name} \
              --batch_size {batch_size} \
              --step_size {step_size} \
              --multi_gpu="True" \
              {"--use_hdfs_connection" if hdfs_connection_available_to_remote else ""} \
              --local_base_dir {remote_base_dir} \
              --num_queries {conf.get("rwc_num_queries_to_generate_embeddings")}
        """),
        task_name="GenerateQueryEmbeddings",
        skip_task=opposite(conf.get("rwc_tasks_run_flags", {}).get("query_embeddings", "false"))
    )

    _generate_query_embeddings_cache = create_remote_processing_task_group(
        input_files=[conf.get("rwc_embeddings_output_path")],
        output_files=[conf.get("rwc_embeddings_cache_output_path")],
        bash_command=textwrap.dedent(f"""
          {source_env_cmd} && python {"/app/notebooks/genai_automation_scripts/ajio/generate_query_embeddings_cache.py"} \
              --query_embeddings_path {conf.get("rwc_embeddings_output_path")} \
              --cache_output_path {conf.get("rwc_embeddings_cache_output_path")} \
              {"--use_hdfs_connection" if hdfs_connection_available_to_remote else ""} \
              --local_base_dir {remote_base_dir} \
              --query_cols {"corrected_query"} \
              --vector_column {"vector"}
        """),
        task_name="GenerateQueryEmbeddingsCache",
        skip_task=opposite(conf.get("rwc_tasks_run_flags", {}).get("query_embeddings", "false"))
    )
    _generate_query_embeddings_cache.set_upstream([_generate_query_embeddings])
