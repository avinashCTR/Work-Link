import pprint
from airflow import DAG

from airflow.contrib.operators.ssh_operator import SSHOperator
import textwrap

# from airflow.operators import CoutureSparkOperator, CouturePythonDockerOperator, DagOperator
from CoutureSpark3Plugin import CoutureSpark3Operator as CoutureSparkOperator
from CouturePythonDockerPlugin import CouturePythonDockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

pp = pprint.PrettyPrinter(indent=4)

code_artifact="couture-search-pipelines-2.0.0-avinash-jiomart.jar"
classPath = 'ai.couture.obelisk.search.MainClass'



dirPathMain="/data1/archive/avinash/"
dirPathHistory=f"{dirPathMain}HistoryDataTest/"
dirPathGAData=f"{dirPathMain}GA_DATA_UPLOAD/"

date = datetime(2025, 1, 1)

default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': date,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    "config_group": "config_group_jiomart_large"
}

Dag = DAG('search_engine_history_transformations_jiomart_test', default_args=default_args, schedule_interval=None, tags=["search-engine"])

bash_command = textwrap.dedent(
        f"""
        bash /home/couture/scripts/send_ga_data_to_hdfs.sh \
		--save_path "{dirPathGAData}"
        """
)

t1 = SSHOperator(
    task_id="GA_DATA_UPLOAD",
    ssh_conn_id="NEW_123_CONN_SSH_SERVER",
    command=f"""{bash_command}""",
    dag=Dag,
    # description="This task will transfer the data we get, From New123 server to our HDFS using a bash script . This script also using regex uploads data of particular type into particular type's directory"
)

PreProcessHistory = CoutureSparkOperator(
    task_id='PreProcessHistory',
    class_path=classPath,
    method_id="PreProcessHistoryData",
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column":"search_term",
        "freq_column":"product_unique_list_view",
        "counts_coulumn":"product_unique_list_clicks",
        "product_id_column":"sku",
        "vertical_column":"product_vertical"
    },
    input_base_dir_path=f"{dirPathGAData}{datetime.now().strftime('%Y-%m-%d')}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"query_product_interactions":"query_product_interactions",
    "query_position_data":"query_position"},
    output_filenames_dict={"history_queries_data":"HistoryMerged"},
    dag=Dag,
    description='Merges the two dataframes , query_product_interactions and query_position and get all the required columns for next tasks to consume'
)

PreProcessHistory.set_upstream([t1])

NormalizeQueryHistoryData = CoutureSparkOperator(
    task_id='NormalizeQueryHistory',
    method_id='NormalizeQueryHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column":"search_term",
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"search_term_data":f"HistoryMerged/historyMerged_{datetime.now().strftime('%Y-%m-%d')}"},
    output_filenames_dict={"history_normalized_data":"HistoryNormalized"},
    dag=Dag,
    description='This task generated query_normalized which is array of normalized tokens from query string column'
)

NormalizeQueryHistoryData.set_upstream([PreProcessHistory])

GetShortQueries = CoutureSparkOperator(
    task_id='GetShortQueries',
    method_id='GetShortQueriesHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "query",
                      "max_query_length": 3,
                      "query_split_column":"query_normalised"
                      },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"history_data_grouped":f"HistoryNormalized/historyNormalizedData_{datetime.now().strftime('%Y-%m-%d')}"},
    output_filenames_dict={"short_queries": "HistoryNormalizedShortQueries",
                           "short_queries_archive":"HistoryNormalizedShortQueries/archive"
                          },
    dag=Dag,
    description='This tasks extracts the short queries from the query_normalized array which are less than some length of array'
)

GetShortQueries.set_upstream([NormalizeQueryHistoryData])

GroupSearchQueries = CoutureSparkOperator(
    task_id='GroupSearchQueries',
    method_id='GroupSearchQueriesHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                      "product_id_column": "sku",
                      "query_clicks_column": "clicks"},
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistory,
  	input_filenames_dict={"search_term_data":f"HistoryNormalized/historyNormalizedData_{datetime.now().strftime('%Y-%m-%d')}"},
  
    output_filenames_dict={"queries_with_frequencies": "QueriesWithFrequencies",
						   "query_impressions_with_frequencies": "QueryImpressionsWithFrequencies",
                           "query_clicks_with_frequencies": "QueryClicksWithFrequencies"},
    dag=Dag,
    description='This task takes the normalized query and creates three dataframes based on frequency,impressions,clicks which are required in further tasks'
)
GroupSearchQueries.set_upstream([NormalizeQueryHistoryData])

AccumulateHistoryData = CoutureSparkOperator(
    task_id='AccumulateHistoryData',
    method_id='AccumulateJiomartHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                      "max_query_length": 3,
                      "min_category_percentile": 0.9733,
                      "min_l1_percentile": 25},
    input_base_dir_path="",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"transpose_catalogue_attributes": "/data1/searchengine/processed/jiomart/05092024/etl/AllVerticalsETL/TransposedCatalogueAttributesWithHierarchy",
                          "history_data": "/data1/archive/avinash/HistoryDataTest/HistoryMerged"},  # "HistoryMergedShortQueries"},
    output_filenames_dict={"queries_with_categories": "QueriesWithCategories",
                           "queries_with_brands": "QueriesWithBrands",
                           "tokens_with_categories": "TokensWithCategories"},
    dag=Dag,
	config_group = "config_group_jiomart_large",
    description=''
)
AccumulateHistoryData.set_upstream([GroupSearchQueries])

ColdCacheQueries = CoutureSparkOperator(
    task_id='ColdCacheQueries',
    method_id='GetColdCacheQueries',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                       "freq_column": "product_unique_list_view",
                       "topk": 100000},
    input_base_dir_path=f"{dirPathGAData}{datetime.now().strftime('%Y-%m-%d')}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"history_data":"query_position"},
    output_filenames_dict={"cold_cache_queries": "ColdCacheQueries"},
    dag=Dag,
    description='This generated the clean queries by applying many transformatiions (see the list of cleaning in code doc string ) '
)

ColdCacheQueries.set_upstream([t1])

############################ DataScience Tasks ################################################################################################

PreProcessDataScienceHistoryData = CoutureSparkOperator(
    task_id='PreProcessDataScienceHistoryData',
    method_id='PreProcessDataScienceHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column":"search_term",
        "freq_column":"product_unique_list_view",
        "counts_coulumn":"product_unique_list_clicks",
        "product_id_column":"sku",
        "vertical_column":"product_vertical",
        "quantity_column":"unique_adds_to_cart"
    },
    input_base_dir_path=f"{dirPathGAData}{datetime.now().strftime('%Y-%m-%d')}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"query_product_interactions":"query_product_interactions",
    "query_position_data":"query_position"},
    output_filenames_dict={"DataSciencePreProcessed": "DataSciencePreProcessed","short_queries_archive":"DataSciencePreProcessed/archive"},
    dag=Dag,
    description='This task merges the query interaction and query positions data and creates a dataframe to create pre-data for the training data creatiion task'
)

PreProcessDataScienceHistoryData.set_upstream([t1])

GenerateRelevanceScore = CoutureSparkOperator(
    task_id='GenerateRelevanceScore',
    method_id='GenerateRelevanceScoreDataScienceHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
                      "ordr_weightage": 0.3,
                      "ctr_weightage": 0.5,
                      "search_term_column":"search_term",
                      "freq_column":"impressions",
                      "counts_column":"clicks",
                      "product_id_column":"sku",
                      "quantity_column":"total_quantity"
                     },
    input_base_dir_path = dirPathHistory,
    output_base_dir_path=dirPathHistory,
	input_filenames_dict={"history_data_grouped": "DataSciencePreProcessed/DataSciencePreProcessed_cummulative"
	},
    output_filenames_dict={"final_df_with_scores": 'FilteredHistoryDataWithScores',
                           "final_df_with_filtered_scores": 'FilteredHistoryDataWithNonZeroScores',
                           },
    config_group = "config_group_jiomart_large",
    dag=Dag,
    description='This task creates training data for model from the pre-processed data based on wieghtage on different history metrics'
)

GenerateRelevanceScore.set_upstream([PreProcessDataScienceHistoryData])
