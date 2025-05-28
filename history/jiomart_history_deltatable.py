import pprint
import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator

# from airflow.operators import CoutureSparkOperator, CouturePythonDockerOperator, DagOperator
from CoutureSpark3Plugin import CoutureSpark3Operator as CoutureSparkOperator
from CouturePythonDockerPlugin import CouturePythonDockerOperator

pp = pprint.PrettyPrinter(indent=4)

code_artifact = "couture-search-2.0.0-avinash-deltaTable.jar"
classPath = 'ai.couture.obelisk.search.MainClass'

################################################################ paths ###########################################################################################

dirPathMain = "/data1/archive/avinash/"
dirPathHistory = f"{dirPathMain}HistoryDataTest/"
dirPathGAData = f"{dirPathMain}GA_DATA_UPLOAD/"
dirPathHistoryAgg = f"{dirPathHistory}aggregated/"

################################################################## dates #########################################################################################
date = datetime(2025, 1, 1)
current_date_str = datetime.now().strftime('%Y-%m-%d')

tomorrow = datetime.now() + timedelta(days=1)
six_months_ago = tomorrow - timedelta(days=182)

range_start = six_months_ago.strftime('%Y%m%d')
range_end = tomorrow.strftime('%Y%m%d')

dynamic_range = f"{range_start},{range_end}"

################################################################## args and config ################################################################################

default_args = {
    'owner': 'couture',
    'depends_on_past': False,
    'start_date': date,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    "config_group": "config_group_jiomart_delta_lake"
}

Dag = DAG('search_engine_history_transformations_jiomart_deltaTable', default_args=default_args, schedule_interval=None, tags=["search-engine"])

################################################################# main dag ########################################################################################

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
)

PreProcessHistory = CoutureSparkOperator(
    task_id='PreProcessHistory',
    class_path=classPath,
    method_id="PreProcessHistoryData",
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column": "search_term",
        "freq_column": "product_unique_list_view",
        "counts_coulumn": "product_unique_list_clicks",
        "product_id_column": "sku",
        "vertical_column": "product_vertical"
    },
    input_base_dir_path=f"{dirPathGAData}{current_date_str}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"query_product_interactions": "query_product_interactions",
                          "query_position_data": "query_position"},
    output_filenames_dict={"history_queries_data": "HistoryMerged"},
    dag=Dag,
    description='Merges the two dataframes, query_product_interactions and query_position and gets all the required columns for next tasks to consume'
)

PreProcessHistory.set_upstream([t1])

NormalizeQueryHistoryData = CoutureSparkOperator(
    task_id='NormalizeQueryHistory',
    method_id='NormalizeQueryHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                      "product_id_column": "sku"
                     },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"search_term_data":"HistoryMerged"},
    output_filenames_dict={"history_normalized_data": "HistoryNormalized",
                           "history_tracker":"Trackers/Guardrails_Tracker"
                          },
    dag=Dag,
    description='Generates query_normalized which is an array of normalized tokens from query string column'
)

NormalizeQueryHistoryData.set_upstream([PreProcessHistory])

GetShortQueries = CoutureSparkOperator(
    task_id='GetShortQueries',
    method_id='GetShortQueriesHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column": "query",
        "max_query_length": 3,
        "query_split_column": "query_normalised"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"history_data_grouped": f"HistoryNormalized",
                           "history_tracker":"Trackers/Guardrails_Tracker"},
    output_filenames_dict={"short_queries": "HistoryNormalizedShortQueries"},
    dag=Dag,
    description='Extracts short queries (less than a defined length) from the normalized query array'
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
    input_filenames_dict={"search_term_data": "HistoryNormalized",
                           "history_tracker":"Trackers/Guardrails_Tracker"},
    output_filenames_dict={"queries_with_frequencies": "QueriesWithFrequencies",
                           "query_impressions_with_frequencies": "QueryImpressionsWithFrequencies",
                           "query_clicks_with_frequencies": "QueryClicksWithFrequencies"},
    dag=Dag,
    description='Creates frequency-based dataframes for impressions and clicks from normalized query data'
)

GroupSearchQueries.set_upstream([NormalizeQueryHistoryData])

ColdCacheQueries = CoutureSparkOperator(
    task_id='ColdCacheQueries',
    method_id='GetColdCacheQueries',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                      "freq_column": "product_unique_list_view",
                      "topk": 100000},
    input_base_dir_path=f"{dirPathGAData}{current_date_str}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"history_data": "query_position"},
    output_filenames_dict={"cold_cache_queries": "ColdCacheQueries"},
    dag=Dag,
    description='Generates cleaned queries applying multiple transformations'
)

ColdCacheQueries.set_upstream([t1])

############################ DataScience Tasks ##################################################################################################

PreProcessDataScienceHistoryData = CoutureSparkOperator(
    task_id='PreProcessDataScienceHistoryData',
    method_id='PreProcessDataScienceHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column": "search_term",
        "freq_column": "product_unique_list_view",
        "counts_coulumn": "product_unique_list_clicks",
        "product_id_column": "sku",
        "vertical_column": "product_vertical",
        "quantity_column": "unique_adds_to_cart"
    },
    input_base_dir_path=f"{dirPathGAData}{current_date_str}/",
    output_base_dir_path=dirPathHistory,
    input_filenames_dict={"query_product_interactions": "query_product_interactions",
                          "query_position_data": "query_position"},
    output_filenames_dict={"DataSciencePreProcessed": "DataSciencePreProcessed"},
    dag=Dag,
    description='Merges query interaction and position data for training data preparation'
)

PreProcessDataScienceHistoryData.set_upstream([t1])

############################ Accumulators for all tasks ############################################################################################
GetShortQueriesAccumulator = CoutureSparkOperator(
    task_id='GetShortQueriesAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"column": "date",
                      "range": dynamic_range,
                      "group_by_columns": "['query_normalised']",
                      "sum_columns":"['freq']"
                     },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "HistoryNormalizedShortQueries"},
    output_filenames_dict={"output_path": "HistoryNormalizedShortQueries"},
    dag=Dag,
    description='Generates cleaned queries applying multiple transformations'
)

GetShortQueriesAccumulator.set_upstream([GetShortQueries])

QueriesWithFrequenciesAccumulator = CoutureSparkOperator(
    task_id='QueriesWithFrequenciesAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['query','query_normalised']",
        "sum_columns": "['freq']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "QueriesWithFrequencies"},
    output_filenames_dict={"output_path": "QueriesWithFrequencies"},
    dag=Dag,
    description='Aggregates QueriesWithFrequencies across time range'
)

QueriesWithFrequenciesAccumulator.set_upstream([GroupSearchQueries])

QueryImpressionsWithFrequenciesAccumulator = CoutureSparkOperator(
    task_id='QueryImpressionsWithFrequenciesAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['query','sku']",
        "sum_columns": "['freq']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "QueryImpressionsWithFrequencies"},
    output_filenames_dict={"output_path": "QueryImpressionsWithFrequencies"},
    dag=Dag,
    description='Aggregates QueryImpressionsWithFrequencies across time range'
)

QueryImpressionsWithFrequenciesAccumulator.set_upstream([GroupSearchQueries])

QueryClicksWithFrequenciesAccumulator = CoutureSparkOperator(
    task_id='QueryClicksWithFrequenciesAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['query','sku']",
        "sum_columns": "['freq']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "QueryClicksWithFrequencies"},
    output_filenames_dict={"output_path": "QueryClicksWithFrequencies"},
    dag=Dag,
    description='Aggregates QueryClicksWithFrequencies across time range'
)

QueryClicksWithFrequenciesAccumulator.set_upstream([GroupSearchQueries])

HistoryNormalizedAccumulator = CoutureSparkOperator(
    task_id='HistoryNormalizedAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['search_term','query_normalised','sku']",
        "sum_columns": "['clicks','freq']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "HistoryNormalized"},
    output_filenames_dict={"output_path": "HistoryNormalized"},
    dag=Dag,
    description='Aggregates HistoryNormalized data over a date range by normalized query'
)

HistoryNormalizedAccumulator.set_upstream([NormalizeQueryHistoryData])

AccumulateHistoryData = CoutureSparkOperator(
    task_id='AccumulateHistoryData',
    method_id='AccumulateJiomartHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "query_normalised",
                      "max_query_length": 3,
                      "min_category_percentile": 0.9733,
                      "min_l1_percentile": 25
                     },
    input_base_dir_path="",
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"transpose_catalogue_attributes": "/data1/searchengine/processed/jiomart/05092024/etl_backup/UnifiedFinalDFJioMart2/TransposedCatalogueAttributesWithHierarchy",
                          "history_data": f"{dirPathHistoryAgg}HistoryNormalized"},
    output_filenames_dict={"queries_with_categories": "QueriesWithCategories",
                           "queries_with_brands": "QueriesWithBrands",
                           "tokens_with_categories": "TokensWithCategories"},
    dag=Dag,
    description=''
)

AccumulateHistoryData.set_upstream([HistoryNormalizedAccumulator])

ColdCacheQueriesAccumulator = CoutureSparkOperator(
    task_id='ColdCacheQueriesAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['query']",
        "sum_columns": "['freq']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "ColdCacheQueries"},
    output_filenames_dict={"output_path": "ColdCacheQueries"},
    dag=Dag,
    description='Aggregates ColdCacheQueries over a date range by search term'
)

ColdCacheQueriesAccumulator.set_upstream([ColdCacheQueries])

DataSciencePreProcessedAccumulator = CoutureSparkOperator(
    task_id='DataSciencePreProcessedAccumulator',
    method_id='DeltaTableAggregator',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "column": "date",
        "range": dynamic_range,
        "group_by_columns": "['search_term','sku']",
        "sum_columns": "['impressions', 'clicks', 'total_quantity']"
    },
    input_base_dir_path=dirPathHistory,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"input_path": "DataSciencePreProcessed"},
    output_filenames_dict={"output_path": "DataSciencePreProcessed"},
    dag=Dag,
    description='Aggregates DataSciencePreProcessed data over a date range'
)

DataSciencePreProcessedAccumulator.set_upstream([PreProcessDataScienceHistoryData])

GenerateRelevanceScore = CoutureSparkOperator(
    task_id='GenerateRelevanceScore',
    method_id='GenerateRelevanceScoreDataScienceHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "ordr_weightage": 0.3,
        "ctr_weightage": 0.5,
        "search_term_column": "search_term",
        "freq_column": "impressions",
        "counts_column": "clicks",
        "product_id_column": "sku",
        "quantity_column": "total_quantity"
    },
    input_base_dir_path=dirPathHistoryAgg,
    output_base_dir_path=dirPathHistoryAgg,
    input_filenames_dict={"history_data_grouped": "DataSciencePreProcessed"},
    output_filenames_dict={"final_df_with_scores": 'FilteredHistoryDataWithScores',
                           "final_df_with_filtered_scores": 'FilteredHistoryDataWithNonZeroScores'},
    config_group="config_group_jiomart_large",
    dag=Dag,
    description='Creates training data using weighted metrics from pre-processed data'
)

GenerateRelevanceScore.set_upstream([DataSciencePreProcessedAccumulator])
