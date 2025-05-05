# flake8: noqa
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from CoutureSparkPlugin import CoutureSparkOperator
from CouturePythonDockerPlugin import CouturePythonDockerOperator
from CouturePySparkPlugin import CouturePySparkOperator
from airflow.utils.task_group import TaskGroup
from DagOperatorPlugin import DagOperator
from airflow.models import Variable

kerberos_hosts = Variable.get("kerberos_hosts", deserialize_json=True)
classPath = 'ai.couture.obelisk.search.MainClass'
code_artifact = "couture-search-pipelines-2.0.0-archita.jar"
code_artifact_python = '__main__search_analysis_jiomart.py'

# python_egg = "couture_search-2.0.0-py3.12.egg"
# python_egg = "couture_search-2.0.0-jiomart-py3.10.egg"
#python_egg = "couture_search-2.0.0-py3.10.egg"
python_egg = "couture_search-2.0.3-py3.13.egg"
python_egg_avinash = "avinash_couture_search-2.0.7-py3.9.egg"

# python_egg = "couture_search-2.0.3-py3.13.egg"
commons_egg = "obelisk_retail_commons-1.0.1-py3.9.egg"  # This is from the obelisk-commons repo
python_image = "couture/python-search-image:1.0.7"  # 'couture/jupyter-server-spawner:python-docker-operator-latest'
dirPathOutput = '/data1/searchengine/'
dirPathRawData = '/data1/searchengine/rawdata/'
dirPathStaticData = '/data1/searchengine/staticdata/jiomart/'
dirPathAjio = "/data1/searchengine/processed/jiomart/"
dirSearchHistoryProcessed = '/data1/searchengine/processedHistory/jiomart/'
dirPathAnalysisOutput = dirPathOutput + "analysis/jiomart/"
searchEngineVolume = 'searchEngineVolume:/home/pythondockervolume:Z'

dirGroundTruth = "/data1/searchengine/groundTruthData/"
catalogue_month = "05092024"
catalogue_date = datetime.strptime("05092024", "%d%m%Y")#15102024
str_date = ""
catalogue_date_old = datetime.strptime("05092024", "%d%m%Y")  # 22122023
oldRunDate = "run_20240530" # "run_20240314"

if len(str_date) == 0:
  str_date = datetime.now().strftime('%Y%m%d')

phrasesFinalDataFrame = "analysis/PhrasesAnalysisJSON"

# query_count_all = 50000
query_count_all = 10000
query_count_short = 2000

month = "Latest_Schema_Sample"
zsr = "zsr_27_9_2021"
top_n_month ="Latest_Schema_Sample"
catalogue_new = catalogue_date  # For quality checks
# version = "V4_delta_changes"
version = "V6_delta_changes"
history_date = "1 Nov 2022 - 30 Apr 2023"
history_months_spread_name = "Latest_Schema_Sample"
dirPathFinalOutput = dirPathAnalysisOutput + "deltacatalogue_" + catalogue_date.strftime('%Y%m%d') + "/"  # Changed to keep prod and delta separate in terms of summary and "run_" folders.
# dirPathFinalOutputOld = dirPathAnalysisOutput + f"deltacatalogue_{catalogue_date_old.strftime('%Y%m%d')}/"
dirPathFinalOutputOld = dirPathAnalysisOutput + f"catalogue_{catalogue_date_old.strftime('%Y%m%d')}/"
dirPathProcessedAllVerticals = dirPathAjio + catalogue_date.strftime('%d%m%Y') + f'/{version}/AllVerticals/'
dirPathProcessed = dirPathAjio + catalogue_date.strftime('%d%m%Y') + f'/{version}/'

# vertical = "Groceries"
# vertical = "Beauty"
# vertical = "Fashion"
# vertical = "Electronics"
# vertical = "Home & Lifestyle"
vertical = "OtherVerticals"

dirPathProcessedOld = dirPathAjio + catalogue_date.strftime('%d%m%Y') +'/V1_delta_changes'
dirCummulativePath = f"{dirPathAjio}accumulateddata/"
dirPathAnalysisLatestRun = f"{dirPathFinalOutput}/run_{str_date}/"
dirPathQualityChecks = f"{dirPathAnalysisLatestRun}QualityChecks"
dirPathETL = f"{dirPathAjio}{catalogue_date.strftime('%d%m%Y')}/etl/"
dirPathProcessedETL = f"{dirPathAjio}{catalogue_date.strftime('%d%m%Y')}/etl/"

schedule_args = {
    'owner': 'couture',
  	'config_group': 'config_group_jiomart_large',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2019, 4, 15),
    'retries': 0,
}

schedule = None
dag = DAG('search_engine_analysis_jiomart', default_args=schedule_args, schedule_interval=schedule, tags=["search-engine"])

# ==================== #
# ======= LOAD ======= #
# ==================== #

# ======== LEVEL 0 ======== #

CheckFilePresence = CoutureSparkOperator(
    task_id='CheckFilePresence',
    method_id='CheckFilePresence',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"file_paths": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/1_data.csv"},
    dag=dag,
    description=''
)

CheckFilePresenceZSR = CoutureSparkOperator(
    task_id='CheckFilePresenceZSR',
    method_id='CheckFilePresence',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"file_paths": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_data.csv"},
    dag=dag,
    description=''
)

CheckFilePresenceNumBrands = CoutureSparkOperator(
    task_id='CheckFilePresenceNumBrands',
    method_id='CheckFilePresence',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"file_paths": dirPathFinalOutput+"numBrands.csv"},
    dag=dag,
    description=''
)


# ======== LEVEL 1 ======== #

IdentifyNumericalBrands = CouturePythonDockerOperator(
    task_id='IdentifyNumericalBrands',
    trigger_rule='one_failed',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    extra_hosts=kerberos_hosts,
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='identify_numerical_brands',
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathETL,
    output_base_dir_path=dirPathFinalOutput,
    input_filenames_dict={"brands": "Brands"},
    output_filenames_dict={"num_brands_out":"numBrands.csv"},
    description=''
)
IdentifyNumericalBrands.set_upstream([CheckFilePresenceNumBrands])

AnalyseTokenCategoryPerformance = CoutureSparkOperator(
    task_id='AnalyseTokenCategoryPerformance',
    method_id='AnalyseTokenCategoryPerformance',
    trigger_rule='none_skipped',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"test_categories_column": "l1l3category_en_string_mv"},
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"word_analysis_df": dirPathProcessed + "analysis/Wrong2RightWordsAnalysisDF",
                          # "phrases": dirPathProcessed + "PhrasesUpdated",
                          # "phrases": dirPathProcessed + "PhrasesWithCorrections",
                          "phrases": dirPathProcessed + phrasesFinalDataFrame,
                          "phrases_before_category_filtering": dirPathProcessed + "PhrasesFiltered",
                          "right_words": dirPathProcessed + "RightwordsCombined",
                          "right_words_filtered": dirPathProcessed + "Rightwords",
                          "history_words_categories_interaction": dirSearchHistoryProcessed + "TokensWithCategories",
                          "stopwords": dirPathStaticData + "StopWords"},
    output_filenames_dict={"tokens_category_performance": dirPathAnalysisOutput+"Archive/tokens_category_performance",
                           "tokens_category_performance_json": dirPathFinalOutput+"run_"+str_date+"/tokens_category_performance_json",
                           "tokens_category_performance_csv": dirPathFinalOutput+"run_"+str_date+"/tokens_category_performance_only_numbers_csv",
                           "all_tokens_history_added": dirPathProcessed + "AllTokensHistoryAdded"},
    dag=dag,
    description=''
)
AnalyseTokenCategoryPerformance.set_upstream([CheckFilePresence])

AnalyseW2RMapping = CoutureSparkOperator(
    task_id='AnalyseW2RMapping',
    method_id='AnalyseW2RMapping',
    class_path=classPath,
    trigger_rule='none_skipped',
    code_artifact=code_artifact,
    method_args_dict={"min_token_count": 100,
                      "max_query_length": 3,
                      "analysis": "true"},
    input_base_dir_path="",
    output_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    input_filenames_dict={"transpose_product_attributes": dirPathProcessed + "ProductAttributesTransposeExploded",
                          "history_data": f"{dirSearchHistoryProcessed}QueryClicksWithFrequencies",
                          "wrong_to_right_mappings": dirPathProcessed + "analysis/Wrong2RightWordsAnalysisDF",
                          "stopwords": dirPathStaticData + "StopWords"},
    output_filenames_dict={"w2r_mapping_precision": "wrong_2_right_precision",
                           "w2r_mapping_precision_only_category": "wrong_2_right_precision_category_only"},
    dag=dag,
    description=''
)
AnalyseW2RMapping.set_upstream([CheckFilePresence])

CleanData = CouturePythonDockerOperator(
    task_id='CleanData',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    trigger_rule='one_failed',
    extra_hosts=kerberos_hosts,
    dag=dag,
    mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='initial_analysis',
    method_args_dict={"month": month,
                      "query_column": "query"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirSearchHistoryProcessed,
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    input_filenames_dict={"month_data_path": "sample/QueriesWithFrequencies"},
    output_filenames_dict={"data_out": "0_data.csv", "data_dashboard_out": "0_data_ajio.csv",
                           "month_data_out": "0_month.csv", "month_word_data_out": "0_month_word.csv"},
    description=''
)
CleanData.set_upstream([CheckFilePresence])

GenerateHistoryCategories1 = CoutureSparkOperator(
    task_id='GenerateHistoryCategories1TokenQueries',
    method_id='GenerateHistoryCategoriesTopNQueries',
    class_path=classPath,
    code_artifact=code_artifact,
    trigger_rule='one_failed',
    method_args_dict={"top_n": query_count_short,
                      "query_length": 1,
                      "query_col": "search_term"},
    input_base_dir_path="",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/",
    input_filenames_dict={"transpose_catalog_attributes": dirPathProcessed + "TransposedCatalogueAttributesWithHierarchy",
                          "history_data": dirSearchHistoryProcessed + "QueriesNormalised"},
    output_filenames_dict={"top_n_history_with_categories": "history_categories_top_"+str(query_count_short)+"_queries_1_token"},
    dag=dag,
    description=''
)
GenerateHistoryCategories1.set_upstream([CheckFilePresence])

GenerateHistoryCategories2 = CoutureSparkOperator(
    task_id='GenerateHistoryCategories2TokenQueries',
    method_id='GenerateHistoryCategoriesTopNQueries',
    class_path=classPath,
    code_artifact=code_artifact,
    trigger_rule='one_failed',
    method_args_dict={"top_n": query_count_short,
                      "query_length": 2,
                      "query_col": "search_term"},
    input_base_dir_path="",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/",
    input_filenames_dict={"transpose_catalog_attributes": dirPathProcessed + "TransposedCatalogueAttributesWithHierarchy",
                          "history_data": dirSearchHistoryProcessed + "QueriesNormalised"},
    output_filenames_dict={"top_n_history_with_categories": "history_categories_top_"+str(query_count_short)+"_queries_2_token"},
    dag=dag,
    description=''
)
GenerateHistoryCategories2.set_upstream([CheckFilePresence])

GenerateJSON = CouturePythonDockerOperator(
    task_id='GenerateJSON',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    trigger_rule='none_skipped',
    extra_hosts=kerberos_hosts,
    user='couture',
    mem_limit="24g",
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='generate_json',
    method_args_dict={"catalogue_date": catalogue_date.strftime('%d %b %Y'),
                      "history_date": history_date},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+vertical,
    output_base_dir_path=dirPathProcessed+vertical + "/AnalysisCorpusJSON/",
    input_filenames_dict={"history_path": dirSearchHistoryProcessed,
                          "wrong_2_right_path": f"{dirPathProcessed+vertical}/analysis/Wrong2RightWordsAnalysisDF",
                          "w2r": f"{dirPathProcessed+vertical}/analysis/W2RErr",
                          "r2cats": f"{dirPathProcessed+vertical}/analysis/R2Cats",
                          "r2otherprops": f"{dirPathProcessed+vertical}/analysis/R2OtherProps",
                          # "phrases_path": "PhrasesUpdated",
                          # "phrases_path": "PhrasesWithCorrections",
                          "phrases_path": f"{dirPathProcessed+vertical}/{phrasesFinalDataFrame}",
                          "numerical_path": f"{dirPathProcessed}/AllVerticals/NumericalEntitiesRangeWise",
                          "entity_path": f"{dirPathProcessed+vertical}/AnalysisCorpusJSON/EntityCorpusJSONInternal"},
    output_filenames_dict={"r2e_out": "r2e.json",
                           "error_type_out": "w2error.json",
                           "w2r_out": "w2r.json",
                           "r2l1l2_out": "r2l1l2.json",
                           "r2l1l3_out": "r2l1l3.json",
                           "r2_token_type_out": "r2tokentype.json",
                           "r2_isCategory_out": "r2iscategory.json",
                           "r2syn_out": "r2syn.json",
                           "phrases_out": "phrases.json",
                           "numerical_ranges_out": "numericalranges.json",
                           "word_entities_out": "wordentities.json",
                           "r2sub_out":"r2sub.json",
                           "summary_out": dirPathFinalOutput+"run_"+str_date+"/summary.txt"},
    description=''
)
GenerateJSON.set_upstream([CheckFilePresence])

with TaskGroup("TestingManuallyCreatedIssueQueries", dag=dag) as TestingManuallyCreatedIssueQueries:
    dirIssueSheet = "/data1/searchengine/testing/jiomart/static_data/jiomart_NDCG_Testing_pipeline - ndcg_queries.csv"

    TopNStepWiseOutputIssueQueries = CouturePythonDockerOperator(
        task_id="TopNStepWiseOutputIssueQueries",
        image=python_image,
        api_version="auto",
        auto_remove=False,
        command="/bin/bash echo 'heloword'",
        extra_hosts=kerberos_hosts,
        user="couture",
        dag=dag,
        mem_limit="18g",
        code_artifact=code_artifact_python,
        python_deps=[
            commons_egg,
            python_egg,
        ],
        method_id="top_nstep_wise_output",
        method_args_dict={
            "n": 10**5,
            "query_column": "Query",
            "json_dump": True,
            "json_dump_additional_columns": [
                "normalize_text",
                "spell_check",
                "match_phrases",
            ],
        },
        volumes=[searchEngineVolume],
        # input_base_dir_path=dirPathProcessed + "AnalysisCorpusJSON/",
        input_base_dir_path="/data1/searchengine/processed/ajio/"
                            + catalogue_month + "/" + version
                            + "/"
                            + "AnalysisCorpusJSON/",
        output_base_dir_path=dirPathFinalOutput,
        input_filenames_dict={
            "all_queries_path": dirIssueSheet,
            "r2syn_path": "r2syn.json",
            "r2sub_path": "r2sub.json",
            "w2r_path": "w2r.json",
            "r2e_path": "r2e.json",
            "r2l1l2_path": "r2l1l2.json",
            "r2l1l3_path": "r2l1l3.json",
            "r2_token_type_path": "r2tokentype.json",
            "r2_isCategory_path": "r2iscategory.json",
            "phrase_corpus_path": "phrases.json",
            "num_corpus_path": "numericalranges.json",
            "entity_corpus_path": "wordentities.json",
            "numerical_data": "/data1/searchengine/archive/meghana/Phase2/GuardRails/SmallSampleNumQueriesCSV/part-00000-fd4e7be9-f2fe-4060-98c1-c00a549838d1-c000.csv",

        },
        output_filenames_dict={"queries_intent_json": "QueriesIntentJsonIssueQueries"},
        description="",
    )

    IssueTestingScorer = CouturePythonDockerOperator(
        task_id="IssueTestingScorer",
        image=python_image,
        api_version="auto",
        auto_remove=False,
        command="/bin/bash echo 'heloword'",
        extra_hosts=kerberos_hosts,
        user="couture",
        dag=dag,
        mem_limit="24g",
        code_artifact=code_artifact_python,
        python_deps=[
            commons_egg,
            python_egg,
        ],
        method_id="issue_testing_scorer",
        method_args_dict={},
        volumes=[searchEngineVolume],
        input_base_dir_path=dirPathFinalOutput,
        output_base_dir_path=dirPathFinalOutput,
        input_filenames_dict={
            "issue_sheet": dirIssueSheet,
            "query_intent_json": "QueriesIntentJsonIssueQueries",
            "filter_word_index": dirPathProcessedETL + "CatalogueWordIndexes/filterWordIndex.pkl",
            "open_token_index": dirPathProcessedETL + "CatalogueWordIndexes/open_token_index.pkl",
        },
        output_filenames_dict={
            "IssueQueriesScored": "IssueQueriesScored",
            "IssueQueriesScoredDelta": "IssueQueriesScoredDelta",
        },
        description="",
    )
    IssueTestingScorer.set_upstream(
        [TopNStepWiseOutputIssueQueries]
    )

TestingManuallyCreatedIssueQueries.set_upstream([GenerateJSON])
 
# -------------------

dirPathTestOutput = "/data1/searchengine/testing/" + catalogue_date.strftime('%d%m%Y') +'/V1/'
# UpdateFrontendSimulator = SSHOperator(
#     ssh_conn_id="AIRFLOW_CONN_SSH_SERVER",  # To connect to DBS
#     task_id="UpdateFrontendSimulator",
#     command="""ssh HD4 "bash /data1/search-simulator/update_frontend_simulator.sh """ +
#   			dirPathProcessed+"AnalysisCorpusJSON/ " +
#   			dirPathProcessed+"CatalogueAttributes/ " +
#   			dirPathTestOutput + "CatalogueWordIndexes/ " +
#   			""" " """,
#     # Runs these commands on the SSH"ed server (i.e. HD4)
#     # description="Update frontend simulator http://10.144.97.22:8491",
#     dag=dag
# )
# UpdateFrontendSimulator.set_upstream([GenerateJSON])

# -------------------

PreProcessZsrData = CoutureSparkOperator(
    task_id='PreProcessZsrData',
    dag=dag,
    trigger_rule='one_failed',
    code_artifact=code_artifact,
    class_path=classPath,
    method_id='PreProcessZsrData',
    method_args_dict={},
    input_base_dir_path=dirPathRawData,
    output_base_dir_path=dirSearchHistoryProcessed,
    input_filenames_dict={"zsr_csv_path": "zsrdata/zsr_sep18.csv"},
    output_filenames_dict={"zsr_parquet_path": "SEARCH_TERM_ZSR"},
    description=''
)
PreProcessZsrData.set_upstream([CheckFilePresenceZSR])


# ======== LEVEL 2 ======== #

CleanDataZSR = CouturePythonDockerOperator(
    task_id='CleanDataZSR',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    extra_hosts=kerberos_hosts,
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='initial_analysis',
    method_args_dict={"month": zsr,
                      "query_column": "search_term"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirSearchHistoryProcessed,
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/",
    input_filenames_dict={"month_data_path": "SEARCH_TERM_ZSR"},
    output_filenames_dict={"data_out": "0_data.csv", "data_dashboard_out": "0_data_ajio.csv",
                           "month_data_out": "0_month.csv", "month_word_data_out": "0_month_word.csv"},
    description=''
)
CleanDataZSR.set_upstream([PreProcessZsrData])

Normalize = CouturePythonDockerOperator(
    task_id='normalize',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='performance_normalisation',
    method_args_dict={"month": month},
    extra_hosts=kerberos_hosts,
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    input_filenames_dict={"data_path": "0_data.csv", "data_dashboard_path": "0_data_ajio.csv",
                          "month_data_path": "0_month.csv", "month_word_data_path": "0_month_word.csv"},
    output_filenames_dict={"data_out": "1_data.csv", "data_dashboard_out": "1_data_ajio.csv",
                           "month_data_out": "1_month.csv", "month_word_data_out": "1_month_word.csv",
                           "word_query_out": "word_query"},
    description=''
)
Normalize.set_upstream([CleanData])

TopNHistoryCoverage1 = CouturePythonDockerOperator(
    task_id='TopNHistoryCoverage1TokenQueries',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    trigger_rule='none_failed',
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_n_history_coverage',
    method_args_dict={"task_id": 'top_n_history_coverage_1_token_queries',
                      "history_freq_col": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON/",
    output_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    input_filenames_dict={"top_n_history_with_categories": dirPathAnalysisOutput+"Archive/history_categories_top_"+str(query_count_short)+"_queries_1_token",
                          "r2syn_path": "r2syn.json",
                          "r2sub_path":"r2sub.json",
                          "w2r_path": "w2r.json", "r2e_path": "r2e.json",
                          "r2l1l2_path": "r2l1l2.json", "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json", "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json", "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"query_data_out": "history_categories_top_"+str(query_count_short)+"_queries_1_token_coverage.csv"},
    description=''
)
TopNHistoryCoverage1.set_upstream([GenerateHistoryCategories1, GenerateJSON])

TopNHistoryCoverage2 = CouturePythonDockerOperator(
    task_id='TopNHistoryCoverage2TokenQueries',
  
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    trigger_rule='none_failed',
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_n_history_coverage',
    method_args_dict={"task_id": 'top_n_history_coverage_2_token_queries',
                      "history_freq_col": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON/",
    output_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    input_filenames_dict={"top_n_history_with_categories": dirPathAnalysisOutput+"Archive/history_categories_top_"+str(query_count_short)+"_queries_2_token",
                          "r2syn_path": "r2syn.json",
                          "w2r_path": "w2r.json", "r2e_path": "r2e.json",
                          "r2sub_path":"r2sub.json",
                          "r2l1l2_path": "r2l1l2.json", "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json", "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json", "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"query_data_out": "history_categories_top_"+str(query_count_short)+"_queries_2_token_coverage.csv"},
    description=''
)
TopNHistoryCoverage2.set_upstream([GenerateHistoryCategories2, GenerateJSON])

TopNStepWiseOutput = CouturePythonDockerOperator(
    task_id='TopNStepWiseOutput',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg_avinash],
    method_id='top_nstep_wise_output',
    method_args_dict={"n": query_count_all,
                      "query_column": "ProductInfo",
                      "verticals_list":[ "Groceries", "Beauty", "Electronics", "Home & Lifestyle", "OtherVerticals", "Fashion"]},  # "search_term"},
    volumes=[searchEngineVolume],
    input_base_dir_path="",#dirPathProcessed + "AnalysisCorpusJSON/",
    output_base_dir_path="",
    input_filenames_dict={
      					  # "all_queries_path": f"{dirSearchHistoryProcessed}/sample/QueriesWithFrequencies",
      					  #"all_queries_path": "/data1/searchengine/testing/jiomart/static_data/topn_count_jiomart_historical_Testing_pipeline",
      					  "all_queries_path": "/data1/searchengine/testing/jiomart/static_data/topn_count_jiomart_NDCG_Testing_pipeline_gt.csv",
      					  # "/data1/searchengine/archive/meghana/NonAjiofashionDict/macmillanDictionary/topnIP",
                          # "/data1/searchengine/archive/meghana/topnmyntra",
                          # "/data1/searchengine/processedHistory/ajio/search_"+top_n_month,
                          # "/data1/archive/meghana/topninput/searchqueries_1_10dec.csv",
                          # "/data1/searchengine/rawdata/AjioExperimentData/Search queries - 20th Sep-4th Oct - results-20221005-220847.csv",
                          # "/data1/archive/data/output - Sheet1.csv",
                          # "/data1/searchengine/processedHistory/ajio/rawHistoryDataParquetFiles/20220810_20220920",
      					  # "base_path": dirPathProcessed,
                          "base_path":"/data1/archive/avinash/unifyJiomartVerticals"
                          "r2syn_path": "/AnalysisCorpusJSON/r2syn.json",
                          "r2sub_path": "/AnalysisCorpusJSON/r2sub.json",
                          "w2r_path": "/AnalysisCorpusJSON/w2r.json",
                          "r2e_path": "/AnalysisCorpusJSON/r2e.json",
                          "r2l1l2_path": "/AnalysisCorpusJSON/r2l1l2.json",
                          "r2l1l3_path": "/AnalysisCorpusJSON/r2l1l3.json",
                          "r2_token_type_path": "/AnalysisCorpusJSON/r2tokentype.json",
                          "r2_isCategory_path": "/AnalysisCorpusJSON/r2iscategory.json",
                          "phrase_corpus_path": "/AnalysisCorpusJSON/phrases.json",
                          "num_corpus_path": "/AnalysisCorpusJSON/numericalranges.json",
                          "entity_corpus_path": "/AnalysisCorpusJSON/wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={
      # "month_data_out" : "stepwise.csv",
      #                      "query_wise_intent_out" : "query_wise",
      #                      "query_open_out" : "query_open",
      #                      "query_weighted_out" : "query_weigh",
      #                      "query_hard_out" : "query_hard",
      #                      "topN_step_wise_path" : "topn_parquet"},
      						"month_data_out": dirPathFinalOutput+"run_"+str_date+"/top_"+str(query_count_all)+"_stepwise_output_top3.csv",
      						"query_wise_intent_out": dirPathAnalysisOutput+"Archive/query_wise",
      						"query_open_out": dirPathAnalysisOutput+"Archive/ProductCount/query_open",
      						"query_weighted_out": dirPathAnalysisOutput+"Archive/ProductCount/query_weighted",
      						"query_hard_out": dirPathAnalysisOutput+"Archive/ProductCount/query_hard",
      						"topN_step_wise_path": dirPathFinalOutput+"run_"+str_date+"/topn_parquet"},
    description=''
)
TopNStepWiseOutput.set_upstream([GenerateJSON])

W2rNetPrecision = CouturePythonDockerOperator(
    task_id='W2rNetPrecision',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='generate_net_score',
    method_args_dict={"task_id": 'w2r_netprecision',
                      "score_columns": ["ratio_right", "ratio_synonym"],
                      "count_column": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    output_base_dir_path=dirPathFinalOutput+"AggregatedScores/",
    input_filenames_dict={"metric_file": "wrong_2_right_precision"},
    output_filenames_dict={"net_score_file": "w2r_precision_score.csv"},
    description=''
)
W2rNetPrecision.set_upstream([AnalyseW2RMapping])

W2rNetPrecisionCategorical = CouturePythonDockerOperator(
    task_id='W2rNetPrecisionCategorical',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='generate_net_score',
    method_args_dict={"task_id": 'w2r_netprecision_categorical',
                      "score_columns": ["ratio_right", "ratio_synonym"],
                      "count_column": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    output_base_dir_path=dirPathFinalOutput+"AggregatedScores/",
    input_filenames_dict={"metric_file": "wrong_2_right_precision_category_only"},
    output_filenames_dict={"net_score_file": "w2r_precision_score_categorical.csv"},
    description=''
)
W2rNetPrecisionCategorical.set_upstream([AnalyseW2RMapping])


# ======== LEVEL 3 ======== #

NormalizeZsr = CouturePythonDockerOperator(
    task_id='NormalizeZsr',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='performance_normalisation',
    method_args_dict={"month": zsr},
    extra_hosts=kerberos_hosts,
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/",
    input_filenames_dict={"data_path": "0_data.csv", "data_dashboard_path": "0_data_ajio.csv",
                          "month_data_path": "0_month.csv", "month_word_data_path": "0_month_word.csv"},
    output_filenames_dict={"data_out": "1_data.csv", "data_dashboard_out": "1_data_ajio.csv",
                           "month_data_out": "1_month.csv", "month_word_data_out": "1_month_word.csv",
                           "word_query_out": "word_query"},
    description=''
)
NormalizeZsr.set_upstream([CleanDataZSR])

SpellCorrect = CouturePythonDockerOperator(
    task_id='SpellCorrect',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    trigger_rule='none_failed',
    extra_hosts=kerberos_hosts,
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    # code_artifact="__main__prod.py",
    python_deps=[commons_egg, python_egg],
    method_id='performance_spell_check',
    method_args_dict={"month": month},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    input_filenames_dict={"month_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/1_month.csv",
                          "month_word_data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/1_month_word.csv",
                          "data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/1_data.csv",
                          "data_dashboard_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/1_data_ajio.csv",
                          "w2r_path": "w2r.json",
                          "r2e_path": "r2e.json",
                          "r2syn_path": "r2syn.json",
                          "r2sub_path":"r2sub.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"data_out": "2_data.csv", "data_dashboard_out": "2_data_ajio.csv",
                           "top_tokens_out": "top_tokens.csv", "month_data_out": "2_month.csv",
                           "month_word_data_out": "2_month_word.csv"},
    description=''
)
SpellCorrect.set_upstream([Normalize, GenerateJSON])

TopNHistoryCoverage1NetScore = CouturePythonDockerOperator(
    task_id='TopNHistoryCoverage1TokenQueriesNetScore',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='generate_net_score',
    method_args_dict={"task_id": 'top_n_history_coverage_1_token_queries_net_score',
                      "score_columns": ["history_coverage"],
                      "count_column": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    output_base_dir_path=dirPathFinalOutput+"AggregatedScores/",
    input_filenames_dict={"metric_file": "history_categories_top_"+str(query_count_short)+"_queries_1_token_coverage.csv"},
    output_filenames_dict={"net_score_file": "history_categories_top_"+str(query_count_short)+"_queries_1_token_coverage.csv"},
    description=''
)
TopNHistoryCoverage1NetScore.set_upstream([TopNHistoryCoverage1])

TopNHistoryCoverage2NetScore = CouturePythonDockerOperator(
    task_id='TopNHistoryCoverage2TokenQueriesNetScore',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='generate_net_score',
    method_args_dict={"task_id": 'top_n_history_coverage_2_token_queries_net_score',
                      "score_columns": ["history_coverage"],
                      "count_column": "freq"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    output_base_dir_path=dirPathFinalOutput+"AggregatedScores/",
    input_filenames_dict={"metric_file": "history_categories_top_"+str(query_count_short)+"_queries_2_token_coverage.csv"},
    output_filenames_dict={"net_score_file": "history_categories_top_"+str(query_count_short)+"_queries_2_token_coverage.csv"},
    description=''
)
TopNHistoryCoverage2NetScore.set_upstream([TopNHistoryCoverage2])

TopNUnderScore = CouturePythonDockerOperator(
    task_id='TopNUnderScore',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_n_under_score',
    method_args_dict={"N": query_count_all,
                      "month": top_n_month},
    volumes=[searchEngineVolume],
    input_base_dir_path="",
    output_base_dir_path=dirPathFinalOutput+"AggregatedScores/",
    input_filenames_dict={"month_data_path": dirPathFinalOutput+"run_"+str_date+"/top_"+str(query_count_all)+"_stepwise_output.csv",
                          "under_score_path": dirPathAnalysisOutput+"Archive/tokens_category_performance"},
    output_filenames_dict={"top_n_under_score_path": "top_n_under_score.csv"},
    description=''
)
TopNUnderScore.set_upstream([AnalyseTokenCategoryPerformance, TopNStepWiseOutput])

WordCorpusCoverage = CoutureSparkOperator(
    task_id='WordCorpusCoverage',
    method_id='word_corpus_coverage',
    class_path=classPath,
    code_artifact=code_artifact,
    trigger_rule='none_failed',
    method_args_dict={},
    input_base_dir_path="",
    output_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    input_filenames_dict={"word_query_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/word_query.parquet",
                          "wrong_2_right_analysis_path": dirPathProcessed + "analysis/Wrong2RightWordsAnalysisDF"},
    output_filenames_dict={"word_corpus_coverage_path": "word_corpus_coverage"},
    dag=dag,
  	config_group="config_group_search_engine_testing",
    description=''
)
WordCorpusCoverage.set_upstream([Normalize])


# ======== LEVEL 4 ======== #

PhraseMatch = CouturePythonDockerOperator(
    task_id='PhraseMatch',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    extra_hosts=kerberos_hosts,
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='performance_phrase_match',
    method_args_dict={"month": month},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/",
    input_filenames_dict={"month_data_path": "2_month.csv",
                          "data_path": "2_data.csv",
                          "data_dashboard_path": "2_data_ajio.csv",
                          "top_tokens_path": "top_tokens.csv",
                          "phrase_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/phrases.json"},
    output_filenames_dict={"data_out": "Intermediate/"+month+"/3_data.csv",
                           "data_dashboard_out": dirPathAnalysisOutput+"Dashboard/"+month+".csv",
                           "top_tokens_out": dirPathAnalysisOutput+"Dashboard/top_tokens_"+month+".csv",
                           "month_data_out": "Intermediate/"+month+"/3_month.csv",
                           "month_word_data_out": "Intermediate/"+month+"/3_month_word.csv",
                           "compare_phrases": "compared_phrases_"+month+".csv"},
    description=''
)
PhraseMatch.set_upstream([SpellCorrect])

TopNStepWiseZsrOutput = CouturePythonDockerOperator(
    task_id='TopNStepWiseZsrOutput',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    trigger_rule='none_failed',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_nstep_wise_output',
    method_args_dict={"n": query_count_short,
                      "query_column": "query"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON/",
    output_base_dir_path="",
    input_filenames_dict={"all_queries_path": dirSearchHistoryProcessed+"SEARCH_TERM_ZSR",
                          # dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_month.csv",
                          "r2syn_path": "r2syn.json",
                          "w2r_path": "w2r.json",
                          "r2e_path": "r2e.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2sub_path":"r2sub.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json",
                          "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"month_data_out": dirPathFinalOutput+"run_"+str_date+"/"+zsr+"_top_"+str(query_count_all)+"_stepwise_output_"+".csv",
                           "query_wise_intent_out": dirPathAnalysisOutput+"Archive/zsr_query_wise",
                           "topN_step_wise_path": dirPathAnalysisOutput + "Archive/run_"+str_date+"/"+zsr+"/topn_parquet"},
    description=''
)
TopNStepWiseZsrOutput.set_upstream([GenerateJSON, NormalizeZsr])

SpellCorrectZsr = CouturePythonDockerOperator(
    task_id='SpellCorrectZsr',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    trigger_rule='none_failed',
    extra_hosts=kerberos_hosts,
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='performance_spell_check',
    method_args_dict={"month": zsr},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/",
    input_filenames_dict={"month_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_month.csv",
                          "month_word_data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_month_word.csv",
                          "data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_data.csv",
                          "data_dashboard_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/1_data_ajio.csv",
                          "w2r_path": "w2r.json",
                          "r2e_path": "r2e.json",
                          "r2syn_path": "r2syn.json",
                          "r2sub_path": "r2sub.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json"},
    output_filenames_dict={"data_out": "2_data.csv", "data_dashboard_out": "2_data_ajio.csv",
                           "top_tokens_out": "top_tokens.csv", "month_data_out": "2_month.csv",
                           "month_word_data_out": "2_month_word.csv"},
    description=''
)
SpellCorrectZsr.set_upstream([NormalizeZsr, GenerateJSON])


# ======== LEVEL 5 ======== #

ConvertToNum = CouturePythonDockerOperator(
    task_id='ConvertToNum',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
  	mem_limit="18g",
    trigger_rule="none_failed",
    code_artifact=code_artifact_python,
    extra_hosts=kerberos_hosts,
    python_deps=[commons_egg, python_egg],
    method_id='performance_text_to_num',
    method_args_dict={"month": month},
    volumes=[searchEngineVolume],
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"month_data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/3_month.csv",
                          "num_brands_path": dirPathFinalOutput+"numBrands.csv",
                          "data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/3_data.csv"},
    output_filenames_dict={"data_out": dirPathFinalOutput+"run_"+str_date+"/tokenization.csv",
                           "month_data_out": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/4_month.csv",
                           "month_word_data_out": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/4_month_word.csv"},
    description=''
)
ConvertToNum.set_upstream([PhraseMatch, IdentifyNumericalBrands])

PhraseMatchZsr = CouturePythonDockerOperator(
    task_id='PhraseMatchZsr',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    extra_hosts=kerberos_hosts,
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='performance_phrase_match',
    method_args_dict={"month": zsr},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/",
    input_filenames_dict={"month_data_path": "2_month.csv",
                          "data_path": "2_data.csv",
                          "data_dashboard_path": "2_data_ajio.csv",
                          "top_tokens_path": "top_tokens.csv",
                          "phrase_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/phrases.json"},
    output_filenames_dict={"data_out": "Intermediate/"+zsr+"/3_data.csv",
                           "data_dashboard_out": dirPathAnalysisOutput+"Dashboard/"+zsr+".csv",
                           "top_tokens_out": dirPathAnalysisOutput+"Dashboard/top_tokens_"+zsr+".csv",
                           "month_data_out": "Intermediate/"+zsr+"/3_month.csv",
                           "month_word_data_out": "Intermediate/"+zsr+"/3_month_word.csv",
                           "compare_phrases": "compared_phrases_"+zsr+".csv"},
    description=''
)
PhraseMatchZsr.set_upstream([SpellCorrectZsr])


# ======== LEVEL 6 ======== #

ConvertToNumZsr = CouturePythonDockerOperator(
    task_id='ConvertToNumZsr',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
  	trigger_rule="none_failed",
    code_artifact=code_artifact_python,
    extra_hosts=kerberos_hosts,
    python_deps=[commons_egg, python_egg],
    method_id='performance_text_to_num',
    method_args_dict={"month": zsr},
    volumes=[searchEngineVolume],
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"month_data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/3_month.csv",
                          "num_brands_path": dirPathFinalOutput+"numBrands.csv",
                          "data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/3_data.csv"},
    output_filenames_dict={"data_out": dirPathFinalOutput+"run_"+str_date+"/"+zsr+"_tokenization.csv",
                           "month_data_out": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/4_month.csv",
                           "month_word_data_out": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+zsr+"/4_month_word.csv"},
    description=''
)
ConvertToNumZsr.set_upstream([PhraseMatchZsr, IdentifyNumericalBrands])

RewriteQuery = CouturePythonDockerOperator(
    task_id='RewriteQuery',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
	extra_hosts=kerberos_hosts,
    python_deps=[commons_egg, python_egg],
    method_id='performance_rewrite_query',
    method_args_dict={"month": month},
  	volumes=[searchEngineVolume],
    input_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    input_filenames_dict={"month_data_path": "4_month.csv",
                          "r2e_path": dirPathProcessed+"AnalysisCorpusJSON/r2e.json",
                          "r2_token_type_path": dirPathProcessed+"AnalysisCorpusJSON/r2tokentype.json",
                          "phrase_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/phrases.json"},
    output_filenames_dict={"month_data_out": "5_month.csv"},
    description=''
)
RewriteQuery.set_upstream([ConvertToNum])


# ======== LEVEL 7 ======== #

identify_intent = CouturePythonDockerOperator(
    task_id='identify_intent',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
	extra_hosts=kerberos_hosts,
    python_deps=[commons_egg, python_egg],
    method_id='performance_identify_intent',
    method_args_dict={"month": month},
  	volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed+"AnalysisCorpusJSON",
    output_base_dir_path=dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/",
    input_filenames_dict={"month_data_path": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/5_month.csv",
                          "w2r_path": "w2r.json",
                          "r2syn_path": "r2syn.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2e_path": "r2e.json",
                          "r2sub_path":"r2sub.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json",
                          "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json"},
    output_filenames_dict={"month_data_out": "6_month"},
    description=''
)
identify_intent.set_upstream([RewriteQuery])

GetPercentageInteractionCovered = CoutureSparkOperator(
    task_id='GetPercentageInteractionCovered',
    method_id='GetPercentageInteractionCovered',
    class_path=classPath,
    code_artifact=code_artifact,
    trigger_rule='none_failed',
    method_args_dict={"query_col": "search_term"},
    input_base_dir_path="",
    output_base_dir_path=dirPathFinalOutput+"run_"+str_date+"/",
    input_filenames_dict={"query_intent": dirPathAnalysisOutput+"Archive/Tokenization/Intermediate/"+month+"/6_month",
                          "history_interactions": dirSearchHistoryProcessed+"rawHistoryDataParquetFiles/"+history_months_spread_name,
                          "transpose_catalogue_with_hierarchy": f"{dirPathETL}TransposedCatalogueAttributesWithHierarchy",
                          "transpose_catalogue_numerical": f"{dirPathETL}TransposedCatalogueAttributesNumerical"},
    output_filenames_dict={"percentage_interaction": "percentage_product_match",
                           "percentage_interaction_intent_aggregated": "percentage_product_match_intent_aggregated"},
    dag=dag,
	config_group="config_group_search_engine_testing",
    description=''
)
GetPercentageInteractionCovered.set_upstream([identify_intent])

ConvertGroundDataToTopNStepWiseInput = CouturePythonDockerOperator(
    task_id='ConvertGroundDataToTopNStepWiseInput',
    trigger_rule = 'none_skipped',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,  #_gtruth,
    python_deps=[commons_egg, python_egg],
  	volumes=[searchEngineVolume],
    method_id='convert_gtruthdata_parquet',
    method_args_dict={},
    input_base_dir_path=dirPathRawData,
    output_base_dir_path="",
    input_filenames_dict={"gtruth_path":"original_groundtruth_data.csv"},
    output_filenames_dict={"groundtruth_parquet_path":dirGroundTruth+str_date+"/"+"groundTruthParquetData",
                          "final_analysis": f"{dirPathFinalOutput}run_{str_date}/"},
    description=''
)
ConvertGroundDataToTopNStepWiseInput.set_upstream([CheckFilePresence])

GroundTruthTopNStepWiseOutput = CouturePythonDockerOperator(
    task_id='GroundTruthTopNStepWiseOutput',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_nstep_wise_output',
    method_args_dict={"query_column": "search_term"},
    volumes=[searchEngineVolume],
    input_base_dir_path="",
    output_base_dir_path=dirGroundTruth+str_date+"/SimulatorOutput/",
    input_filenames_dict={
        "all_queries_path":dirGroundTruth+str_date+"/groundTruthParquetData",
        "r2syn_path": dirPathProcessed+"AnalysisCorpusJSON/"+"r2syn.json",
        "r2sub_path": dirPathProcessed+"AnalysisCorpusJSON/"+"r2sub.json",
        "w2r_path": dirPathProcessed+"AnalysisCorpusJSON/"+ "w2r.json",
        "r2e_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2e.json",
        "r2l1l2_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2l1l2.json",
        "r2l1l3_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2l1l3.json",
        "r2_token_type_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2tokentype.json",
        "r2_isCategory_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2iscategory.json",
        "phrase_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"phrases.json",
        "num_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"numericalranges.json",
        "entity_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"wordentities.json",
    	"custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"month_data_out": "FinalAnalysis/ground_truth_data_stepwise_output.csv",
                           "query_wise_intent_out": "Archive/query_wise",
                           "topN_step_wise_path": "FinalAnalysis/ground_truth_data_topn_parquet"},
    description=''
)
GroundTruthTopNStepWiseOutput.set_upstream([ConvertGroundDataToTopNStepWiseInput, GenerateJSON])


NewGroundTruthTopNStepWiseOutput = CouturePythonDockerOperator(
    task_id='NewGroundTruthTopNStepWiseOutput',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_nstep_wise_output',
    method_args_dict={"query_column": "query"},
    volumes=[searchEngineVolume],
    input_base_dir_path="",
    output_base_dir_path=dirPathProcessed + "analysis/new_ground_truth/",
    input_filenames_dict={
        "all_queries_path": "/data1/searchengine/rawdata/ground_truth_data/new_ground_truth_data.csv",
        "r2syn_path": dirPathProcessed+"AnalysisCorpusJSON/"+"r2syn.json",
        "r2sub_path": dirPathProcessed+"AnalysisCorpusJSON/"+"r2sub.json",
        "w2r_path": dirPathProcessed+"AnalysisCorpusJSON/"+ "w2r.json", 
        "r2e_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2e.json",
        "r2l1l2_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2l1l2.json", 
        "r2l1l3_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2l1l3.json",
        "r2_token_type_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2tokentype.json", 
        "r2_isCategory_path":dirPathProcessed+"AnalysisCorpusJSON/"+ "r2iscategory.json",
        "phrase_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"phrases.json", 
        "num_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"numericalranges.json",
        "entity_corpus_path": dirPathProcessed+"AnalysisCorpusJSON/"+"wordentities.json",
        "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={"month_data_out": "ground_truth_data_stepwise_output.csv",
                           "query_wise_intent_out": "query_wise",
                           "topN_step_wise_path": "ground_truth_data_topn_parquet"},
    description=''
)
NewGroundTruthTopNStepWiseOutput.set_upstream([GenerateJSON])

GroundTruthAnalysis = CouturePythonDockerOperator(
    task_id='GroundTruthAnalysis',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,  # _gtruth,
    python_deps=[commons_egg, python_egg],
  	volumes=[searchEngineVolume],
    method_id='ground_truth_analysis',
    method_args_dict={},
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"sim_output_path": dirGroundTruth+ str_date + "/"+"SimulatorOutput/FinalAnalysis/ground_truth_data_stepwise_output.csv",
                          "original_path":dirPathRawData + "original_groundtruth_data.csv"},
    output_filenames_dict={"analysis_path": dirGroundTruth+str_date+"/"+"analysis/",
                          "final_analysis": f"{dirPathFinalOutput}run_{str_date}/"},
    description=''
)
GroundTruthAnalysis.set_upstream([GroundTruthTopNStepWiseOutput])

# GetProductCounts = CouturePySparkOperator(
#     task_id='GetProductCounts',
#     dag=dag,
#     code_artifact="__main__pyspark.py",
#     method_id='get_product_counts',
#     input_base_dir_path="",
#     output_base_dir_path="",
#     input_filenames_dict={"simulator_output_path":dirGroundTruth + str(str_date) + "/" + "analysis/processedSimulatorOutput/",
#                          "groundtruth_path":dirPathRawData + "original_groundtruth_data.csv",
#                          "transpose_catalogue_attributes":dirPathProcessed+"TransposedCatalogueAttributesWithHierarchy"},
#     output_filenames_dict={"product_counts":dirGroundTruth+str(str_date) + "/productCounts/",
#                           "final_analysis":dirPathAnalysisOutput+"FinalAnalysis/"},
#     description='',
# )
# GetProductCounts.set_upstream([GroundTruthTopNStepWiseOutput])

# ======== LEVEL 8 ======== #

FormatAnalysisFiles = CouturePythonDockerOperator(
    task_id='FormatAnalysisFiles',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='format_analysis_files',
    method_args_dict={"query_count_1": query_count_all,
                      "query_count_2": query_count_short},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput + "AggregatedScores/",
    output_base_dir_path=dirPathFinalOutput + "run_" + str_date,
    input_filenames_dict={"top_n_under_score": "top_n_under_score.csv",
                          "history_categories_1_token": "history_categories_top_"+str(query_count_short)+"_queries_1_token_coverage.csv",
                          "history_categories_2_token": "history_categories_top_"+str(query_count_short)+"_queries_2_token_coverage.csv",
                          "w2r_precision_score": "w2r_precision_score.csv",
                          "w2r_precision_score_categorical": "w2r_precision_score_categorical.csv",
                          "tokenization": dirPathFinalOutput + "run_"+str_date + "/tokenization.csv",
                          "tokenization_zsr": dirPathFinalOutput + "run_"+str_date + "/" + zsr + "_tokenization.csv"},
    output_filenames_dict={"summary_out": "summary.txt"},
    description=''
)
FormatAnalysisFiles.set_upstream([TopNUnderScore, TopNHistoryCoverage1NetScore, TopNHistoryCoverage2NetScore,
                                  W2rNetPrecision, W2rNetPrecisionCategorical, WordCorpusCoverage, ConvertToNum,
                                  ConvertToNumZsr, GetPercentageInteractionCovered, GroundTruthAnalysis ])

with TaskGroup("QualityChecks", dag = dag) as QualityChecks:
    PerformQualityChecks = CoutureSparkOperator(
        task_id="PerformQualityChecks",
        dag=dag,
        code_artifact=code_artifact,
        class_path=classPath,
        method_id="PerformQualityChecks",
        method_args_dict={"old_corpus": f"{catalogue_date_old.strftime('%d%m%Y')}",  # "20032023/V1",
                          "new_corpus": f"{catalogue_date.strftime('%d%m%Y')}"},  # "26042023/V1"},
        input_base_dir_path=f"{dirPathAjio}10032024",
        output_base_dir_path=dirPathQualityChecks,
        input_filenames_dict={# "brands": "Brands",
          					  # "special_brands": "SpecialBrands",
          					  # "rightwords": "RightwordsCombined",
          					  # "phrases_initial": "FilteredPhrases",
          					  # "w2r_spell_variants": "SpellVariants"
            "brands_old": "V1/Brands",
            "brands_new": "etl/Brands",
            "special_brands_old": "V1/SpecialBrands",
            "special_brands_new": "etl/SpecialBrands",
            "rightwords_old": "V1/Rightwords",
            "rightwords_new": f"{version}/RightwordsCombined",
            "phrases_initial_old": "V1/FilteredPhrases",
            "phrases_initial_new": f"{version}/PhrasesFiltered",
            "w2r_spell_variants_old": "V1/Wrong2RightWordsSpellVariants",
            "w2r_spell_variants_new": f"{version}/SpellVariants",
			"w2r_1to1_old": "V1/FinalW2RMappings",
			"w2r_1to1_new": f"{version}/W2RFinal1To1"
        },
        output_filenames_dict={"brands": "BrandsComparison",
                               "special_brands": "SpecialBrandsComparison",
                               "rightwords": "Rightwords",
                               "is_pure_brand": "IsPureBrandTags",
                               "is_brand": "IsBrandTags",
                               "phrases_initial": "PhrasesInitiallyGenerated",
                               "w2r_1to1_rightword": "W2RRightwords",
                               "w2r_1to1_score": "W2RScores"},
        description="Performs quality checks on the corpus run of the latest catalogue"
    )
QualityChecks.set_upstream([FormatAnalysisFiles])
# ======== LEVEL 9 ======== #

CompileAnalysisReport = CouturePythonDockerOperator(
    task_id='CompileAnalysisReport',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='compile_analysis_report',
    method_args_dict={},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathFinalOutput,
    output_base_dir_path=dirPathFinalOutput + "run_" + str_date,
    input_filenames_dict={"catalogue_summary": "Summary/CatalogueSummary",
                          "catalogue_summary_old": dirPathFinalOutputOld + "Summary/CatalogueSummary",
                          # "corpus_summary": "Summary/AllCorpusSummary",
                          "corpus_summary": dirPathFinalOutputOld + "Summary/AllCorpusSummary",
                          "corpus_summary_old": dirPathFinalOutputOld + "Summary/AllCorpusSummary",
                          "run_summary": "run_" + str_date + "/summary.txt",
                          "run_summary_old": dirPathFinalOutputOld + oldRunDate + "/summary.txt",
                          "step_wise": "run_" + str_date + "/top_"+str(query_count_all)+"_stepwise_output.csv",
                          "tokenization": "run_" + str_date + "/tokenization.csv",
                          "single_token": "run_" + str_date + "/history_categories_top_"+str(query_count_short)+"_queries_1_token_coverage.csv",
                          "double_token": "run_" + str_date + "/history_categories_top_"+str(query_count_short)+"_queries_2_token_coverage.csv",
                          "changed_brands": "run_" + str_date + "/QualityChecks/BrandsComparison",
                          "ground_truth": "run_" + str_date + "/groundtruth_simulator_comparitive_analysis_l1l3categories.csv",
                          "tokenization_zsr": "run_" + str_date + "/" + zsr + "_tokenization.csv",
                          "step_wise_zsr": "run_" + str_date + "/" + zsr + "_top_" + str(query_count_all) + "_stepwise_output_.csv",
                          "category_performance": "run_" + str_date + "/tokens_category_performance_only_numbers_csv.csv"},
    output_filenames_dict={"analysis_report": "Analysis.xlsx"},
    description=''
)
CompileAnalysisReport.set_upstream(QualityChecks)

CopyLastIterationFiles = CoutureSparkOperator(
    task_id='CopyLastIterationFiles',
    method_id='CopyLastIterationFiles',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict = {"analysis_path": dirPathAnalysisOutput,
                        "catalogue": catalogue_date.strftime('%Y%m%d')},
    input_base_dir_path="",
    input_filenames_dict={"second_last_iteration": dirPathAnalysisOutput+"SecondLastIteration",
                          "last_iteration": dirPathAnalysisOutput+"LastIteration",
                          "current_iteration": dirPathAnalysisOutput+"CurrentIteration",
                          "final": dirPathAnalysisOutput+"FinalAnalysis",
                          "word_corpus": dirPathProcessed+"Wrong2RightUpdatedHistory",
                          # "phrase_corpus": dirPathProcessed+"PhrasesUpdated",
                          # "phrase_corpus": dirPathProcessed+"PhrasesWithCorrections",
                          "phrase_corpus": dirPathProcessed + phrasesFinalDataFrame,
                          "numerical_corpus": dirPathProcessed+"NumericalEntitiesRangeWise",
                          "entity_corpus": dirPathProcessed+"JSONData/EntityCorpusDF"},
    dag=dag,
    description=''
)
CopyLastIterationFiles.set_upstream([FormatAnalysisFiles, TopNStepWiseZsrOutput, ConvertToNum, ConvertToNumZsr])

CompareCorpuses = DagOperator(
    task_id='CompareCorpuses',
    run_dag_id='CompareSearchData',
    dag=dag,
    description=''
)
CompareCorpuses.set_upstream([CopyLastIterationFiles])


# GroundTruthAnalysisDAG = DagOperator(
#     task_id='GroundTruthAnalysisDAG',
#     run_dag_id='groundTruthQueriesProductCounts',
#     dag=dag,
#     description=''
# )
# GroundTruthAnalysisDAG.set_upstream([CopyLastIterationFiles])
# GroundTruthAnalysisDAG = TriggerDagRunOperator(
#     task_id='GroundTruthAnalysisDAG',
#     trigger_dag_id='groundTruthQueriesProductCounts',
#   	# execution_date = '{{ds}}',
#   	reset_dag_run = True,
#     dag=dag,

# )
# GroundTruthAnalysisDAG.set_upstream([CopyLastIterationFiles])







# ========================== #
# ======= DEPRECATED ======= #
# ========================== #

# combine_history_data = CoutureSparkOperator(
#     task_id='combine_history_data',
#     method_id='CombineHistoryData',
#     class_path=classPath,
#     code_artifact=code_artifact,
#     method_args_dict={"search_term_column": "search_term",
#                       "max_query_length":3},
#     input_base_dir_path="",
#     output_base_dir_path=dirSearchHistoryProcessed,
#     input_filenames_dict={"search_term_data": dirPathHistoryData},
#     output_filenames_dict={"all_raw_queries": "search_"+month.upper(),
#                            "processed_history": "HistoryMergedShortQueries"},
#     dag=dag,
#     description=''
# )

# merge_history_data = CoutureSparkOperator(
#     task_id='merge_history_data',
#     method_id='MergeHistoryData',
#     class_path=classPath,
#     code_artifact=code_artifact,
#     method_args_dict={"max_query_length":3},
#     input_base_dir_path="",
#     output_base_dir_path=dirSearchHistoryProcessed,
#     input_filenames_dict={"search_term_data": dirPathHistoryData},
#     output_filenames_dict={"processed_history": "HistoryMergedShortQueries"},
#     dag=dag,
#     description=''
# )

# suggest_categories_from_history = CoutureSparkOperator(
#     task_id='suggest_categories_from_history',
#     method_id='SuggestCategoriesFromHistory',
#     class_path=classPath,
#     code_artifact=code_artifact,
#     method_args_dict={},
#     input_base_dir_path="",
#     output_base_dir_path=dirPathProcessed + "analysis/ajio/" + month + "/",
#     input_filenames_dict={"transpose_catalogue_attributes": dirPathProcessed + "TransposedCatalogueAttributesWithHierarchy",
#                           "history_data": "/data1/searchengine/rawdata/historydata",
#                           "word_analysis_df": dirPathProcessed + "Wrong2RightWordsAnalysisDF1"},
#     output_filenames_dict={"history_words_categories_interaction": "HistoryRightWordsCategoriesInteractionFull",
#                            "corpus_words_category_performance": "CorpusWordsCategoryPerformanceFull",
#                            "corpus_words_category_performance_csv": "CorpusWordsCategoryPerformanceCSVFull"},
#     dag=dag,
#     description=''
# )
# suggest_categories_from_history.set_upstream([combine_history_data])

# suggest_entities_from_history = CoutureSparkOperator(
#     task_id='suggest_entities_from_history',
#     method_id='SuggestEntitiesFromHistory',
#     class_path=classPath,
#     code_artifact=code_artifact,
#     method_args_dict={"min_category_percentile": 45,
#                       "min_l1_percentile": 30},
#     input_base_dir_path="",
#     output_base_dir_path=dirPathProcessed + "analysis/ajio/" + "Oct_Dec" + "/",
#     input_filenames_dict={"transpose_catalogue_attributes": dirPathProcessed + "TransposedCatalogueAttributesWithHierarchy",
#                           "history_data": "/data1/searchengine/rawdata/historydata/SEARCH_TERM_DATA_20201224.csv",
#                           "word_analysis_df": dirPathProcessed + "analysis/ajio/Wrong2RightWordsAnalysisDF1"},
#     output_filenames_dict={"history_words_categories_interaction": "HistoryRightWordsEntitiesInteractionSolr",
#                            "corpus_words_entity_performance": "CorpusWordsEntityyPerformanceSolr",
#                            "corpus_words_category_performance_csv": "CorpusWordsEntityPerformanceCSVSolr"},
#     dag=dag,
#     description=''
# )
# suggest_entities_from_history.set_upstream([combine_history_data])

# compare_two_word_corpuses = CoutureSparkOperator(
#     task_id='compare_two_word_corpuses',
#     method_id='CompareTwoWordCorpuses',
#     class_path=classPath,
#   extra_hosts=kerberos_hosts,
#     code_artifact=code_artifact,
#     method_args_dict={"compare_column": "spellChecked",
#                       "primary_column": "word"},
#     input_base_dir_path=dirPathOutput,
#     output_base_dir_path=dirPathOutput + "comparisons",
#     input_filenames_dict={"month_word_data_a": "processedPRODMar/analysis/ajio/Oct_Dec/2_month_word.csv",
#                           "month_word_data_b": "processedPRODDec/analysis/ajio/Oct_Dec/2_month_word.csv"},
#     output_filenames_dict={"month_word_comparison": "MonthWordOctDecPhase1and1x"},
#     dag=dag,
#     description=''
# )
# compare_two_word_corpuses.set_upstream([spell_correct])

# analyse_entity_performance = CoutureSparkOperator(
# 	task_id='analyse_entity_performance',
#     method_id='AnalyseEntityPerformance',
#     class_path=classPath,
#     code_artifact=code_artifact,
#     method_args_dict={},
#     input_base_dir_path="",
#     output_base_dir_path=dirPathProcessed + "analysis/ajio/" + month,
#     input_filenames_dict={"history_query_jsons": dirPathProcessed + "analysis/ajio/" + month + "/6_month",
#                           "search_history_data": "/data1/searchengine/rawdata/historydata"},
#     output_filenames_dict={"month_data_out": "7_month"},
#     dag=dag,
#     description=''
# )
# analyse_entity_performance.set_upstream([identify_intent])
	
TopNStepWiseOutputOfInterest = CouturePythonDockerOperator(
    task_id='TopNStepWiseOutputOfInterest',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_nstep_wise_output',
    method_args_dict={"n": 10000,
                      "query_column": "ProductInfo"},  # "search_term"},
    volumes=[searchEngineVolume],
    input_base_dir_path=dirPathProcessed + "AnalysisCorpusJSON/",
    output_base_dir_path="",
    input_filenames_dict={"all_queries_path": "/data1/searchengine/rawdata/lookerstudio/QueriesOfInterestParquetData",
                          # "/data1/searchengine/archive/meghana/NonAjiofashionDict/macmillanDictionary/topnIP",
                          # "/data1/searchengine/archive/meghana/topnmyntra",
                          # "/data1/searchengine/processedHistory/ajio/search_"+top_n_month,
                          # "/data1/archive/meghana/topninput/searchqueries_1_10dec.csv",
                          # "/data1/searchengine/rawdata/AjioExperimentData/Search queries - 20th Sep-4th Oct - results-20221005-220847.csv",
                          # "/data1/archive/data/output - Sheet1.csv",
                          # "/data1/searchengine/processedHistory/ajio/rawHistoryDataParquetFiles/20220810_20220920",
                          "r2syn_path": "r2syn.json",
                          "r2sub_path": "r2sub.json",
                          "w2r_path": "w2r.json",
                          "r2e_path": "r2e.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json",
                          "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={
      # "month_data_out" : "stepwise.csv",
      #                      "query_wise_intent_out" : "query_wise",
      #                      "query_open_out" : "query_open",
      #                      "query_weighted_out" : "query_weigh",
      #                      "query_hard_out" : "query_hard",
      #                      "topN_step_wise_path" : "topn_parquet"},
      						"month_data_out": dirPathFinalOutput+"run_"+str_date+"/top_1000_golden_queries_stepwise_output.csv",
      						"query_wise_intent_out": dirPathAnalysisOutput+"Archive/query_wise",
      						"query_open_out": dirPathAnalysisOutput+"Archive/ProductCount/query_open",
      						"query_weighted_out": dirPathAnalysisOutput+"Archive/ProductCount/query_weighted",
      						"query_hard_out": dirPathAnalysisOutput+"Archive/ProductCount/query_hard",
      						"topN_step_wise_path": dirPathFinalOutput+"run_"+str_date+"/TopNGoldenQueries"},
    description=''
)
TopNStepWiseOutputOfInterest.set_upstream([GenerateJSON])

GenerateQueryFilters = CouturePythonDockerOperator(
    task_id='GenerateQueryFilters',
    image=python_image,
    api_version='auto',
    auto_remove=False,
    command="/bin/bash echo 'heloword'",
    extra_hosts=kerberos_hosts,
    user='couture',
    dag=dag,
  	mem_limit="18g",
    code_artifact=code_artifact_python,
    python_deps=[commons_egg, python_egg],
    method_id='top_nstep_wise_output',
    method_args_dict={"n": query_count_all,
                      "query_column": "query"},  # "search_term"},
    volumes=[searchEngineVolume],
    config_group = "config_group_old_cluster",
    input_base_dir_path=dirPathProcessed + "AnalysisCorpusJSON/",
    output_base_dir_path="",
    input_filenames_dict={"all_queries_path": f"/data1/searchengine/phase2/Dataset/SearchEvaluationHistoryQueries/",
                          # "/data1/searchengine/archive/meghana/NonAjiofashionDict/macmillanDictionary/topnIP",
                          # "/data1/searchengine/archive/meghana/topnmyntra",
                          # "/data1/searchengine/processedHistory/ajio/search_"+top_n_month,
                          # "/data1/archive/meghana/topninput/searchqueries_1_10dec.csv",
                          # "/data1/searchengine/rawdata/AjioExperimentData/Search queries - 20th Sep-4th Oct - results-20221005-220847.csv",
                          # "/data1/archive/data/output - Sheet1.csv",
                          # "/data1/searchengine/processedHistory/ajio/rawHistoryDataParquetFiles/20220810_20220920",
                          "r2syn_path": "r2syn.json",
                          "r2sub_path": "r2sub.json",
                          "w2r_path": "w2r.json",
                          "r2e_path": "r2e.json",
                          "r2l1l2_path": "r2l1l2.json",
                          "r2l1l3_path": "r2l1l3.json",
                          "r2_token_type_path": "r2tokentype.json",
                          "r2_isCategory_path": "r2iscategory.json",
                          "phrase_corpus_path": "phrases.json",
                          "num_corpus_path": "numericalranges.json",
                          "entity_corpus_path": "wordentities.json",
                          "custom_splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
    output_filenames_dict={
      # "month_data_out" : "stepwise.csv",
      #                      "query_wise_intent_out" : "query_wise",
      #                      "query_open_out" : "query_open",
      #                      "query_weighted_out" : "query_weigh",
      #                      "query_hard_out" : "query_hard",
      #                      "topN_step_wise_path" : "topn_parquet"},
      						"month_data_out": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"run_"+str_date+"/top_"+str(query_count_all)+"_stepwise_output.csv",
      						"query_wise_intent_out": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"Archive/query_wise",
      						"query_open_out": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"Archive/ProductCount/query_open",
      						"query_weighted_out": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"Archive/ProductCount/query_weighted",
      						"query_hard_out": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"Archive/ProductCount/query_hard",
      						"topN_step_wise_path": "/data1/searchengine/phase2/Dataset/Phase1QueriesIntentExtracted/"+"run_"+str_date+"/topn_parquet"},
    description=''
)
