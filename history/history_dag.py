ColdCacheQueries = CoutureSparkOperator(
    task_id='ColdCacheQueries',
    method_id='GetColdCacheQueries',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                       "freq_colunm": "total_searches",
                       "topk": 100000},
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"history_data":"/data1/searchengine/rawdata/historydata/jiomart/dump/query_level_data"},
    output_filenames_dict={"cold_cache_queries": "ColdCacheQueries"},
    dag=Dag,
    description=''
)

ParseCleanAndNormaliseHistoryData = CoutureSparkOperator(
    task_id='ParseCleanAndNormaliseHistoryData',
    method_id='ParseCleanAndNormaliseHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "search_term",
                      "type_of_data": "query_level_data",
                      "freq_column": "total_searches"},
    input_base_dir_path=dirPathProcessedHistory,
    output_base_dir_path=dirPathProcessedHistory,
    input_filenames_dict={"search_term_data": "HistoryQueriesNormalised"},
    output_filenames_dict={"history_queries_normalised": "path"},
    dag=Dag,
    description=''
)