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