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
    input_filenames_dict={"history_data":"/data1/searchengine/rawdata/historydata/jiomart/dump/query_level_data/date"},
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
                      "freq_column": "total_searches"},
    input_base_dir_path=dirPathProcessedHistory,
    output_base_dir_path=dirPathProcessedHistory,
    input_filenames_dict={"search_term_data": "HistoryQueriesNormalised"},
    output_filenames_dict={"history_queries_normalised": "path"},
    dag=Dag,
    description=''
)

PreProcessHistory = CoutureSparkOperator(
    task_id='PreProcessHistory',
    method_id='PreProcessHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column":"search_term",
        "freq_column":"product_unique_list_view",
        "counts_coulumn":"product_unique_list_clicks",
        "product_id_column":"sku",
        "vertical_column":"product_vertical"
    },
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"query_product_interactions":"/data1/archive/",
    "query_position_data":"/data1/archive/"},
    output_filenames_dict={"history_queries_data_archive": "ColdCacheQueries",
    "history_queries_data":"asdasdasd"
    },
    dag=Dag,
    description=''
)

NormalizeQueryHistoryData = CoutureSparkOperator(
    task_id='NormalizeQueryHistory',
    method_id='NormalizeQueryHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={
        "search_term_column":"search_term",
    },
    input_base_dir_path="",
    output_base_dir_path="",
    input_filenames_dict={"search_term_data":""},
    output_filenames_dict={"history_normalized_archive": "ColdCacheQueries",
    "history_normalized_data":"asdasdasd"
    },
    dag=Dag,
    description=''
)

GetShortQueries = CoutureSparkOperator(
    task_id='GetShortQueries',
    method_id='GetShortQueriesHistoryData',
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"search_term_column": "query",
                      "max_query_length": 3,
                      "query_split_column":"query_normalised"
                      },
    input_base_dir_path=dirPathProcessedHistory,
    output_base_dir_path=dirPathProcessedHistory,
    input_filenames_dict={"history_data_grouped": "rawHistoryDataParquetFiles"},
    output_filenames_dict={"short_queries": "HistoryMergedShortQueries","short_queries_archive":"HistoryMergedShortQueries/archive"},
    dag=Dag,
    description=''
)

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
    input_base_dir_path=dirPathProcessedHistory,
    output_base_dir_path=dirPathProcessedHistory,
    input_filenames_dict={"query_product_interactions":"/data1/archive/",
    "query_position_data":"/data1/archive/"},
    output_filenames_dict={"DataSciencePreProcessed": "HistoryMergedShortQueries","short_queries_archive":"HistoryMergedShortQueries/archive"},
    dag=Dag,
    description=''
)