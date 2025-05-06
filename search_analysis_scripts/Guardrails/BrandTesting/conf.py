# model_endpoint = 'http://10.166.181.219:8498/search-model-test'

input_parameters = {
    "query": "puma",
    "store": "rilfnl",
    "page_number": 1,
    "records_per_page": 24,
    "include_unrated_products": True,
    "enable_stack_trace": True,
    "filters": [],
    "dev": {
        # "disable_guardrails": True,
        "disable_guardrails": False,
        "disable_spell_check": True,
        # "disable_spell_check": False,
        "enable_classification": False,
        "classification_n_search": 2,
        "classification_iterations": 18,
        "classification_window": 9,
        "classification_threshold": 10.5,
        "disable_cache": True,
        "GPR": 0.05,
        "min_threshold": 100,
        "fetch_scaling": 10,
        "E": {
            "strict_guardrails": True,
            "hnsw_ef": 128,
            "rescore": True,
            "oversampling": 1,
            "EMB": 0.25
        },
        "S": {
            "strict_guardrails": True,
            "hnsw_ef": 128,
            "rescore": True,
            "oversampling": 1,
            "EMB": 0.25,
            "fetch_scaling": 10
        },
        "C": {
            "strict_guardrails": True,
            "hnsw_ef": 128,
            "rescore": True,
            "oversampling": 1,
            "EMB": 0.25,
            "fetch_scaling": 20
        },
        "override": False,
        "disable_embedding_score": False
    },
    "sort_field": "relevance",
    "relevance_experiment_field_name": "boostingScore_rilfnl_premium_product_ranking"
}


all_endpoints = {
    "latest_stable": "http://10.166.181.219:8497/search-latest-stable",
    "pre-validation": "http://10.166.181.219:8491/search-pre-validation",
    # "dev_testing": "http://10.166.181.219:8498/search",
    "test": "http://10.166.181.219:8496/search-dev"
}