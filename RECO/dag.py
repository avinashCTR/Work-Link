# airflow import
from airflow import DAG

from airflow.utils.task_group import TaskGroup

# custom operators import
from CouturePythonDockerPlugin import CouturePythonDockerOperator
from CoutureSpark3Plugin import CoutureSpark3Operator as CoutureSparkOperator
from DagOperatorPlugin import DagOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import date, timedelta

classPath = "ai.couture.discovery.MainClass"
code_artifact = "couture-discovery-pipelines-1.0.0-avinash.jar"

date = date.today().strftime("%d-%m-%Y")

default_args = {
    "owner": "couture",
    "depends_on_past": False,
    "start_date": date,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "config_group": "config_group_jiomart"
}

Dag = DAG("couture-discovery-test", default_args=default_args, concurrency=4, schedule_interval=None, tags=["couture-discovery"])

important_attributes = ["brandname"]

LTOV = CoutureSparkOperator(
    task_id="extractLTOV",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="LifeTimeOrderValue",
    method_args_dict={"":""},
    input_base_dir_path="/data/recommendations/ajio/sample",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"interactions"},
    output_filenames_dict={"ltov":"LTOV"},
    description=""
)

TVOR = CoutureSparkOperator(
    task_id="extractTVOR",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="TotalValueOfReturn",
    method_args_dict={"":""},
    input_base_dir_path="/data/recommendations/ajio/sample",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"interactions"},
    output_filenames_dict={"tvor":"TVOR"},
    description=""
)

TNOR = CoutureSparkOperator(
    task_id="extractTNOR",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="TotalNoOfReturns",
    method_args_dict={"":""},
    input_base_dir_path="/data/recommendations/ajio/sample",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"interactions"},
    output_filenames_dict={"tnor":"TNOR"},
    description=""
)

AIO = CoutureSparkOperator(
    task_id="extractAIO",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="AverageIntervalOrders",
    method_args_dict={"":""},
    input_base_dir_path="/data/recommendations/ajio/sample",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"interactions"},
    output_filenames_dict={"aio":"AIO"},
    description=""
)

OFPW = CoutureSparkOperator(
    task_id="extractOFPW",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="OrderFrequencyPerWeek",
    method_args_dict={"":""},
    input_base_dir_path="/data/recommendations/ajio/sample",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"interactions"},
    output_filenames_dict={"ofpw":"OFPW"},
    description=""
)

ABV = CoutureSparkOperator(
    task_id="extractABV",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="AverageBagValue",
    method_args_dict={"":""},
    input_base_dir_path="",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"/data/recommendations/ajio/sample/interactions",
                          "product_info":"/data/ecomm/ajio/processed/productAttributesLegosFNL"
                         },
    output_filenames_dict={"abv":"ABV"},
    description=""
)

APD = CoutureSparkOperator(
    task_id="extractAPD",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="AveragePercentageDiscount",
    method_args_dict={"":""},
    input_base_dir_path="",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"/data/recommendations/ajio/sample/interactions",
                          "product_info":"/data/ecomm/ajio/processed/productAttributesLegosFNL"
                         },
    output_filenames_dict={"apd":"APD"},
    description=""
)

ACTCT = CoutureSparkOperator(
    task_id="extractACTCT",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="AverageCartToCheckoutTime",
    method_args_dict={"":""},
    input_base_dir_path="",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"/data/recommendations/ajio/sample/interactions",
                          "product_info":"/data/ecomm/ajio/processed/productAttributesLegosFNL"
                         },
    output_filenames_dict={"actct":"ACTCT"},
    description=""
)

TNOC = CoutureSparkOperator(
    task_id="extractTNOC",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="TotalNoOfCategories",
    method_args_dict={"":""},
    input_base_dir_path="",
    output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
    input_filenames_dict={"interactions":"/data/recommendations/ajio/sample/interactions",
                          "product_info":"/data/ecomm/ajio/processed/productAttributesLegosFNL"
                         },
    output_filenames_dict={"tnoc":"TNOC"},
    description=""
)

with TaskGroup(group_id="attribute_affinity_tasks", dag=Dag) as attribute_affinity_group:
    for attr in important_attributes:
        CoutureSparkOperator(
            task_id=f"extract_{attr}_affinity",
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id="AttributeAffinity",
            method_args_dict={"attribute_col_name": attr},
            input_base_dir_path="",
            output_base_dir_path="/data1/archive/avinash/RECO/userFeatures",
            input_filenames_dict={
                "interactions": "/data/recommendations/ajio/sample/interactions",
                "product_info": "/data/ecomm/ajio/processed/productAttributesLegosFNL"
            },
            output_filenames_dict={"affinity": f"{attr}_affinity"},
            description=""
        )

UAF = CoutureSparkOperator(
    task_id="UnionAllFeatures",
    dag=Dag,
    code_artifact=code_artifact,
    class_path=classPath,
    method_id="UnionAllFeatures",
    method_args_dict={"":""},
    input_base_dir_path="/data1/archive/avinash/RECO",
    output_base_dir_path="/data1/archive/avinash/RECO",
    input_filenames_dict={"user_features_files":"userFeatures"},
    output_filenames_dict={"user_features_files":"userFeaturesFinal"},
    description=""
)

trigger_task = DummyOperator(
    task_id="trigger_pipeline",
    dag=Dag
)

trigger_task.set_downstream([
    LTOV, TVOR, TNOR, AIO, OFPW, ABV, APD, ACTCT, TNOC, attribute_affinity_group
])

UAF.set_upstream([
    LTOV, TVOR, TNOR, AIO, OFPW, ABV, APD, ACTCT, TNOC, attribute_affinity_group
])

with TaskGroup(group_id="attribute_recommendation_tasks", dag=Dag) as attribute_recommendation_group:
    for attr in important_attributes:
        CoutureSparkOperator(
            task_id=f"AttributeRecommendation_{attr}",
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id="AttributeRecommendation",
            method_args_dict={"attribute_col_name": attr, "top_n": 10},
            input_base_dir_path="/data1/archive/avinash/RECO",
            output_base_dir_path="/data1/archive/avinash/RECO/userRecommendations",
            input_filenames_dict={"scored_interactions": "userFeaturesFinal"},
            output_filenames_dict={"recommendations": f"{attr}_recommendations"},
            description=""
        ).set_upstream(UAF)

attribute_recommendation_group.set_upstream([UAF])
