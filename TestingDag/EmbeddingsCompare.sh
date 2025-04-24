#!/bin/bash

# Default values
stella_model_path="/data1/searchengine/EmbeddingTrainingDataCuration/ajio/trained_models/mpnet_v3_ep3"
product_descriptions_path="/data1/archive/avinash/SearchTests/TestData/uk_us_data.parquet"
product_embeddings_path="/data1/archive/avinash/SearchTests/TestData/outputs/uk_us_output_mpnet_v3e3.csv"
product_desc_col="query"
product_desc_col2="map_queries"
n=20000
model_name="model"
batch_size=1024
step_size=100000
multi_gpu=True

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --model_path) stella_model_path="$2"; shift ;;
        --query_file_path) product_descriptions_path="$2"; shift ;;
        --report_csv_path) product_embeddings_path="$2"; shift ;;
        --query_col1) product_desc_col="$2"; shift ;;
        --query_col2) product_desc_col2="$2"; shift ;;
        --n) n="$2"; shift ;;
        --model_name) model_name="$2"; shift ;;
        --batch_size) batch_size="$2"; shift ;;
        --step_size) step_size="$2"; shift ;;
        --multi_gpu) multi_gpu="$2"; shift ;;
        *) echo "Unknown option $1"; exit 1 ;;
    esac
    shift
done

# Run the Python script with the provided parameters
python /app/notebooks/avinash/ModelTestingDag/EmbeddingsCompare.py \
    --model_path="$stella_model_path" \
    --query_file_path="$product_descriptions_path" \
    --report_csv_path="$product_embeddings_path" \
    --query_col1="$product_desc_col" \
    --query_col2="$product_desc_col2" \
    --model_name="$model_name" \
    --batch_size="$batch_size" \
    --step_size="$step_size" \
    --multi_gpu="$multi_gpu" \
    --n="$n"
