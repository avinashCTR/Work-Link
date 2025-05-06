#!/bin/bash
set -e

# Default values
Brands_path="/data1/searchengine/processed/ajio/09022025/etl/Brands"
endpoint="pre-validation"
disable_spell_check="True"
output_path="/data1/archive/avinash"

# Overriding the default values with arguments, if provided
while [[ $# -gt 0 ]]; do
  case "$1" in
    --brands_path)
      Brands_path="$2"
      shift 2
      ;;
    --endpoint)
      endpoint="$2"
      shift 2
      ;;
    --disable_spell_check)
      disable_spell_check="$2"
      shift 2
      ;;
    --output_path)
     output_path="$2"
     shift 2
     ;;
    *)
      shift
      ;;
  esac
done

# Calling the python script with the set values
python /app/notebooks/avinash/Gaurdrails_task/Brand-Testing/Build-Script.py \
--brands_path "$Brands_path" \
--endpoint "$endpoint" \
--disable_spell_check "$disable_spell_check" \

hdfs dfs -test -e "$output_path/brands.csv" && hdfs dfs -rm "$output_path/brands.csv"

hdfs dfs -copyFromLocal /app/notebooks/avinash/Gaurdrails_task/output/Brand_Recognition_output.csv "$output_path/brands.csv"
