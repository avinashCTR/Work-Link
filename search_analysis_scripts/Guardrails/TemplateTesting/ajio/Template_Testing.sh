#!/bin/bash
set -e

# paths and conf
templates_path="/data1/archive/avinash/GuardRails_TemplateQueries"
output_base_path="/data1/archive/avinash/Template-Testing"
local_output_path="/app/notebooks/avinash/Gaurdrails_task/Template-Testing/ajio/output"
vertical="ajio"
phase="dev09022025_1802"
top_n=15

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --templates_path) templates_path="$2"; shift ;;
        --output_base_path) output_base_path="$2"; shift ;;
        --vertical) vertical="$2"; shift ;;
        --phase) phase="$2"; shift ;;     # This is the key part for --phase
        --top_n) top_n="$2"; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done


echo "templates_path: $templates_path"
echo "output_base_path: $output_base_path"
echo "vertical: $vertical"
echo "phase: $phase"
echo "top_n: $top_n"

# calling python script
python /app/notebooks/avinash/Gaurdrails_task/Template-Testing/ajio/Template_Testing.py \
--templates_path=$templates_path \
--output_path=$local_output_path \
--vertical=$vertical \
--phase=$phase \
--top_n=$top_n

hdfs dfs -test -e $output_base_path && hdfs dfs -rm -r $output_base_path

hadoop dfs -mkdir -p $output_base_path

echo "strating copying local output folder to hdfs"
hdfs dfs -copyFromLocal $local_output_path $output_base_path