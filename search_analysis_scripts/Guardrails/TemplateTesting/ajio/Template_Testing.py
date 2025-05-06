import pandas as pd
import requests
import aiohttp
import asyncio
import time
import os
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import re
import argparse

#internal python imports
from couturesearch.guardrails_utils import *
from cache import run_script


class TestGaurdrails():
    
    def __init__(self,templates_path,output_path,vertical,phase,top_n):
        self.templates_path=templates_path
        self.fs = pa.hdfs.connect()
        self.output_path = output_path
        self.vertical=vertical
        # Make this also dynamic
        self.top_n_queries =top_n #15
        self.phase=phase
    
    def load(self):
        print(f"loading the data from {self.templates_path}")
        # self.grdf = pq.ParquetDataset(self.templates_path,self.fs).read().to_pandas()
        self.grdf = pq.ParquetDataset(self.templates_path,self.fs).read().to_pandas()
        
    async def transform(self):
        
        grdf = self.grdf
        
        # add the 5 columns first
        grdf["l1_category"] = None
        grdf["l2_category"] = None
        grdf["l3_category"] = None
        grdf["brickprimarycolor_en_string_mv"] = None
        grdf["brand_string_mv"] = None
        
        # Extract each entity from a row
        grdf['query_template'] = grdf['query_template'].apply(list)
        grdf['query'] = grdf['query'].apply(list)
        grdf["l1"] = grdf.apply(lambda row: self.extract_val(row, "l1_category"), axis=1)
        grdf["l2"] = grdf.apply(lambda row: self.extract_val(row, "l2_category"), axis=1)
        grdf["l3"] = grdf.apply(lambda row: self.extract_val(row, "l3_category"), axis=1)
        grdf["brickprimarycolor_en_string_mv"] = grdf.apply(lambda row: self.extract_val(row, "brickprimarycolor_en_string_mv"), axis=1)
        grdf["brand_string_mv"] = grdf.apply(lambda row: self.extract_val(row, "brand_string_mv"), axis=1)
        grdf = grdf.drop(['l1_category', 'l2_category', 'l3_category'], axis = 1)
        grdf["input_query"] = grdf.apply(lambda row: ' '.join(row['query']), axis=1)
        
        #2nd cell
        print("2nd cell")
        grdf = grdf[grdf['input_query'].map(grdf['input_query'].value_counts()) == 1].reset_index(drop=True)
        grdf = grdf.drop(['product_count'], axis = 1)
        
        # 3rd cell
        # Fill nulls with empty arrays
        print("3rd cell")
        grdf['l1'] = grdf['l1'].apply(lambda x: [x] if isinstance(x, str) else [])
        grdf['l2'] = grdf['l2'].apply(lambda x: [x] if isinstance(x, str) else [])
        grdf['l3'] = grdf['l3'].apply(lambda x: [x] if isinstance(x, str) else [])
        grdf['brand_string_mv'] = grdf['brand_string_mv'].apply(lambda x: [x] if isinstance(x, str) else [])
        grdf['brickprimarycolor_en_string_mv'] = grdf['brickprimarycolor_en_string_mv'].apply(lambda x: [x] if isinstance(x, str) else [])
        
        #4th cell
        # Get full query for analysis
        print("4th cell")
        grdf["query_template_string"] = grdf['query_template'].apply(lambda x: ', '.join(x))
        # Take top n templates
        top_n_queries = self.top_n_queries
        reduced_grdf = grdf.groupby("query_template_string")['input_query'].apply(list).reset_index(name="agg_template_queries")
        reduced_grdf["agg_template_queries"] = reduced_grdf["agg_template_queries"].apply(lambda x: x[:top_n_queries])
        reduced_grdf = reduced_grdf.explode('agg_template_queries')
        new_df = pd.merge(grdf, reduced_grdf,  how='inner', left_on=['query_template_string',"input_query"], right_on = ["query_template_string","agg_template_queries"])
        unique_grdf = grdf.drop_duplicates(subset=['query_template_string'], keep='first')
        
        #5th cell
        print("5th cell")
        arr = await self.process_gr(new_df)
        procdf = pd.DataFrame.from_dict(arr)
        required_columns = ["query", "l1", "l2", "l3", "brand_string_mv", "brickprimarycolor_en_string_mv", "all_syn"]
        # Add missing columns with None
        for column in required_columns:
            if column not in procdf.columns:
                procdf[column] = None
        # # Reorder columns
        procdf = procdf[required_columns]
        procdf = procdf.where(procdf.notna(), None)
        procdf = procdf[~procdf['query'].isna()]
        
        #6th cell
        print("6th cell")
        result = grdf.merge(
            procdf, 
            how='inner', 
            left_on='input_query', 
            right_on='query', 
            suffixes=('_gr', '_proc')
        )

        # Adding prefixes to non-key columns
        # result = result.add_prefix('gr_')
        result = result.rename(columns={'gr_input_query': 'input_query', 'gr_query': 'query'})
        result = result.where(result.notna(), None)
        ### EXACT MATCHING CHECK
        columns_gr = ["l1_gr", "l2_gr", "l3_gr", "brand_string_mv_gr", "brickprimarycolor_en_string_mv_gr"]
        columns_proc = ["l1_proc", "l2_proc", "l3_proc", "brand_string_mv_proc", "brickprimarycolor_en_string_mv_proc"]
        # Filter condition
        condition = ~((result[columns_gr].values == result[columns_proc].values).all(axis=1))
        # condition = (result[columns_gr].values == result[columns_proc].values).all(axis=1)
        filtered_df = result[condition]
        filtered_df.to_parquet(f"{self.output_path}/EXACT_MATCHES.parquet")
        
        #7th cell
        print("7th cell")
        ### SYNONYM check
        filtered_df['all_syn'] = filtered_df['all_syn'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        filtered_df["all_syn"] = filtered_df["all_syn"].apply(lambda x: [] if x is None else x)
        columns_to_check = [
            "brickprimarycolor_en_string_mv_gr",
            "brand_string_mv_gr",
            "l1_gr",
            "l2_gr",
            "l3_gr",
        ]
        filtered_df["check_syn"] = filtered_df.apply(
            lambda row: any(val in row["all_syn"] for val in row[columns_to_check]),
            axis=1,
        )
        filtered_df.to_parquet(f"{self.output_path}/template_queries_with_results.parquet")
        
        #8th cell
        print("8th cell")
        # Fill nulls with empty arrays
        filtered_df['l1_proc'] = filtered_df['l1_proc'].apply(lambda x: x if isinstance(x, list) else [])
        filtered_df['l2_proc'] = filtered_df['l2_proc'].apply(lambda x: x if isinstance(x, list) else [])
        filtered_df['l3_proc'] = filtered_df['l3_proc'].apply(lambda x: x if isinstance(x, list) else [])
        filtered_df['brand_string_mv_proc'] = filtered_df['brand_string_mv_proc'].apply(lambda x: x if isinstance(x, list) else [])
        filtered_df['brickprimarycolor_en_string_mv_proc'] = filtered_df['brickprimarycolor_en_string_mv_proc'].apply(lambda x: x if isinstance(x, list) else [])
        
        #9th cell
        print("9th celll")
        filtered_df['l1_score'] = filtered_df.apply(lambda row: len(set(row['l1_proc']) & set(row['l1_gr'])), axis=1)
        filtered_df['l2_score'] = filtered_df.apply(lambda row: len(set(row['l2_proc']) & set(row['l2_gr'])), axis=1)
        filtered_df['l3_score'] = filtered_df.apply(lambda row: len(set(row['l3_proc']) & set(row['l3_gr'])), axis=1)
        filtered_df['brand_score'] = filtered_df.apply(lambda row: len(set(row['brand_string_mv_proc']) & set(row['brand_string_mv_gr'])), axis=1)
        filtered_df['colour_score'] = filtered_df.apply(lambda row: len(set(row['brickprimarycolor_en_string_mv_proc']) & set(row['brickprimarycolor_en_string_mv_gr'])), axis=1)
        important_entities = [
            "brickprimarycolor_en_string_mv",
            "brand_string_mv",
            "l1_category",
            "l2_category",
            "l3_category",
        ]

        filtered_df["imp_entities"] = filtered_df["query_template"].apply(lambda x: list(set(x)&set(important_entities)))
        
        #10th cell
        print("10th cell")
        filtered_df["score_alignment"] = filtered_df.apply(lambda row: len(row["imp_entities"]) == row["l1_score"]+row["l2_score"]+row["l3_score"]+row["brand_score"]+row["colour_score"], axis=1)


        #11th cell
        print("11th cell")
        failed_gr_df = filtered_df[["input_query","query_template","brickprimarycolor_en_string_mv_gr", "brickprimarycolor_en_string_mv_proc", 
                                    "brand_string_mv_gr","brand_string_mv_proc", "l1_gr", "l1_proc","l2_gr","l2_proc","l3_proc", "l3_gr","score_alignment"]]
        failed_gr_df = failed_gr_df[failed_gr_df["score_alignment"] == False]
        
        #12th cell
        print("12th cell")
        filtered_df.to_parquet(f"{self.output_path}/template_queries_with_results_scores.parquet")
        failed_gr_df.to_parquet(f"{self.output_path}/failed_queries_0602.parquet")
        
        #13th cell
        print("Final cell")
        failed_gr_df.to_csv(f"{self.output_path}/failed_queries_0902.csv")
        
    def extract_val(self,row, col):
            template_parts = row["query_template"]
            query_parts = row["query"]

            if col in template_parts:
                index = template_parts.index(col)
                # Use the same index to get the value from the query
                return query_parts[index]
            return None
        
    async def process_gr(self,new_df):
        arr = []
        for index, row in new_df[:].iterrows():
        # for index in range(0, len(mis_queries)):
        # for index in range(0, 200):
            # print(row['input_query'])
            # i = mis_queries[index]
            if(index % 500 == 0):
                print(index)
                f = open("progress.txt", "a")
                q = str(index) + '\n'
                f.write(q)
                f.close()

            i = row['input_query']
            res = await run_script(i, self.vertical, self.phase)
            print(res)
            if res == None:
                res = {}
            arr.append(res)
        return arr


if __name__ =="__main__":
    
    parser = argparse.ArgumentParser(description="GaurdrailsTesting")
    parser.add_argument('--templates_path',type=str)
    parser.add_argument('--output_path',type=str)
    parser.add_argument('--vertical',type=str)
    parser.add_argument('--phase',type=str)
    parser.add_argument('--top_n',type=int)
    
    args = parser.parse_args()
    
    print(args.templates_path)
    print(args.vertical)
    print(args.phase)
    
    TestObject = TestGaurdrails(args.templates_path, args.output_path, args.vertical, args.phase, args.top_n)
    TestObject.load()
    asyncio.run(TestObject.transform())