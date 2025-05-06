import pandas as pd
import requests
import aiohttp
import asyncio
import time
import os
import pyarrow.parquet as pq
from pyarrow import fs
import pyarrow as pa
import pandas as pd
import re
import argparse

# internal python imports
from conf import input_parameters
from conf import all_endpoints


class TestGaurdrails():
    
    def __init__(self,brands_path,endpoint,disable_spell_check):
        self.brands_path=brands_path
        self.required_count = 30
        self.api_endpoint = all_endpoints[endpoint]
        self.topk=20
        self.disable_spell_check = self.check_spell_check(disable_spell_check)
        self.fs = pa.hdfs.connect()
        self.lfs = pa.LocalFileSystem()
        
    def load(self):
        print(f"loading the data from {self.brands_path}")
        self.bdf = pq.ParquetDataset(self.brands_path,self.fs).read().to_pandas()
        self.bdf = self.bdf[self.bdf['distinct_products'] > 25].sort_values(by=['distinct_products'], ascending=[False])
        
    def transform(self):
        print("processing top")
        self.top = self.process_with_n(self.bdf, 'brandname', 'brandName_text_en_mv',self.required_count, True, False, False)
        print("processing_mid")
        self.mid = self.process_with_n(self.bdf, 'brandname', 'brandName_text_en_mv',self.required_count, False, True, False)
        print("processing_least")
        self.least = self.process_with_n(self.bdf, 'brandname', 'brandName_text_en_mv',self.required_count, False, False, True)
        
        
    def save(self):
        top_df = pd.DataFrame.from_dict(self.top)
        mid_df = pd.DataFrame.from_dict(self.mid)
        least_df = pd.DataFrame.from_dict(self.least)
        final_df = pd.concat([top_df,mid_df,least_df],axis=0)
        final_df.to_csv("/app/notebooks/avinash/Gaurdrails_task/output/Brand_Recognition_output.csv")
        
    def check_spell_check(self,disable_spell_check):
        if disable_spell_check.lower()=="true": 
            return True 
        elif disable_spell_check.lower()=="false" :
            return False 
        else:
            print("wrong value for disable_spell_check running for True")
            return True

    def process_with_n(self, df, df_key, key_in_api, n = None, top = True, mid = True, least = True):        
        # checks the top K products to have the same key

        if n is None:
            print(f"Processing all {key_in_api}: -----\n")
            all_entries = df[df_key].to_list()
            failed = self.process_all(self,all_entries, key_in_api)
            return failed

        # process all N
        required_count = n

        count = df.shape[0]

        t = df[:required_count][df_key].to_list()
        l = df.tail(n)[df_key].to_list()
        m = df[count//2 - required_count//2: count//2 + required_count//2][df_key].to_list()

        f1 = []
        f2 = []
        f3 = []

        if top == True:
            print(f"\nTop Selling {key_in_api}: ----------")
            f1 = self.process(t, key_in_api)

        if mid == True:
            print(f"\nMid Selling {key_in_api}: ----------")
            f2 = self.process(m, key_in_api)


        if least == True:
            print(f"\nLeast Selling {key_in_api}: ----------")
            f3 = self.process(l, key_in_api)

        final_failed = f1 + f2 + f3
        return final_failed


    def process_all(self, arr, name):
        count = 0
        failed = []
        for index in range(0, len(arr)):
            query = arr[index]
            print(index, query)
            res, not_matching, len_missing = self.check_query(query, name)
            if res:
                count += 1
            else:
                 # write_row(index=index, query=query)
                q = {'query': query, 'additional_entries': not_matching, "missed_count": len_missing, "total_count": self.topk }
                failed.append(q)
        print(f"Score: {count}/{len(arr)}")   
        return failed


    def process(self, arr, name):
        count = 0
        all_queries = []
        for query in arr:
            # if(query in ["pinkvillejaipur", "trezora", "manuprink", "mac duggal", "selrov"]):
            #     continue
            res, not_matching, len_missing, matching, len_matching = self.check_query(query, name)
            q = {'query': query,
                 'not_matching_brands': not_matching,
                 "not_matching_count": len_missing,
                 "matchin_brands":matching,
                 "matching_count":len_matching,
                 "total_count": self.topk,
                 "Brand_Recognition_Result":res
                }
            all_queries.append(q)
        print(f"Score: {len_matching}/{len(arr)}")    
        return all_queries



    def check_query(self,query, name):
        res = self.api_call(query, name)

        res = res[:self.topk]

        # Normalize query for comparison
        normalized_query = self.normalize_string(query)
        matching = [entry for entry in res if self.normalize_string(entry) == normalized_query]
        not_matching = [entry for entry in res if self.normalize_string(entry) != normalized_query]

        result = len(not_matching) == 0  
        if result: 
            print(f"Query: {query} -> type: {name} passed ✅")
        else:
            print(f"Query: {query} -> type: {name} failed ❌\n")

        if not_matching:
            print(f"Matching: {len(matching)}/{self.topk}")
            print(f"Not Matching: {len(not_matching)}/{self.topk}")
            print(f"Additional entries not matching: {set(not_matching)}\n")
        return result, list(set(not_matching)), len(not_matching), list(set(matching)), len(matching)

    def normalize_string(self, s):
        """
        Remove all spaces and symbols from the string and convert it to lowercase.
        """
        return re.sub(r'[^a-zA-Z0-9]', '', s).lower()


    def api_call(self, query, to_check):
        query_to_test = query
        input_parameters["query"] = query_to_test
        input_parameters["disable_spell_check"] = self.disable_spell_check
        
        while True:
            try:
                response = requests.post(url=self.api_endpoint, json=input_parameters)
                if response.status_code == 500:
                    print("Request failed with status code 500. Retrying in 3 seconds...")
                    time.sleep(3)
                    continue
                response.raise_for_status()  # Raise other HTTP errors if present
            except requests.exceptions.HTTPError as e:
                print("API Error:", e)
                return []
            except requests.exceptions.RequestException as e:
                print("Request Error:", e)
                return []
            else:
                response_json = response.json()
                vals = [entry[to_check][0] for entry in response_json.get("docs", [])]
                # print(vals)
                return vals

            
if __name__ =="__main__":
    
    parser = argparse.ArgumentParser(description="GaurdrailsTesting")
    parser.add_argument('--brands_path',type=str)
    parser.add_argument('--endpoint',type=str)
    parser.add_argument('--disable_spell_check',type=str)
    args = parser.parse_args()
    
    brands_path=args.brands_path
    endpoint=args.endpoint
    disable_spell_check=args.disable_spell_check
    
    print(brands_path,endpoint,disable_spell_check)
    
    TestObject = TestGaurdrails(brands_path,endpoint,disable_spell_check)
    TestObject.load()
    TestObject.transform()
    TestObject.save()