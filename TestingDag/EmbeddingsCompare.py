import warnings
import numpy as np
import pandas as pd
import shutil
import secrets
import stat
import sys
from pathlib import Path
from pyarrow import fs
from pyarrow import csv as csv
import numpy as np
import os
import gc
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from sklearn.preprocessing import normalize
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

os.environ["LIBHDFS_OPTS"] = (
            "-Djava.security.krb5.conf=/home/jioapp/aditya/jiomart_cluster/krb5.conf"
        )

class EmbeddingsCompare:
    def __init__(self, model_path, descriptions_path, output_path_embeddings, product_desc_col, product_desc_col2,n, model_name="model", batch_size=1024, step_size=100000, multi_gpu=True):
        
        # print( model_path, descriptions_path, output_path_embeddings, product_desc_col, product_desc_col2, model_name, batch_size, step_size, multi_gpu)
        
        self.model_path = model_path
        self.descriptions_path = descriptions_path
        self.output_path_embeddings = output_path_embeddings
        self.product_desc_col = product_desc_col
        self.product_desc_col2 = product_desc_col2
        self.model_name = model_name
        self.batch_size = batch_size
        self.step_size = step_size
        self.multi_gpu = multi_gpu
        self.temp_dir = Path("/app/notebooks/avinash/temp_dir")
        self.hdfs = fs.HadoopFileSystem(host='10.144.96.170', port=8020, kerb_ticket="/home/jioapp/aditya/jiomart_cluster/krb5cc_154046")
        self.lfs = fs.LocalFileSystem()
        self.sampling_fraction = int(n)
        self.product_descriptions = None
        
        self.bad_hdfs_path = output_path_embeddings.split(".")[0]+"badQueries.csv"
        
        print(self.bad_hdfs_path)

    def compare_embdeddings(self):
        print("Comparing embeddings")

        def cosine_similarity(vec_a, vec_b):
            dot_product = np.dot(vec_a, vec_b)
            norm_a = np.linalg.norm(vec_a)
            norm_b = np.linalg.norm(vec_b)
            return dot_product / (norm_a * norm_b)

        # Compute cosine similarities
        first_vectors = self.product_descriptions[self.product_desc_col + "vector"].tolist()
        second_vectors = self.product_descriptions[self.product_desc_col2 + "vector"].tolist()
        similarities = [cosine_similarity(first, second) for first, second in zip(first_vectors, second_vectors)]
        self.product_descriptions["similarity"] = similarities

        # Container for all reports
        all_reports = []

        # Category-wise reports
        if "category" in self.product_descriptions.columns:
            print(self.product_descriptions.columns)
            grouped = self.product_descriptions.groupby(["category"])
            for cat, group in grouped:
                stats = group["similarity"].describe().to_frame().T
                stats.insert(0, "category", cat)
                all_reports.append(stats)

        # Overall report
        overall_stats = self.product_descriptions["similarity"].describe().to_frame().T
        overall_stats.insert(0, "category", "All")
        all_reports.append(overall_stats)

        # Combine and export
        final_report_df = pd.concat(all_reports, ignore_index=True)
        report_path = self.temp_dir / "report.csv"
        final_report_df.to_csv(report_path, index=False)
        self.load_file_to_hdfs(report_path.as_posix(), self.output_path_embeddings)

        print(f"Embeddings compared and saved report to {self.output_path_embeddings}")

        # Save bad queries
        print("Calculating bad queries")
        bad_queries = self.product_descriptions[self.product_descriptions["similarity"] < 0.9]
        bad_queries_path = self.temp_dir / "bad_queries.csv"
        bad_queries.to_csv(bad_queries_path, index=False)
        print(f"saving bad queries at {self.bad_hdfs_path}")
        self.load_file_to_hdfs(bad_queries_path.as_posix(), self.bad_hdfs_path)

        
    def check_file_type(self,filesystem,input_path):
        return ["parquet",input_path]


    def generate_vectors_from_list(self, str_list, model, batch_size=32, multi_gpu=False):
        if multi_gpu:
            # query_vectors = model.encode_multi_process(str_list, self.multi_process_pool, show_progress_bar=True, chunk_size=2*batch_size)
            query_vectors = model.encode_multi_process(str_list, self.multi_process_pool,chunk_size=2*batch_size)
        else:
            query_vectors = model.encode(str_list, batch_size=batch_size)
        query_vectors = normalize(query_vectors)
        return query_vectors.tolist()

    def set_args(self):
        self.model_path = Path(self.model_path)

    def load_model_to_local(self):

        # Download JSON model
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir(parents=True)

        self.lc_model_path: Path = self.temp_dir / self.model_path.name

        # Copy the model file to the local filesystem
        fs.copy_files(
            source=self.model_path.as_posix(),
            destination=self.lc_model_path.as_posix(),
            source_filesystem=self.hdfs,
            destination_filesystem=self.lfs
        )

        self.lc_model_path.chmod(stat.S_IRWXU)
        print("model loaded")
        
    def load_file_to_hdfs(self,local_path,hdfs_path):

        # Copy the model file to the local filesystem
        fs.copy_files(
            source=local_path,
            destination=hdfs_path,
            source_filesystem=self.lfs,
            destination_filesystem=self.hdfs
        )
        print("sent to hdfs")

    def load(self):
        print("starting loading ")
        
        if not os.path.exists(self.temp_dir.as_posix() / Path("stella_v3_e3_ajio")):
            
            print("loading model into local")
        
            # Load the modeluration if it exists
            self.load_model_to_local()
            
        else:
            self.lc_model_path = self.temp_dir / Path("stella_v3_e3_ajio")
            
        print("model saved loaded")

        input_path = self.descriptions_path
        
        print(f"The input path is {input_path}")
        
        file_extension,data_paths = self.check_file_type(self.hdfs, input_path)

        if file_extension == "csv":

            # Load the input data from CSV files , if multiple files are present in the directory or if single file just load it
            self.df = pd.concat([csv.read_csv(self.hdfs.open_input_file(path)).to_pandas() for path in data_paths], ignore_index=True)[:self.sampling_fraction]

        elif file_extension == "parquet":
            # Load the input data from Parquet file
            self.df: pd.DataFrame = pq.ParquetDataset(input_path, self.hdfs).read().to_pandas()[:self.sampling_fraction]
            
        else:
            raise ValueError("Unsupported file type. Only CSV and Parquet files are supported.")
            
        self.product_descriptions = self.df

        print("Input Data loaded")
        
        self.model = SentenceTransformer(
            self.lc_model_path.as_posix(), 
            trust_remote_code=True, 
            # model_kwargs = {"use_memory_efficient_attention": False, "unpad_inputs": False}
            )

        print("Model loaded")

        if self.multi_gpu == "True":
            self.multi_gpu = True
            self.multi_process_pool = self.model.start_multi_process_pool()

    def transform(self):

        # if not os.path.exists(self.output_path_embeddings): os.makedirs(self.output_path_embeddings)
        
        print("Generating Embeddings")

        num_steps = len(self.product_descriptions) // self.step_size + 1
        logging.info(f"total parts = {num_steps}")

        
        final_df = pd.DataFrame()

        for index, i in enumerate(range(0, len(self.product_descriptions) // self.step_size + 1)):

            final_df_part = pd.DataFrame()

            for col in [self.product_desc_col, self.product_desc_col2]:
                logging.info(f"on step {index} of {num_steps}")
                step_df = self.product_descriptions[i * self.step_size: (i + 1) * self.step_size]
                s2p_prompt = "Instruct: Given a web search query, retrieve relevant passages that answer the query.\nQuery: "
                if "query" in col or "queries" in col:
                    step_df[col] = step_df[col].apply(lambda x: s2p_prompt + x)
                # Generate vectors using the provided product description column name
                vectors = self.generate_vectors_from_list(step_df[col].tolist(), self.model, batch_size=self.batch_size, multi_gpu=self.multi_gpu)
                step_df[col+"vector"] = vectors
                logging.info(f"the length of df is: {len(step_df)}")

                if(index==0):
                    print(step_df)
                step_df[col] = step_df[col].apply(lambda x: x[len(s2p_prompt):] if x.startswith(s2p_prompt) else x)

                if "query" in col or "queries" in col:
                    step_df = step_df.drop(col, axis=1, errors='ignore')
                    step_df = step_df.reset_index(drop=True)
                
                if final_df_part.empty:
                    final_df_part = step_df.copy()
                else:
                    # Avoid duplicating shared columns like 'category' or 'product_id'
                    non_overlap_cols = [col for col in step_df.columns if col not in final_df_part.columns]
                    final_df_part = pd.concat([final_df_part, step_df[non_overlap_cols]], axis=1)


                if(index==0):
                    print(step_df)

            final_df = pd.concat([final_df, final_df_part], axis=0)

        self.product_descriptions = final_df

        self.compare_embdeddings()

        logging.info("Embeddings generated and saved.")

    def save(self):
        logging.info(f"Embeddings saved to {self.output_path_embeddings}.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Product Embedding Generator")
    parser.add_argument('--model_path', type=str, required=True, help="Path to the model")
    parser.add_argument('--query_file_path', type=str, required=True, help="Path to product query parquet file")
    parser.add_argument('--report_csv_path', type=str, required=True, help="Output path to save comparision report")
    parser.add_argument('--query_col1', type=str, required=True, help="Column name for original query")
    parser.add_argument('--query_col2', type=str, required=True, help="Column name for modified query")
    parser.add_argument('--model_name', type=str, help="Name of the model")
    parser.add_argument('--batch_size', type=int, help="Batch size for processing query")
    parser.add_argument('--step_size', type=int, help="Step size for chunking  query")
    parser.add_argument('--multi_gpu', type=str, help="whether to use multi gpu query")
    parser.add_argument('--n', type=str, help="no of queries to compare")
    

    args = parser.parse_args()

    generator = EmbeddingsCompare(
        model_path=args.model_path,
        descriptions_path=args.query_file_path,
        output_path_embeddings=args.report_csv_path,
        product_desc_col=args.query_col1,
        product_desc_col2=args.query_col2,
        n=args.n,
        model_name=args.model_name,
        batch_size=args.batch_size,
        step_size=args.step_size,
        multi_gpu=args.multi_gpu
    )

    generator.set_args()
    generator.load()
    generator.transform()
    generator.save()
