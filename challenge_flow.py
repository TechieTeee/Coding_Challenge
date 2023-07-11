import json
import requests
import pandas as pd
from datetime import datetime
from prefect import task
from prefect import flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyarrow.parquet as pq
import pyarrow as pa
import os
from prefect.deployments import Deployment


@task
def extract(url: str) -> dict:
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.content)
    else:
        raise Exception('Failed to successfully fetch data')


@task
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for item in data:
        transformed.append({
            'UserId': item['userId'],
            'Id': item['id'],
            'Title': item['title'],
            'Body': item['body']
        })
    return pd.DataFrame(transformed)

@task
def filter_missing_values(data: dict) -> dict:
    filtered = [item for item in data if all (key in item for key in ('userId', 'id', 'title', 'body'))]
    return filtered

@task
def find_unique_posts(data: dict) -> list:
    unique_posts = []
    post_ids = set()
    for item in data:
        post_id = item['id']
        if post_id not in post_ids:
            post_ids.add(post_id)
            unique_posts.append(item)
    return unique_posts

@task
def load(data: list, path: str) -> None:
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(data)
    df.write.parquet(path, mode ='overwrite')


@task
def compress_parquet(data: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(data)
    compression = "snappy"
    pq.write_table(table, path, compression=compression)


@flow(log_prints=True)
def data_processing_flow():
    url = 'https://jsonplaceholder.typicode.com/posts'
    posts = extract(url)
    filtered_posts = filter_missing_values(posts)
    unique_posts = find_unique_posts(filtered_posts)
    df_posts = transform(unique_posts)
    directory = 'data'
    if not os.path.exists(directory):
        os.makedirs(directory)
    compressed_path = f'data/posts_{int(datetime.now().timestamp())}.parquet.snappy'
    compress_parquet(data=df_posts, path=compressed_path)
    load(data=df_posts, path=compressed_path)


def deploy():
    deployment = Deployment.build_from_flow(
        flow=data_processing_flow,
        name="Prefect Challenge Deployment"
    )
    deployment.apply()


if __name__ == "__main__":
    data_processing_flow()
    deploy()