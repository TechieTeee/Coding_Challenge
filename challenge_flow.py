import json
import requests
import pandas as pd
from datetime import datetime
from prefect import task, Flow


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
def load(data: pd.DataFrame, path: str) -> None:
    data.to_parquet(path, index=False)

with Flow("DataProcessingFlow") as flow:
    posts = extract(url='https://jsonplaceholder.typicode.com/posts')
    filtered_posts = filter_missing_values(posts)
    unique_posts = find_unique_posts(filtered_posts)
    df_posts = transform(filtered_posts)
    load(data=df_posts, path=f'data/posts_{int(datetime.now().timestamp())}.parquet')

flow.run()