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
def load(data: pd.DataFrame, path: str) -> None:
    data.to_parquet(path, index=False)

with Flow("DataProcessingFlow") as flow:
    posts = extract(url='https://jsonplaceholder.typicode.com/posts')
    df_posts = transform(posts)
    load(data=df_posts, path=f'data/posts_{int(datetime.now().timestamp())}.parquet')

flow.run()