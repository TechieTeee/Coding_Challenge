from prefect import flow, task
import requests

@task
def fetch_data():
    response = requests.get('https://jsonplaceholder.typicode.com/posts')
    if response.status_code == 200:
        return response,json()
    else:
        raise Exception('Failed to successfully fetch data')