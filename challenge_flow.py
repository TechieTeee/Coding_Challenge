from prefect import Flow, task
import requests

@task
def fetch_data():
    response = requests.get('https://jsonplaceholder.typicode.com/posts')
    if response.status_code == 200:
        return response,json()
    else:
        raise Exception('Failed to successfully fetch data')

def data_processing_flow():
    with Flow("DataProcessingFlow") as flow:
        data = fetch_data
    return flow


flow = data_processing_flow()
flow.run()