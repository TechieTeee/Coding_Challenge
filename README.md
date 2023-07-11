# Coding Challenge
ETL pipeline with Prefect, PySpark, PyArrow, and Python with json placeholder data

## How to Run Code
Run `git clone https://github.com/TechieTeee/Coding_Challenge.git or download code` in your terminal.

Run `cd Coding_Challenge` in your terminal.

## Install all necessary packages
Run `pip install requests pandas prefect pyspark pyarrow` in your terminal.

## Load Your Prefect Credentials ##
If you don't already have a Prefect account, go to the Prefect website https://www.prefect.io/ and create an account. Then, go to your profile (circle in the lower left
corner of your screen) and then navigate to "API Keys", and create an API key. Store it in a secure place. You will need this to access the Prefect API and run the code.
Then run `prefect cloud login` in your terminal and paste in your API key.

## Running the Flow
Now, navigate to the challenge_flow.py file and run it with `python ./challenge_flow.py`. Once the flow successfully runs, it will output a message in the terminal.
