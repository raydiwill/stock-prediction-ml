"""Hello World DAG - Phase 1: Learn Airflow basics.

Your first DAG to understand:
    1. How DAGs are defined (the @dag decorator)
    2. How tasks are defined (the @task decorator)
    3. How tasks pass data to each other (XCom via return values)
    4. How dependencies work (>> operator)
    5. How scheduling works (schedule parameter)

Run without scheduler:
    $ airflow dags test hello_world 2024-01-01

Then try from the UI at localhost:8080.

Resources:
    - TaskFlow tutorial: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
    - DAG concepts: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
"""

from datetime import datetime

from airflow.sdk import dag, task


# --- DAG Definition ---
@dag(
    schedule=None,  # schedule
    start_date=datetime(2025,2,24),  # start_date
    catchup=False,  # catchup
    tags=["example DAG"],  # tags
)
def hello_world():
    """A minimal DAG to verify your Airflow setup works."""

    # --- Task 1: Greet ---
    # This task should:
    #   - Print a greeting message with the current timestamp
    #   - Return a dict with a "message" key (this goes to XCom automatically)
    @task()
    def greet():
        message = "Hello world!"
        print(message)
        return {"message": message}

    # --- Task 2: Report ---
    #   - Accepts the output of greet() as a parameter
    #   - Prints the received message
    #   - Returns a summary string
    @task()
    def report(greeting_data):
        message = greeting_data.get("message")
        print(f"Received: {message}")
        return f"Printed {message}"

    # --- Wire Dependencies ---
    result = greet()
    report(result)


# --- Instantiate the DAG ---
hello_world()
