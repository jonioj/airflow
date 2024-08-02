from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Define your parent DAG
parent_dag_id = 'stock_market_job'

# Define the target DAG you want to trigger
target_dag_id = 'stock_market'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# List of stocks to trigger the target DAG for
stocks = [
    "AAPL", "MSFT", "AMZN", "GOOG", "GOOGL", "FB", "TSLA", "BRK.B", "JPM",
    "NFLX", "NVDA", "V", "MA", "DIS", "PYPL", "AMD", "INTC", "CSCO", "ORCL",
    "ADBE", "CRM", "T", "KO", "PFE", "MRK", "ABT", "WMT", "HD", "UNH", "MCD",
    "XOM", "CVX", "C", "BAC", "WFC", "USB", "AMGN", "GILD", "BIIB", "CELG",
    "ALGN", "LLY", "TMO", "DHR", "MCK", "HUM", "BMY", "VRTX", "NKE", "SBUX",
    "LOW", "BKNG", "PEP", "TGT", "CCL", "DAL", "AAL", "LUV", "UAL", "RBLX",
    "SQ", "SNAP", "TWTR", "ZM", "SHOP", "ETSY", "DOCU", "VEEV", "MDB", "OKTA",
    "HUBS", "PAYC", "PLTR", "FSLY", "TWLO", "YELP", "CRWD", "ZS", "NET",
    "EVGO", "LSCC", "HPE", "TXN", "INGR", "CARR", "DUK", "AEP", "NEE", "XEL",
    "MRO", "PXD", "COP", "EOG", "HAL", "SLB", "MUR"
]
with DAG(dag_id=parent_dag_id,
         default_args=default_args,
         schedule_interval='@once',
         start_date=days_ago(1),
         tags=['example']) as dag:

    for stock in stocks:
        # Define a TriggerDagRunOperator for each stock
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_target_dag_for_{stock}',
            trigger_dag_id=target_dag_id,  # This is the DAG to be triggered
            # You can also specify execution date
            conf={"stock": stock},  # Pass parameters to the triggered DAG
        )
