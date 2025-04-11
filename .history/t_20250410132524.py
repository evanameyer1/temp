import warnings
import pandas as pd
import datetime
warnings.filterwarnings("ignore")

from processing import make_net_transaction_dataset
from forecasting import forecast_project
from queries.superchain_sandbox import query_transaction_data_from_bq_superchain_sandbox
from queries.defillama import query_tvl_data_from_bq

from utils import (connect_bq_client,
                   extract_addresses,
                   read_in_defi_llama_protocols, 
                   read_in_stored_dfs_for_projects,
                   save_datasets,
                   return_protocol, 
                   read_in_grants)

from config import (DEFI_LLAMA_PROTOCOLS_PATH,
                    SERVICE_ACCOUNT_PATH,
                    STORED_DATA_PATH)

# helper function to add timestamps to print statements
def print_with_timestamp(message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

projects = read_in_grants(grants_path="updated_grants_reviewed.json")
defi_llama_protocols = read_in_defi_llama_protocols(path=DEFI_LLAMA_PROTOCOLS_PATH)

grab_from_bigquery = True
failed_projects = []

cutoff_date = pd.to_datetime("2024-09-01")
today = pd.to_datetime("today").normalize()
days_since_cutoff = (today - cutoff_date).days
start_date = cutoff_date - pd.Timedelta(days=days_since_cutoff)

for project_name, project in projects.items():
    datasets = read_in_stored_dfs_for_projects(project_name, STORED_DATA_PATH, defi_llama_protocols)
    clean_name = project_name.lower().replace(" ", "_").replace(".", "-").replace("/","-")

    if 'daily_transactions' in datasets.keys():
        df = datasets['daily_transactions']
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        

        #full_date_range = pd.date_range(start=start_date, end=today)
        #df = df.set_index('transaction_date').reindex(full_date_range).fillna(0).reset_index()
        #df = df.rename(columns={'index': 'transaction_date'})

        print(f"{project_name} extended to full range: {start_date.date()} to {today.date()}")



        #df.to_csv(f"{clean_name}/{clean_name}_daily_transactions.csv")