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

        # List of key columns to check for activity
        activity_cols = [
            'transaction_cnt',
            'active_users',
            'total_transferred',
            'gas_fee',
            'unique_users',
            'cum_transferred'
        ]

        # Find the first date where at least one of the columns is non-zero
        non_zero_mask = (df[activity_cols] != 0).any(axis=1)
        if non_zero_mask.any():
            first_valid_index = non_zero_mask.idxmax()
            first_valid_date = df.loc[first_valid_index, 'transaction_date']
            df = df[df['transaction_date'] >= first_valid_date].reset_index(drop=True)
            print(f"{project_name} trimmed to start at {first_valid_date.date()}")
        else:
            print(f"{project_name} has no non-zero rows, skipping...")

        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])

        df.to_csv(f"{clean_name}/{clean_name}_daily_transactions.csv", index=False)