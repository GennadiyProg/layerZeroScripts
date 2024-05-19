import pandas as pd
import numpy as np

def optimize_memory_usage(df):
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    for col in df.select_dtypes(include=['object']).columns:
        num_unique_values = len(df[col].unique())
        num_total_values = len(df[col])
        if num_unique_values / num_total_values < 0.5:
            df[col] = df[col].astype('category')
    return df


file1_path = 'path/to/your/first_file.csv'
df1 = pd.read_csv(file1_path)
df1 = optimize_memory_usage(df1)


required_columns_df1 = ['SENDER_WALLET', 'NATIVE_DROP_USD', 'STARGATE_SWAP_USD']
if all(column in df1.columns for column in required_columns_df1):

    wallet_transactions = df1.groupby('SENDER_WALLET')[['NATIVE_DROP_USD', 'STARGATE_SWAP_USD']].sum()


    wallet_transactions['TOTAL_SUM_USD'] = wallet_transactions['NATIVE_DROP_USD'] + wallet_transactions['STARGATE_SWAP_USD']


    wallet_transactions = wallet_transactions.reset_index()


    output_file_path = 'merged_file.csv'


    chunk_size = 10**6  
    file2_path = 'path/to/your/second_file.csv'


    chunks = pd.read_csv(file2_path, chunksize=chunk_size)
    for i, chunk in enumerate(chunks):
        chunk = optimize_memory_usage(chunk)
        chunk = chunk.rename(columns={'Unnamed: 0': 'SENDER_WALLET'})
        chunk = chunk.merge(wallet_transactions, on='SENDER_WALLET', how='left')
        chunk['total'] = chunk['total'].fillna(0) + chunk['TOTAL_SUM_USD'].fillna(0)

        if i == 0:
            chunk.to_csv(output_file_path, index=False, mode='w')
        else:
            chunk.to_csv(output_file_path, index=False, mode='a', header=False)
