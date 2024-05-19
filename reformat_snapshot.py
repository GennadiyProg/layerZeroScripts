import pandas as pd
from datetime import datetime



def process_chunk(chunk, agg_data):
    
    for index, row in chunk.iterrows():
        wallet = row['SENDER_WALLET']
        date = row['SOURCE_TIMESTAMP_UTC'][:19]
        if wallet not in agg_data:
            agg_data[wallet] = {
                'count': 0,
                'first': date,
                'last': date,
                'total': 0.0
            }

        agg_data[wallet]['count'] += 1
        

        if date < agg_data[wallet]['first']:
            agg_data[wallet]['first'] = date
        if date > agg_data[wallet]['last']:
            agg_data[wallet]['last'] = date

       
        native = row.get('NATIVE_DROP_USD', 0)
        stargate = row.get('STARGATE_SWAP_USD', 0)
        agg_data[wallet]['total'] += float(native or 0) + float(stargate or 0)


aggregated_data = {}


chunk_size = 5*(10**6)  # Можно подстроить в зависимости от объёма доступной памяти
for chunk in pd.read_csv('snapshot.csv', chunksize=chunk_size):
    process_chunk(chunk, aggregated_data)
print("Reading ended")


aggregated_df = pd.DataFrame.from_dict(aggregated_data, orient='index',
                                       columns=['count', 'first', 'last', 'total'])
print("Agregation ended")



aggregated_df.to_csv('aggregated_data.csv')
