import pandas as pd
from datetime import datetime


# Функция для обработки каждой порции данных
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

        # Для безопасности сначала проверьте, что NATIVE_DROP_USD и STARGATE_SWAP_USD могут быть преобразованы в float
        native = row.get('NATIVE_DROP_USD', 0)
        stargate = row.get('STARGATE_SWAP_USD', 0)
        agg_data[wallet]['total'] += float(native or 0) + float(stargate or 0)

# Инициализация словаря для агрегированных данных
aggregated_data = {}

# Чтение файла по частям
chunk_size = 5*(10**6)  # Можно подстроить в зависимости от объёма доступной памяти
for chunk in pd.read_csv('snapshot.csv', chunksize=chunk_size):
    process_chunk(chunk, aggregated_data)
print("Reading ended")

# Преобразование агрегированных данных в DataFrame
aggregated_df = pd.DataFrame.from_dict(aggregated_data, orient='index',
                                       columns=['count', 'first', 'last', 'total'])
print("Agregation ended")


# Сохранить агрегированный DataFrame в файл
aggregated_df.to_csv('aggregated_data.csv')
