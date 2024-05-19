import pandas as pd

# Загрузите данные из CSV файла в pandas DataFrame
df = pd.read_csv('data.csv')

# Удалите строки, где значение в столбце 'count' меньше 10.
filtered_df = df[df['count'] >= 10]

# Сохраните отфильтрованные данные обратно в CSV.
filtered_df.to_csv('filtered_data.csv', index=False)