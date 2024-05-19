import pandas as pd
from pathlib import Path


df = pd.read_csv('10tx_filtered.csv')


df['count_min'] = df['count'] * 0.9
df['count_max'] = df['count'] * 1.1


grouped = df.groupby(['first', 'last'])


output_dir = Path('matching_groups')
output_dir.mkdir(exist_ok=True)


for (first, last), group in grouped:
    if len(group) > 19:
        group_min = group['count'].min()
        group_max = group['count'].max()
        
        
        if all(group_min >= group['count_min']) and all(group_max <= group['count_max']):
            
            filename = f"{first}_{last}.csv"
            
            group.to_csv(output_dir / filename, index=False)
