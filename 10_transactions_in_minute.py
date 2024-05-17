import csv
import json
from datetime import datetime, timedelta

data = {}

def process_chunk(chunk):
    try:
        wallet = data[chunk[0]]
        wallet[0].append(chunk[1])
        wallet[1] += 1
    except KeyError:
        data[chunk[0]] = [[chunk[1]], 1]

csv_file_path = '2024-05-15-snapshot1_transactions.csv'

with open(csv_file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        process_chunk(row)
        if line_count % 500000 == 0:
            print(f"Line: {line_count}, 128,000,000")
        line_count += 1
    print(f'Processed {line_count} lines.')

data2 = {k: [sorted(v[0]), v[1]] for k, v in data.items() if v[1] > 2}

def filter_addresses_by_short_intervals(data, threshold_minutes=1, min_transactions=10):
    line_count = 0
    threshold = timedelta(minutes=threshold_minutes)
    filtered_data = {}

    for address, (timestamps, count) in data.items():
        times = [datetime.fromisoformat(ts) for ts in timestamps]
        n = len(times)
        for i in range(n):
            start_time = times[i]
            end_time = start_time + threshold
            count = 0
            for j in range(i, n):
                if times[j] < end_time:
                    count += 1
                else:
                    break
            if count >= min_transactions:
                filtered_data[address] = [timestamps, count]

        if line_count % 500000 == 0:
            print(f"Line by_short_intervals: {line_count}")
        line_count += 1
    return filtered_data

data2 = filter_addresses_by_short_intervals(data2)

with open('res_short_intervals_one_minute.txt', 'w') as convert_file:
    convert_file.write(json.dumps(data2, indent=4))