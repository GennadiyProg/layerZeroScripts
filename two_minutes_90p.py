import csv
import json
from datetime import datetime, timedelta

data = {}

def process_chunk(chunk):
    try:
        wallet = data[chunk[6]]
        wallet[0].append(chunk[7])
        wallet[1] += 1
    except KeyError:
        data[chunk[6]] = [[chunk[7]], 1]

csv_file_path = '2024-05-15-snapshot1_transactions.csv'

with open(csv_file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            process_chunk(row)
            if line_count % 500000 == 0:
                print(f"Line: {line_count}, 128,000,000")
            line_count += 1
    print(f'Processed {line_count} lines.')

data2 = {k: [sorted(v[0]), v[1]] for k, v in data.items() if v[1] > 2}

# Address filtering function (10% of the intervals between transactions are more than 2 minutes)
def filter_addresses_by_short_intervals(data, threshold_minutes=2, percentage=0.1):
    threshold = timedelta(minutes=threshold_minutes)
    filtered_data = {}

    for address, (timestamps, count) in data.items():
        times = [datetime.fromisoformat(ts) for ts in timestamps]
        intervals = [times[i+1] - times[i] for i in range(len(times) - 1)]

        # Counting the number of intervals less than the specified threshold
        short_intervals = sum(1 for interval in intervals if interval < threshold)

        # Check whether the proportion of short intervals corresponds to the specified percentage
        if short_intervals / len(intervals) >= percentage:
            filtered_data[address] = [timestamps, count]

    return filtered_data

# Applying the filter
data2 = filter_addresses_by_short_intervals(data2)

print(f"Length: {len(data)}")

with open('result.txt', 'w') as convert_file:
    convert_file.write(json.dumps(data2, indent=4))
