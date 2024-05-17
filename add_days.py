import csv
import pprint
import json

data = {}

def process_chunk(chunk):
    time = chunk[1][:10]
    try:
        wallet_time = data[chunk[0]]
        if time < wallet_time[0]:
            wallet_time[0] = time
        elif time > wallet_time[1]:
            wallet_time[1] = time
    except KeyError:
        data[chunk[0]] = [time, time]

csv_file_path = '2024-05-15-snapshot1_transactions.csv'

with open(csv_file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            pass
        process_chunk(row)
        if line_count % 500000 == 0:
            print(f"Line: {line_count}, 128,000,000")
        line_count += 1
    print(f'Processed {line_count} lines.')

csv_file_path = 'result_wallets.csv'
with open(csv_file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    with open('result_wallets_with_date.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, quotechar=',')
        for row in csv_reader:
            spamwriter.writerow([row[0], data[row[0]][0], data[row[0]][1]])