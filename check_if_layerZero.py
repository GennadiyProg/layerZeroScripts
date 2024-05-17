import csv

data = []

csv_file_path = '2024-05-15-snapshot1_transactions.csv'

with open(csv_file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        data.append(row[0])
        if line_count % 500000 == 0:
            print(f"Line: {line_count}, 128,000,000")
        line_count += 1
    print(f'Processed {line_count} lines.')
data = set(data)

export_data = []
csv_file_path = 'export-0x5e809a85aa182a9921edd10a4163745bb3e36284.csv'
with open(csv_file_path) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	for row in csv_reader:
	    export_data.append(row[4])

export_data = set(export_data)
data = {v for v in export_data if v in data}

with open('result_wallets.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, quotechar=',')
    spamwriter.writerow(data)