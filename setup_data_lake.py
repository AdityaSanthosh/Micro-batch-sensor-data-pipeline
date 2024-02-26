import csv
import os
from math import ceil

from dateutil.parser import parse


def write_to_csv(rows, csv_file_path, columns):
    if not os.path.isfile(csv_file_path):
        with open(csv_file_path, 'w+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(columns)
            csv_writer.writerow(rows)
    else:
        with open(csv_file_path, 'a+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(rows)


def get_current_window(datetimeobj, window_size):
    return ceil((datetimeobj.minute + 1) / window_size)


def run():
    window_size = 10
    for filename in os.listdir('raw_files'):
        file = 'raw_files/' + filename
        with open(file, 'r') as f:
            device_name = filename[:filename.find('_')]
            reader = csv.reader(f)
            columns = list(map(lambda col: col.title(), next(reader)))
            for row in reader:
                dateTime = parse(row[0])
                day = dateTime.date()
                window = get_current_window(dateTime, window_size=window_size)
                outfile = f"{device_name}.csv"
                path = f'raw_data/{day}/{dateTime.hour}/{window}/'
                os.makedirs(name=path, exist_ok=True)
                write_to_csv(row, path + outfile, columns)


if __name__ == '__main__':
    run()
