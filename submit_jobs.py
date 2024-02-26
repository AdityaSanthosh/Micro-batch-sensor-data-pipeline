import json
from pathlib import Path

import arrow

import processor


def generate_all_files(root: Path, only_files: bool = True):
    for p in root.rglob("*"):
        if only_files and not p.is_file():
            continue
        yield p


def get_date_range(start_date, end_date):
    start_date = arrow.get(start_date, 'YYYY-MM-DD')
    end_date = arrow.get(end_date, 'YYYY-MM-DD')
    return [dt.format('YYYY-MM-DD') for dt in arrow.Arrow.range('day', start_date, end_date)]


def get_all_files(start_date, end_date, batch_size):
    for date in get_date_range(start_date, end_date):
        day_path = f"raw_data/{date}/"
        for i in range(24):
            hour_path = day_path + f"{i}/"
            for j in range(1, batch_size):
                window_path = hour_path + f"{j}/"
                yield [str(filepath) for filepath in generate_all_files(Path(window_path))]


if __name__ == '__main__':
    no_of_threads = 6
    startDate = '2020-07-01'
    endDate = '2020-07-06'
    for batch in get_all_files(startDate, endDate, batch_size=no_of_threads):
        if not batch:
            continue
        event = {
            "filenames": batch,
            "timeframe": 10
        }
        processor.lambda_handler(json.dumps(event), None)
