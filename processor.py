import csv
import json
from datetime import timedelta, datetime
from multiprocessing import Pool
import pandas as pd


def get_window_range(window_number, window_duration_minutes, start_time):
    window_start_time = start_time + timedelta(minutes=window_number * window_duration_minutes)
    window_end_time = window_start_time + timedelta(minutes=window_duration_minutes)

    return window_start_time, window_end_time


def write_to_csv(rows, out):
    with open(out, 'a+', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def clean_data(df: pd.DataFrame):
    initial_no_of_rows = len(df.index)
    # data type checks
    df.Value = df.Value.astype(float)
    df.Timestamp = df.Timestamp.apply(lambda x: pd.Timestamp(x))
    df.Timestamp = df.Timestamp.dt.tz_localize('UTC')
    df.loc[~df['Device'].isin(['Device1', 'Device2', 'Device3', 'Device4', 'Device5', 'Device6']), 'Device'] = None

    # data quality metrics
    missing_values = df.Value.isna().sum()
    total_incorrect_timestamps = df.Timestamp.isna().sum()
    total_invalid_device_ids = df.Device.isna().sum()
    total_invalid_variable_names = df.Variable.isna().sum()
    dropped_rows = initial_no_of_rows - len(df.index)
    row = [[str(window_end_time), device, initial_no_of_rows, dropped_rows, missing_values, total_invalid_device_ids,
            total_incorrect_timestamps, total_invalid_variable_names]]
    write_to_csv(row, 'data_quality.csv')
    return df


# a generic function to get aggregations for custom timeframes
def get_aggregations(df, duration, unit):
    rdf = df.pivot_table(index=df.Timestamp, columns='Variable', values='Value', dropna=False)
    units = {'days': 'D', 'minutes': 'T', 'months': 'M', 'seconds': 'S'}
    for col in rdf.columns:
        rdf[col].fillna(rdf[col].rolling(window='1T', min_periods=2).mean(), inplace=True)
    rdf = rdf.resample(f'{duration}{units.get(unit)}').agg(['mean', 'min', 'max', 'std', 'last'])
    rdf.columns = [f'{col[0]}_{col[1]}' for col in rdf.columns]
    rdf = rdf.reset_index().round(2)
    return rdf


def process(args):
    file_name, timeframe = args[0], args[1]
    global device, window, hour, window_end_time

    df = pd.read_csv(file_name)
    # window data
    device = df['Device'].iloc[0]
    window = int(file_name.split('/')[-2])
    hour = int(file_name.split('/')[-3])
    date = datetime.strptime(file_name.split('/')[1] + f'_{hour}', "%Y-%m-%d_%H")
    window_start_time, window_end_time = get_window_range(window, timeframe, date)

    df = clean_data(df)
    df['Timestamp_z'] = df.Timestamp.dt.tz_convert('America/Los_Angeles')
    metrics = get_aggregations(df, duration=timeframe, unit='minutes')
    metrics['device'] = device
    metrics.to_parquet(f'analysed_data/aggregated/{device}_{window_end_time.strftime("%Y_%m_%d_%H_%M_%S")}.parquet')


def lambda_handler(event, context):
    event = json.loads(event)
    print(event)

    file_names = event['filenames']
    timeframe = int(event.get('timeframe', 10))
    file_names = list(zip(file_names, [timeframe]*len(file_names)))
    with Pool(len(file_names)) as p:
        p.map(process, file_names)

    return {
        "statusCode": 200,
        "body": json.dumps("Files processed successfully. Exiting"),
    }