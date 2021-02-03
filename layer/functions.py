from datetime import timedelta, datetime
import requests
import awswrangler as wr
import pandas as pd
import numpy as np
import os


def list_data(endpoint, url, token, last_updated=None):
    """
    A function to list all data from a specific endpoint from Chargebee.
    Returns object with all results. Based to be processed to a list using comprehension like:
    [x['endpoint'] for x in list_data(endpoint,url,token)]

    last_updated has the options of:
    None: default behavior returns the data updated since yesterday 00:00:00
    Timestamp: int (timestamp in seconds)
    'All': do not limit on date
    """

    output = []
    offset = ""


    # set last_updated to change the filter date. By default set to yesterday 00.00.00
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_timestamp = datetime(
        year=yesterday.year,
        month=yesterday.month,
        day=yesterday.day,
        hour=0,
        minute=0,
        second=0).utcnow().timestamp()

    while offset is not None:

        if last_updated is None:
            last_updated = int(yesterday_timestamp)

        if str(last_updated).lower() == 'all':
            params = {
                "include_deleted": "true",
                "offset": offset,
                "sort_by[desc]": "updated_at"
            }
        else:
            params = {
                "include_deleted": "true",
                "offset": offset,
                "sort_by[desc]": "updated_at",
                "updated_at[after]": str(last_updated)
            }
        print(params)
        result = requests.get(
            url=f"https://{url}.chargebee.com/api/v2/{endpoint}",
            auth=(f"{token}", ""),
            params=params
        )
        r = result.json()

        if 'list' not in r:
            print(r)

        for row in r['list']:
            output.append(row)

        if 'next_offset' in r.keys():
            offset = r['next_offset']

        else:
            offset = None
            continue

    return output


def extract_nested_lists(input_list, keyname, keys_to_add=[{"id":"subscription_id"},{"updated_at":"updated_at"}]):
    """
    A function to extract nested lists from an input list. 
    with keys_to_add you can pass a list with dicts with from to to, so you can rename certain column names
    """
    if len(input_list) > 0:
        output = []
        for item in input_list:
            if keyname in item.keys():
                output_obj = item.pop(keyname)
                if isinstance(output_obj, list) is False:
                    output_obj = [output_obj]
                for j in output_obj:
                    for key_to_add in keys_to_add:
                        for k, v in key_to_add.items():
                            j[v] = item[k]
                    output.append(j)
        return output 
    else:
        return "Passed an empty list"



def load_list_to_s3(bucket_name, input_list, list_name, date_columns, instance):
    """
    With this function you load a list, create a dataframe from it.
    Based on the list of column names in date_columns these are set to timestamp.
    The dataframe is set to parquet and sent to s3
    """
    if len(input_list) > 0:
        df = pd.DataFrame(input_list)

        filepath = f"s3://{bucket_name}/chargebee/{list_name}/entity={instance}/"

        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], unit='s')

        f = wr.s3.to_parquet(
            df=df,
            path=filepath,
            dataset=True,
            mode="append",
            compression='snappy'
            )

        return f


def col_to_str(df,colname):
    if colname in df.columns:
        df[colname] = df[colname].astype("string")


def col_to_timestamp(df, colname, timezone='UTC'):
    """
    shitload of print statements, since this is a place where I found lots of issues.
    Will be removed it this proves itself.
    """
    if colname in df.columns:
        print("start")
        print(colname, df[colname].dtypes)
        print(df[colname].head())
        if np.issubdtype(df[colname].dtype, np.datetime64) == False:  # skips columns that are already datetime64
            try:
                df[colname] = df[colname].astype('Int64')
                df[colname] = pd.to_datetime(
                    df[colname],
                    unit='s',
                    errors='coerce',
                    utc=True)
                print(df[colname].head())
                df[colname] = pd.DatetimeIndex(df[colname]).tz_convert(timezone)
                print(df[colname].head())
            except:
                print(colname, df[colname].dtypes)

        print(df[colname].head())

        # replace all values that are a digit

        df[colname] = pd.to_datetime(df[colname])
        print(df[colname].head())
        df[colname] = df[colname].dt.tz_localize(None)
        print(df[colname].head())
        print("done")
