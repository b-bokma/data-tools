import awswrangler as wr
import boto3
import os

session = boto3.Session()
secret_client = session.client('secretsmanager')
source_bucket_name = os.getenv('SOURCE_BUCKETNAME')
target_bucket_name = os.getenv('TARGET_BUCKETNAME')


def lambda_handler(Event, Context):
    #### Extract all paths from folder chargebee/parquet
    folders = []
    l = wr.s3.list_objects(f"s3://{source_bucket_name}/chargebee/")
    for entry in l:
        print(entry.split("/"))
        folder_name = entry.split("/")[4]
        if folder_name in folders:
            continue
        folders.append(folder_name)

    #### create table from all separate paths in list

    for folder in folders:

        df = wr.s3.read_parquet(f"s3://{source_bucket_name}/chargebee/{folder}/")
        # filter subscriptions on latest known values and remove all subscriptions if deleted
        if 'id' in df.columns:
            df['rn'] = df.groupby(['id', 'updated_at']).cumcount() + 1
        elif 'customer_id' in df.columns:
            df['rn'] = df.groupby(['customer_id', 'updated_at']).cumcount() + 1
        # this will break if there is another table with a different key. This is on purpose
        df = df[df.rn == 1]

        if 'deleted' in df.columns:
            df = df[~df.id.isin(df[df.deleted == True]['id'])]

        if 'vat_number' in df.columns:
            df['vat_number'] = df['vat_number'].astype("string")

        # write filtered data to S3
        # save subscription data to s3 bucket

        print(df.dtypes)

        wr.s3.to_parquet(
            df=df,
            path=f"s3://{target_bucket_name}/latest/chargebee/{folder}/",
            dataset=True,
            mode="overwrite",
            compression='snappy'
        )

    #### Extract exchange rates to get last known price from folder exchange_rates/

    df_rx_rates = wr.s3.read_parquet(f"s3://{source_bucket_name}/exchange_rates/")
    max_timestamp = max(df_rx_rates[~df_rx_rates['timestamp'].isna()]['timestamp'])
    df_rx_rates_latest = df_rx_rates[df_rx_rates.timestamp == max_timestamp]

    wr.s3.to_parquet(
        df=df_rx_rates_latest,
        path=f"s3://{target_bucket_name}/latest/exchange_rates/",
        dataset=True,
        mode="overwrite",
        compression='snappy'
    )
