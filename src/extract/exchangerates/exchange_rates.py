import awswrangler as wr
import os

source_bucket_name = os.getenv('SOURCE_BUCKETNAME')
target_bucket_name = os.getenv('TARGET_BUCKETNAME')

def lambda_handler(Event, Context):

    df = wr.s3.read_json(
        path=Event['Body']['path'],
        lines=True,
        convert_dates=False,  # dont convert columns to dates
        convert_axes=False
    )

    print(df)

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{target_bucket_name}/exchange_rates/",
        mode='append',
        dataset=True
    )