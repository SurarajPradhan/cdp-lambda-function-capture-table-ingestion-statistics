import boto3
import pandas as pd
from datetime import datetime
import uuid


def append_slash_if_missing(input_string):
    if not input_string.endswith('/'):
        input_string += '/'
    return input_string


def update_table_load_statistics(ingested_tables, metadata_path, ingestion_date):
    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(ingested_tables)

    # Add an additional ingestion date column
    processedDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    df['IngestionDate'] = ingestion_date
    df['ProcessedDate'] = processedDate

    # Generate a unique ID using UUID
    unique_id = str(uuid.uuid4())
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    metadata_filename = f"{metadata_path}year={year}/month={month}/day={day}/metadata_{unique_id}_{timestamp}.parquet"
    # Save the DataFrame to a Parquet file
    df.to_parquet(metadata_filename, index=False)


def lambda_handler(event, context):
    # Extract parameters from the event context
    source_bucket = event.get('source_bucket')
    ingestion_date = event.get('ingestion_date')
    ingested_tables = event.get('ingested_tables')

    try:
        table_load_statistics_path = append_slash_if_missing(
            f"s3://{source_bucket}/metadata/loadtype=bulkload/")
        update_table_load_statistics(ingested_tables, table_load_statistics_path, ingestion_date)

        return {
            'statusCode': 200,
            'body': f'Tables statistics updated.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
