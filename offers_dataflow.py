import argparse
import os
from datetime import date, datetime
import google_crc32c
import psycopg2
from google.cloud import secretmanager
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.transforms.util import CoGroupByKey
import urllib.parse

# Function to access secrets from Secret Manager
def access_secret_version(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Data corruption detected.")
    return response.payload.data.decode("UTF-8")
'''
def access_secret_version(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
'''
output_offer_metadata_path = "gs://outfiles_parquet/offer_bank/offer_result/offer_metadata.json" 

# Function to fetch offer metadata 
def fetch_offer_metadata(jdbc_url, jdbc_user, jdbc_password):
    query = """
        select o.offer_id, o.offer_source, o.start_datetime, o.end_datetime, o.time_zone, o.discount_type, o.discount_value,
        o.applicable_channel, o.club_list, o.labels, oi.item_number, oi.product_id, oi.item_type, mo.membership_id,
        oc.club_number as exclusive_club_number, oc.start_datetime as exclusive_club_startdate, oc.end_datetime as exclusive_club_enddate
        from public.offers o
        left join public.member_offers mo on o.offer_id = mo.offer_id
        left join public.offer_items_v2 oi on o.offer_id = oi.offer_id
        left join public.club_overrides oc on o.offer_id = oc.offer_id
        where o.start_datetime <= now() and o.end_datetime >= now() and o.offer_source = 'BROADREACH' and o.discount_value > 0 and discount_value != 'NaN' and oi.item_number IS NOT NULL and o.discount_type in ('DOLLAR_OFF_EACH','AMOUNT_OFF','PERCENT_OFF')
    """

    def read_from_postgres():
        parsed_url = urllib.parse.urlparse(jdbc_url)
        conn = psycopg2.connect(
            host="10.51.181.97",
            port=5432,
            dbname="sams_offer_bank",
            user=jdbc_user,
            password=jdbc_password
        )

        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        field_names = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        for row in rows:
            yield dict(zip(field_names, row))

    return beam.Create(read_from_postgres())

# Function to set pipeline options
def set_pipeline_options(project_id):
    options = PipelineOptions()

    # Set Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = 'us-central1'
    google_cloud_options.temp_location = 'gs://outfiles_parquet/offer_bank/temp/'
    google_cloud_options.staging_location = 'gs://outfiles_parquet/offer_bank/stage/'

    # Set standard options
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    return options

def dict_to_csv(dict_data):
    output = ','.join(map(str, dict_data.values()))
    return output

def run():
    # Fetch secrets from Secret Manager
    project_id = "dev-sams-data-generator"
    jdbc_url_secret_id = "DevPostgresJdbcUrl"
    jdbc_user_secret_id = "DevPostgresRWUser"
    jdbc_password_secret_id = "DevPostgresRWPassword"

    jdbc_url = access_secret_version(project_id, jdbc_url_secret_id)
    jdbc_user = access_secret_version(project_id, jdbc_user_secret_id)
    jdbc_password = access_secret_version(project_id, jdbc_password_secret_id)

    # Set pipeline options
    options = set_pipeline_options(project_id)

    with beam.Pipeline(options=options) as p:
        # Fetch offer metadata
        offer_metadata = (
            p
            | 'FetchOfferMetadata' >> fetch_offer_metadata(jdbc_url, jdbc_user, jdbc_password)
            | 'DictToCSV' >> beam.Map(dict_to_csv)
        )

        offer_metadata_written = ( offer_metadata | 'Write offer Metadata to GCS' >> beam.io.WriteToText(output_offer_metadata_path, file_name_suffix=".json") )

        # Write to GCS as CSV
        #output_path = 'gs://cdp_bucket_check/output/offer_metadata.csv'
        #offer_metadata | 'WriteToGCS' >> beam.io.WriteToText(output_path, file_name_suffix='.csv')


run()
