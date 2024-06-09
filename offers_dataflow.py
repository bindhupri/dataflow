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

    class FetchOfferMetadata(beam.PTransform):
        def expand(self, pcoll):
            return pcoll | beam.Create(read_from_postgres())

    return FetchOfferMetadata()

# Define pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', required=True, help='Environment')
        parser.add_argument('--project_id', required=True, help='GCP Project ID')
        parser.add_argument('--savings_ds_bucket', required=True, help='GCS Bucket for savings dataset')
 
def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    project = custom_options.project_id
    env = custom_options.env
    savings_ds_bucket = custom_options.savings_ds_bucket
 
    try:
        jdbc_url = access_secret_version(project, f'{env}PostgresJdbcUrl', 'latest')
        jdbc_user = access_secret_version(project, f'{env}PostgresRWUser', 'latest')
        jdbc_password = access_secret_version(project, f'{env}PostgresRWPassword', 'latest')
    except Exception as error:
        raise Exception(f"Error while fetching secrets from secret manager: {error}")

    with beam.Pipeline(options=pipeline_options) as p:
        # Fetch offer metadata
        offer_metadata = (
            p
            | 'FetchOfferMetadata' >> fetch_offer_metadata(jdbc_url, jdbc_user, jdbc_password) 
        )

        offer_metadata_written = (
            offer_metadata 
            | 'Write offer Metadata to GCS' >> beam.io.WriteToText(output_offer_metadata_path, file_name_suffix=".json")
        )

if __name__ == '__main__':
    run()
