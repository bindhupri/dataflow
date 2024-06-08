import argparse
import os
from datetime import date
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
def access_secret_version(project_id, secret_id, version_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Data corruption detected.")
    return response.payload.data.decode("UTF-8")

# Define transformation function for offers metadata
def transform_offer_metadata(offer):
    from datetime import datetime

    def parse_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") if date_str else None

    offer['savingsId'] = str(offer['offer_id'])
    offer['savingsType'] = offer['offer_source']
    offer['startDate'] = parse_date(offer['start_datetime']).strftime("%Y-%m-%d %H:%M:%S") if offer['start_datetime'] else ""
    offer['endDate'] = parse_date(offer['end_datetime']).strftime("%Y-%m-%d %H:%M:%S") if offer['end_datetime'] else ""
    offer['timeZone'] = offer['time_zone']
    offer['discountType'] = offer['discount_type']
    offer['discountValue'] = float(offer['discount_value'])
    offer['applicableChannels'] = offer['applicable_channel']
    offer['clubs'] = offer['club_list']
    offer['exclusiveClubStartDate'] = parse_date(offer['exclusive_club_startdate']).strftime("%Y-%m-%d %H:%M") if offer['exclusive_club_startdate'] else ""
    offer['exclusiveClubEndDate'] = parse_date(offer['exclusive_club_enddate']).strftime("%Y-%m-%d %H:%M") if offer['exclusive_club_enddate'] else ""
    offer['labels'] = offer['labels'] if offer['labels'] else []
    offer['eventTag'] = 2 if 'event' in offer['labels'] else 1 if 'ISB' in offer['labels'] else 0
    offer['basePrice'] = 0.0
    offer['itemId'] = offer['item_number']
    offer['productId'] = offer['product_id'] if offer['product_id'] else ""
    offer['itemType'] = offer['item_type'] if offer['item_type'] else "DiscountedItem"
    offer['clubNumber'] = int(offer['exclusive_club_number']) if offer['exclusive_club_number'] else 0
    offer['productItemMappingStatus'] = ""
    
    return offer

# Function to fetch offer metadata and transform it
def fetch_and_transform_offer_metadata(jdbc_url, jdbc_user, jdbc_password):
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

    parsed_url = urllib.parse.urlparse(jdbc_url)
    dbname = parsed_url.path[1:]
    host = parsed_url.hostname
    port = parsed_url.port

    def read_from_postgres():
        conn = psycopg2.connect(
            dbname=dbname,
            user=jdbc_user,
            password=jdbc_password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        field_names = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        for row in rows:
            yield dict(zip(field_names, row))

    return beam.Create(read_from_postgres()) | 'TransformOfferMetadata' >> beam.Map(transform_offer_metadata)

# Function to fetch product-item mapping from BigQuery
def fetch_product_item_mapping():
    query = """
        select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
        join prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD t2 on t1.PROD_ID = t2.PROD_ID
        where t2.PROD_STATUS_CD = 'ACTIVE'
    """
    return beam.io.ReadFromBigQuery(query=query) | 'GroupProductItemMapping' >> beam.Map(lambda row: (row['ITEM_NBR'], row))

# Define pipeline options
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', required=True, help='Environment')
        parser.add_argument('--project_id', required=True, help='GCP Project ID')
        parser.add_argument('--savings_ds_bucket', required=True, help='GCS Bucket for savings dataset')

def run(argv=None):
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomPipelineOptions)
    custom_options.view_as(StandardOptions).runner = 'DataflowRunner'
    custom_options.view_as(GoogleCloudOptions).project = custom_options.project_id
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'dev-sams-data-generator'
    google_cloud_options.region = 'us-central1'
    project_id = custom_options.project_id
    env = custom_options.env
    savings_ds_bucket = custom_options.savings_ds_bucket

    try:
        jdbc_url = access_secret_version(project_id, f'{env}PostgresJdbcUrl', 'latest')
        jdbc_user = access_secret_version(project_id, f'{env}PostgresRWUser', 'latest')
        jdbc_password = access_secret_version(project_id, f'{env}PostgresRWPassword', 'latest')
    except Exception as error:
        raise Exception(f"Error while fetching secrets from secret manager: {error}")

    with beam.Pipeline(options=options) as p:
        # Fetch and transform offer metadata
        offer_metadata = (
            p | 'FetchOfferMetadata' >> fetch_and_transform_offer_metadata(jdbc_url, jdbc_user, jdbc_password)
        )

        # Fetch product-item mapping from BigQuery
        product_item_mapping = fetch_product_item_mapping()

        # Combine the two datasets
        combined = (
            {
                'offer_metadata': offer_metadata,
                'product_item_mapping': product_item_mapping
            }
            | 'CombineDatasets' >> CoGroupByKey()
        )

        # Define Parquet schema
        schema = {
            'fields': [
                {'name': 'savingsId', 'type': 'STRING'},
                {'name': 'savingsType', 'type': 'STRING'},
                {'name': 'startDate', 'type': 'STRING'},
                {'name': 'endDate', 'type': 'STRING'},
                {'name': 'timeZone', 'type': 'STRING'},
                {'name': 'discountType', 'type': 'STRING'},
                {'name': 'discountValue', 'type': 'FLOAT'},
                {'name': 'applicableChannels', 'type': 'STRING'},
                {'name': 'clubs', 'type': 'STRING'},
                {'name': 'exclusiveClubStartDate', 'type': 'STRING'},
                {'name': 'exclusiveClubEndDate', 'type': 'STRING'},
                {'name': 'labels', 'type': 'STRING'},
                {'name': 'eventTag', 'type': 'INTEGER'},
                {'name': 'basePrice', 'type': 'FLOAT'},
                {'name': 'itemId', 'type': 'STRING'},
                {'name': 'productId', 'type': 'STRING'},
                {'name': 'itemType', 'type': 'STRING'},
                {'name': 'clubNumber', 'type': 'INTEGER'},
                {'name': 'productItemMappingStatus', 'type': 'STRING'}
            ]
        }

        # Write to GCS as Parquet
        output_path = os.path.join(savings_ds_bucket, str(date.today()))
        (
            combined
            | 'WriteToParquet' >> WriteToParquet(file_path_prefix=output_path, schema=schema)
        )

if __name__ == '__main__':
    run()
