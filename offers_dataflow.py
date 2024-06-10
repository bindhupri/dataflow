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

class GroupByAndCount(beam.DoFn):
    def process(self, element):
        item_nbr, prod_ids = element
        yield {
            'itemId': item_nbr,
            'ProductCount': len(set(prod_ids))
        }

def transform_and_write_offer_metadata(offer):
    offer['savingsId'] = str(offer['offer_id'])
    offer['savingsType'] = offer['offer_source']
    offer['startDate'] = offer['start_datetime'].strftime("%Y-%m-%d %H:%M:%S") if offer['start_datetime'] else ""
    offer['endDate'] = offer['end_datetime'].strftime("%Y-%m-%d %H:%M:%S") if offer['end_datetime'] else ""
    offer['timeZone'] = offer['time_zone']
    offer['discountType'] = offer['discount_type']
    offer['discountValue'] = float(offer['discount_value'])  # Ensure proper handling of discount_value
    offer['applicableChannels'] = offer['applicable_channel']
    offer['clubs'] = offer['club_list']
    offer['exclusive_club_startdate'] = offer['exclusive_club_startdate'].strftime("%Y-%m-%d %H:%M") if offer['exclusive_club_startdate'] else ""
    offer['exclusive_club_enddate'] = offer['exclusive_club_enddate'].strftime("%Y-%m-%d %H:%M") if offer['exclusive_club_enddate'] else ""
    offer['labels'] = offer['labels'] if offer['labels'] else []
    offer['eventTag'] = 2 if 'event' in offer['labels'] else 1 if 'ISB' in offer['labels'] else 0
    offer['basePrice'] = 0.0
    offer['exclusive_club_number'] = int(offer['exclusive_club_number']) if offer['exclusive_club_number'] else 0
    offer['itemId'] = offer['item_number']
    offer['productId'] = offer['product_id'] if offer['product_id'] else ""
    offer['itemType'] = offer['item_type'] if offer['item_type'] else "DiscountedItem"
    offer['productItemMappingStatus'] = ""
    
    # Delete the original keys used for renaming
    del offer['offer_id']
    del offer['offer_source']
    del offer['start_datetime']
    del offer['end_datetime']
    del offer['time_zone']
    del offer['discount_type']
    del offer['discount_value']
    del offer['applicable_channel']
    del offer['club_list'] 

    # cdp_items_list = get_product_item_mapping(spark)
    print("Fetching active products from cdp tables")
    query_cdp_items = """select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
                join `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD` t2
                on t1.PROD_ID = t2.PROD_ID
                where t2.PROD_STATUS_CD = 'ACTIVE'"""
    
    cdp_items_list = (p 
                          | 'Read CDP Items' >> beam.io.ReadFromBigQuery(query=query_cdp_items, use_standard_sql=True, gcs_location = 'gs://outfiles_parquet/offer_bank/temp/',project=google_cloud_options.project)
        )

    # Group by ITEM_NBR and compute distinct PROD_ID count
    cdp_items_list_grouped = (
            cdp_items_list
            | 'Pair with ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], x['PROD_ID']))
            | 'Group by ITEM_NBR' >> beam.GroupByKey()
            | 'Count Distinct PROD_ID' >> beam.ParDo(GroupByAndCount())
            | 'Key by itemId for cdp data' >> beam.Map(lambda row: (row['itemId'], row))
        )
         

output_offer_metadata_path = "gs://outfiles_parquet/offer_bank/offer_result/offer_metadata.json" 

# Function to fetch offer metadata 
def write_broadreach_offers(jdbc_url, jdbc_user, jdbc_password):
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
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project
 
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
            | 'broadreachoffersMetadata' >> write_broadreach_offers(jdbc_url, jdbc_user, jdbc_password) 
            | 'TransformOfferMetadata' >> beam.Map(transform_and_write_offer_metadata)
        )

        offer_metadata_written = (
            offer_metadata 
            | 'Write offer Metadata to GCS' >> beam.io.WriteToText(output_offer_metadata_path, file_name_suffix=".json")
        )

if __name__ == '__main__':
    run()
