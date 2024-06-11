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

def transform_and_write_offer_metadata(offer_metadata,p,pipeline_options):
    # Apply transformations
    transformed_data = (
        offer_metadata
        # Apply column renaming
        | 'RenameColumns' >> beam.Map(lambda row: {
            'savingsId': row['offer_id'],
            'savingsType': row['offer_source'],
            'startDate': row['start_datetime'],
            'endDate': row['end_datetime'],
            'timeZone': row['time_zone'],
            'discountType': row['discount_type'],
            'discountValue': row['discount_value'],
            'applicableChannels': row['applicable_channel'],
            'clubs': row['club_list']
        })
        # Set default values
        | 'SetDefaultValues' >> beam.Map(lambda row: {
            **row,
            'exclusive_club_startdate': row.get('exclusive_club_startdate', ''),
            'exclusive_club_enddate': row.get('exclusive_club_enddate', ''),
            'labels': row.get('labels', []),
            'eventTag': 0,
            'basePrice': 0
        })
        | 'FillNullValues' >> beam.Map(lambda row: {
            **row,
            'exclusive_club_number': row.get('exclusive_club_number', 0),
            'exclusive_club_startdate': row.get('exclusive_club_startdate', ''),
            'exclusive_club_enddate': row.get('exclusive_club_enddate', ''),
            'item_type': row.get('item_type', 'DiscountedItem')
        })
        # Convert column data types
        | 'ConvertDataTypes' >> beam.Map(lambda row: {
            **row,
            'basePrice': float(row['basePrice']),  # Assuming basePrice is float
            'discountValue': float(row['discountValue']),  # Assuming discountValue is float
            'savingsId': str(row['savingsId']),
            'startDate': str(row['startDate']),
            'endDate': str(row['endDate']),
            'exclusive_club_number': int(row['exclusive_club_number']),  # Assuming exclusive_club_number is int
            'item_number': int(row['exclusive_club_number'])  # Assuming item_number is int
        })
        # Set eventTag based on labels
        | 'SetEventTag' >> beam.Map(lambda row: {
            **row,
            'eventTag': 2 if 'event' in row['labels'] else (1 if 'ISB' in row['labels'] else 0)
        })
        # Additional transformations...
        | 'Key by item_number for offer metadata' >> beam.Map(lambda row: (row['item_number'], row))
    ) 
    offer_metadata_written = transformed_data | 'Write offer Metadata to GCS' >> beam.io.WriteToText(output_offer_metadata_path, file_name_suffix=".json")
    
    # cdp_items_list = get_product_item_mapping(spark)
    print("Fetching active products from cdp tables")
    query_cdp_items = """select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
                join `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD` t2
                on t1.PROD_ID = t2.PROD_ID
                where t2.PROD_STATUS_CD = 'ACTIVE'"""
    
    cdp_items_list = (p 
                          | 'Read CDP Items' >> beam.io.ReadFromBigQuery(query=query_cdp_items, use_standard_sql=True, gcs_location = 'gs://outfiles_parquet/offer_bank/temp/',project=pipeline_options.view_as(GoogleCloudOptions).project)
        )

    # Group by ITEM_NBR and compute distinct PROD_ID count
    cdp_items_list_grouped = (
            cdp_items_list
            | 'Pair with ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], x['PROD_ID']))
            | 'Group by ITEM_NBR' >> beam.GroupByKey()
            | 'Count Distinct PROD_ID' >> beam.ParDo(GroupByAndCount())
            | 'Rename item_number' >> beam.Map(lambda row: {'item_number': row['itemId'], 'ProductCount': row['ProductCount']})
            | 'Key by itemId for cdp data' >> beam.Map(lambda row: (row['item_number'], row))
        )

    cdp_items_list_grouped_written = cdp_items_list_grouped | ' Write offer Metadata to GCS' >> beam.io.WriteToText(output_cdp_items_list_grouped_path, file_name_suffix=".json")
    
    # Drop duplicates based on ITEM_NBR (already done by GroupByKey)
    cdp_items_list_deduplicated = (
            cdp_items_list
            | 'Key by ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], (x['PROD_ID'], x['ITEM_NBR'])))
            | 'Drop Duplicates' >> beam.Distinct()
            | 'Extract Values' >> beam.Map(lambda x: {'PROD_ID': x[1][0], 'ITEM_NBR': x[1][1]})
            | 'Key by ITEM_NBR for cdp data' >> beam.Map(lambda row: (row['ITEM_NBR'], row))
        )
    
    cdp_items_list_deduplicated_written = cdp_items_list_deduplicated | 'Write offer Metadata to GCS.' >> beam.io.WriteToText(output_cdp_items_list_deduplicated_path, file_name_suffix=".json")
    
    # join to fetch item-product mapping only for relevant items
    transformed_offer_metadata1 = (
            {'offers': transformed_data, 'cdp': cdp_items_list_deduplicated}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Filter Joined Results' >> beam.FlatMap(lambda x: [
                {**clearance_item, 'productId': cdp_item['PROD_ID']}
                for clearance_item in x[1]['offers']
                for cdp_item in x[1]['cdp']
                if clearance_item['item_number'] == cdp_item['ITEM_NBR'] ])
        )

    join_one_written = transformed_offer_metadata1 | 'Write join Metadata to GCS.' >> beam.io.WriteToText(output_join_one_path, file_name_suffix=".json")
    
    # Join the resulting PCollection with cdp_items_list_grouped 
    transformed_offer_metadata3 = ( 
            {'offers1': transformed_offer_metadata1, 'cdp_grouped': cdp_items_list_grouped} 
            | 'CoGroupByKey Final' >> beam.CoGroupByKey() 
            | 'Filter and Flatten Final Join' >> beam.FlatMap(lambda x: [ 
                {**clearance_item, **{'ProductCount': grouped_item['ProductCount']}} 
                for clearance_item in x[1]['offers1'] 
                for grouped_item in x[1]['cdp_grouped'] 
                if clearance_item['item_number'] == grouped_item['item_number'] ]) )  
    
    join_two_written = transformed_offer_metadata3 | 'Write join two Metadata to GCS.' >> beam.io.WriteToText(output_join_two_path, file_name_suffix=".json")
    
               
output_offer_metadata_path = "gs://outfiles_parquet/offer_bank/offer_result/offer_metadata.json" 
output_cdp_items_list_grouped_path = "gs://outfiles_parquet/offer_bank/offer_result/cdp_items_list_grouped.json" 
output_cdp_items_list_deduplicated_path = "gs://outfiles_parquet/offer_bank/offer_result/cdp_items_list_deduplicated.json"
output_join_one_path = "gs://outfiles_parquet/offer_bank/offer_result/join_one_metadata.json" 
output_join_two_path = "gs://outfiles_parquet/offer_bank/offer_result/join_two_metadata.json" 

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
        )
        # Apply transformations and write to destination
        transform_and_write_offer_metadata(offer_metadata,p,pipeline_options)

if __name__ == '__main__':
    run()
