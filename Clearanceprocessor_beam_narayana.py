import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.parquetio import WriteToParquet
from datetime import date
import json
from apache_beam.io.parquetio import WriteToParquet
from datetime import date
import pyarrow as pa


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--output_path', required=True, help='Path to write output data')
        parser.add_argument('--temp_gcs_location',required=True,help='temp GCS location')
        #parser.add_argument('--project',required=True,help='project name')
        #parser.add_argument('--region',required=True,help='region')


def format_clearance_item(row):
    return {
        'startDate': row['effectivedate'],
        'endDate': row['expirationdate'],
        'basePrice': row['originalamount'],
        'discountValue': row['discountedamount'],
        'itemId': row['itemnbr'],
        'clubs': [int(row['clubnbr'])],
        'timeZone': 'UTC',
        'savingsId': f"{row['clubnbr']}{row['itemnbr']}",
        'savingsType': 'Clearance',
        'applicableChannels': [],
        'discountType': 'AMOUNT_OFF',
        'eventTag': 0,
        'members': [],
        'items': "abc,DiscountedItem,xyz",
        'clubOverrides': ",,",
        'productId': None
    }

def create_items_field(row):
    item_schema = {
        'itemId': row['itemId'],
        'productId': row['productId'],
        'itemType': 'DiscountedItem',
        'productItemMappingStatus': row['productItemMappingStatus']
    }
    row['items'] = [item_schema]
    return row

def create_club_overrides_field(row):
    club_overrides_schema = {
        'clubNumber': 0,
        'clubStartDate': '',
        'clubEndDate': ''
    }
    row['clubOverrides'] = [club_overrides_schema]
    return row

class GroupByAndCount(beam.DoFn):
    def process(self, element):
        item_nbr, prod_ids = element
        yield {
            'itemId': item_nbr,
            'ProductCount': len(set(prod_ids))
        }

def coalesce_product_id(row):
        row['productId'] = row['productId'] if row['productId'] is not None else row['PROD_ID']
        return row

def run(argv=None):
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)
    custom_options.view_as(StandardOptions).runner = 'DataflowRunner'
    #custom_options.view_as(GoogleCloudOptions).project = custom_options.project
    google_cloud_options=options.view_as(GoogleCloudOptions)
    google_cloud_options.project='dev-sams-data-generator'
    google_cloud_options.region='us-central1'

    query_clearance_items = """
    SELECT t2.effectivedate,t2.expirationdate, t1.retailamount as originalamount, t1.retailamount-t2.retailamount as discountedamount, t1.itemnbr, t1.clubnbr
            FROM `prod-sams-cdp.prod_pricing_wingman_pricing.current_retail_action` t1
            JOIN `prod-sams-cdp.prod_pricing_wingman_pricing.current_retail_action` t2
            ON t1.itemnbr=t2.itemnbr
            WHERE t1.retailtype ="BP" and t1.clubnbr = 6279 and t2.retailtype ="MD" and t2.clubnbr = 6279 and DATE(t2.effectivedate) <= CURRENT_DATE() and DATE(t2.expirationdate) >= CURRENT_DATE() and t1.retailamount-t2.retailamount > 0
    """

    query_cdp_items = """
    select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
                join `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD` t2
                on t1.PROD_ID = t2.PROD_ID
                where t2.PROD_STATUS_CD = 'ACTIVE'
    """

    with beam.Pipeline(options=options) as p:

        # Read clearance items
        clearance_items_metadata = (p 
                                    | 'Read Clearance Items' >> beam.io.ReadFromBigQuery(query=query_clearance_items, use_standard_sql=True,gcs_location = 'gs://outfiles_parquet/offer_bank/temp/',project=google_cloud_options.project)
                                    | 'Format Clearance Items' >> beam.Map(format_clearance_item)
        )

        # Read CDP items
        cdp_items_list = (p 
                          | 'Read CDP Items' >> beam.io.ReadFromBigQuery(query=query_cdp_items, use_standard_sql=True, gcs_location = 'gs://outfiles_parquet/offer_bank/temp/',project=google_cloud_options.project)
        )

        # Group by ITEM_NBR and compute distinct PROD_ID count
        cdp_items_list_grouped = (
            cdp_items_list
            | 'Pair with ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], x['PROD_ID']))
            | 'Group by ITEM_NBR' >> beam.GroupByKey()
            | 'Count Distinct PROD_ID' >> beam.ParDo(GroupByAndCount())
        )

        # Drop duplicates based on ITEM_NBR (already done by GroupByKey)
        cdp_items_list_deduplicated = (
            cdp_items_list
            | 'Key by ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], (x['PROD_ID'], x['ITEM_NBR'])))
            | 'Drop Duplicates' >> beam.Distinct()
            | 'Extract Values' >> beam.Map(lambda x: {'PROD_ID': x[1][0], 'ITEM_NBR': x[1][1]})
        )

        # Join the two PCollections
        clearanced_items_metadata_two = (
            {'clearance': clearance_items_metadata, 'cdp': cdp_items_list_deduplicated}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Filter Joined Results' >> beam.FlatMap(lambda x: [
                {**clearance_item, 'productId': cdp_item['PROD_ID']}
                for clearance_item in x[1]['clearance']
                for cdp_item in x[1]['cdp']
            ])
        )
