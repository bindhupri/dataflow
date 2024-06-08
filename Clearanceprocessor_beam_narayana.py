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

def drop_redundant_columns(row): 
    row.pop('PROD_ID', None) 
    row.pop('ITEM_NBR', None) 
    return row

def add_product_item_mapping_status(row):
    if row.get('ProductCount', 0) > 1:
        row['productItemMappingStatus'] = '1-to-multiple'
    elif row['productId'] is None:
        row['productItemMappingStatus'] = 'missing'
    else:
        row['productItemMappingStatus'] = 'normal'
    return row

def replace_null_with_empty(row):
    if row['productId'] is None:
        row['productId'] = ''
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

    output_clearance_metadata_path = "gs://outfiles_parquet/offer_bank/result/clearance_metadata.json" 
    output_cdp_items_list_grouped_path = "gs://outfiles_parquet/offer_bank/result/cdp_items_list_grouped.json" 
    output_deduplicated_path = "gs://outfiles_parquet/offer_bank/result/deduplicated_data.json"
    output_join_one_path = "gs://outfiles_parquet/offer_bank/result/join_one_data.json"
    output_join_two_path = "gs://outfiles_parquet/offer_bank/result/join_two_data.json"
    output_final_path = "gs://outfiles_parquet/offer_bank/result/final_data.json"

    with beam.Pipeline(options=options) as p:

        # Read clearance items
        clearance_items_metadata = (p 
                                    | 'Read Clearance Items' >> beam.io.ReadFromBigQuery(query=query_clearance_items, use_standard_sql=True,gcs_location = 'gs://outfiles_parquet/offer_bank/temp/',project=google_cloud_options.project)
                                    | 'Format Clearance Items' >> beam.Map(format_clearance_item)
                                    | 'Key by itemId for clearance' >> beam.Map(lambda row: (row['itemId'], row))
        )

        clearance_metadata_written = ( clearance_items_metadata | 'Write Clearance Metadata to GCS' >> beam.io.WriteToText(output_clearance_metadata_path, file_name_suffix=".json") )

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
            | 'Key by itemId for cdp data' >> beam.Map(lambda row: (row['itemId'], row))
        )

        cdp_items_list_grouped_written = ( cdp_items_list_grouped | 'Write Clearance Metadata to GCS.' >> beam.io.WriteToText(output_cdp_items_list_grouped_path, file_name_suffix=".json") )

        # Drop duplicates based on ITEM_NBR (already done by GroupByKey)
        cdp_items_list_deduplicated = (
            cdp_items_list
            | 'Key by ITEM_NBR' >> beam.Map(lambda x: (x['ITEM_NBR'], (x['PROD_ID'], x['ITEM_NBR'])))
            | 'Drop Duplicates' >> beam.Distinct()
            | 'Extract Values' >> beam.Map(lambda x: {'PROD_ID': x[1][0], 'ITEM_NBR': x[1][1]})
            | 'Key by ITEM_NBR for cdp data' >> beam.Map(lambda row: (row['ITEM_NBR'], row))
        )
         
        deduplicated_written = ( cdp_items_list_deduplicated | 'Write Deduplicated Data to GCS' >> beam.io.WriteToText(output_deduplicated_path, file_name_suffix=".json") )

        # join to fetch item-product mapping only for relevant items
        clearanced_items_metadata1 = (
            {'clearance': clearance_items_metadata, 'cdp': cdp_items_list_deduplicated}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Filter Joined Results' >> beam.FlatMap(lambda x: [
                {**clearance_item, 'productId': cdp_item['PROD_ID']}
                for clearance_item in x[1]['clearance']
                for cdp_item in x[1]['cdp']
                if clearance_item['itemId'] == cdp_item['ITEM_NBR'] ])
        )

        # Apply coalesce operation to the joined PCollection
        clearanced_items_metadata2 = (
            clearanced_items_metadata1 
            | 'Coalesce productId' >> beam.Map(coalesce_product_id)
            | 'Key by itemId for clearance.' >> beam.Map(lambda row: (row['itemId'], row))
        )

        join_one_written = ( clearanced_items_metadata2 | 'Write Join one to GCS' >> beam.io.WriteToText(output_join_one_path, file_name_suffix=".json") )


        # Join the resulting PCollection with cdp_items_list_grouped 
        clearanced_items_metadata3 = ( 
            {'clearance1': clearanced_items_metadata2, 'cdp_grouped': cdp_items_list_grouped} 
            | 'CoGroupByKey Final' >> beam.CoGroupByKey() 
            | 'Filter and Flatten Final Join' >> beam.FlatMap(lambda x: [ 
                {**clearance_item, **{'ProductCount': grouped_item['ProductCount']}} 
                for clearance_item in x[1]['clearance1'] 
                for grouped_item in x[1]['cdp_grouped'] 
                if clearance_item['itemId'] == grouped_item['itemId'] ]) )

        join_two_written = ( clearanced_items_metadata3 | 'Write Join two to GCS' >> beam.io.WriteToText(output_join_two_path, file_name_suffix=".json") )

        #Filter items with ProductCount > 1
        multiple_product_mapping = (
            clearanced_items_metadata3
            | 'Filter Multiple Product Count' >> beam.Filter(lambda x: x['ProductCount'] > 1)
        )

        multiple_product_mapping_count = (
            multiple_product_mapping
            | 'Count Multiple Product Items' >> beam.combiners.Count.Globally()
        )

        multiple_product_mapping_count | 'Print Multiple Product Count' >> beam.Map(print)

        # Filter items with null productId
        null_product_items = (
            clearanced_items_metadata3
            | 'Filter Null ProductId' >> beam.Filter(lambda x: x['productId'] is None)
        )

        null_product_items_count = (
            null_product_items
            | 'Count Null Product Items' >> beam.combiners.Count.Globally()
        )

        null_product_items_count | 'Print Null Product Count' >> beam.Map(lambda count: print(f'Number of null values: {count}'))

        # Drop redundant columns
        final_joined_metadata =  clearanced_items_metadata3 | 'Drop Redundant Columns' >> beam.Map(drop_redundant_columns)

        # Add productItemMappingStatus column
        final_joined_metadata1 = final_joined_metadata | 'Add productItemMappingStatus' >> beam.Map(add_product_item_mapping_status)

        # Replace null values with empty string
        final_joined_metadata2 = final_joined_metadata1 | 'Replace Null with Empty' >> beam.Map(replace_null_with_empty)

        # Final processing
        final_clearance_items = ( final_joined_metadata2
                                 | 'Create Items Field' >> beam.Map(create_items_field)
                                 | 'Create Club Overrides Field' >> beam.Map(create_club_overrides_field)
        )

        final_written = ( final_clearance_items | 'Write finalto GCS' >> beam.io.WriteToText(output_final_path, file_name_suffix=".json") )

'''
        

        # Define schema
        parquet_schema = pa.schema([
            ('startDate', pa.string()),
            ('endDate', pa.string()),
            ('basePrice', pa.float64()),
            ('discountValue', pa.float64()),
            ('itemId', pa.string()),
            ('clubs', pa.list_(pa.int32())),
            ('timeZone', pa.string()),
            ('savingsId', pa.string()),
            ('savingsType', pa.string()),
            ('applicableChannels', pa.list_(pa.string())),
            ('discountType', pa.string()),
            ('eventTag', pa.int32()),
            ('members', pa.list_(pa.string())),
            ('items', pa.list_(pa.struct([
                ('itemId', pa.string()),
                ('productId', pa.string()),
                ('itemType', pa.string()),
                ('productItemMappingStatus', pa.string())
            ]))),
            ('clubOverrides', pa.list_(pa.struct([
                ('clubNumber', pa.int32()),
                ('clubStartDate', pa.string()),
                ('clubEndDate', pa.string())
            ]))),
        ])

        # Write to Parquet on local drive (C: drive)
        output_path = custom_options.output_path
        final_clearance_items | 'Write to Parquet' >> WriteToParquet(
            file_path_prefix=os.path.join(output_path, str(date.today())),
            file_name_suffix='.parquet',
            schema=parquet_schema
        )
'''

# Run the pipeline
if __name__ == '__main__':
    run()
