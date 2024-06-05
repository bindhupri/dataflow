# logic for pulling clearance item data

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,lit,concat,coalesce,countDistinct,when
from pyspark.sql.types import StructType, StringType, StructField, LongType, ArrayType, IntegerType
from datetime import date
import os
import argparse
from utility.common_code import get_product_item_mapping

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('ClearanceProcessor') \
  .getOrCreate()
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationProject","stage-sams-offerbank")
spark.conf.set("materializationDataset","stage_sams_offerbank")

import pyspark.sql.types as T
int_array_type = T.ArrayType(T.IntegerType()) 
string_array_type = T.ArrayType(T.StringType())
today = date.today()

def write_clearance_items():
    # read active clearance offer items
    sql = """SELECT t2.effectivedate,t2.expirationdate, t1.retailamount as originalamount, t1.retailamount-t2.retailamount as discountedamount, t1.itemnbr, t1.clubnbr
            FROM `prod-sams-cdp.prod_pricing_wingman_pricing.current_retail_action` t1
            JOIN `prod-sams-cdp.prod_pricing_wingman_pricing.current_retail_action` t2
            ON t1.itemnbr=t2.itemnbr
            WHERE t1.retailtype ="BP" and t1.clubnbr = 6279 and t2.retailtype ="MD" and t2.clubnbr = 6279 and DATE(t2.effectivedate) <= CURRENT_DATE() and DATE(t2.expirationdate) >= CURRENT_DATE() and t1.retailamount-t2.retailamount > 0"""

    print("Fetching clearance items from the DB")
    clearance_items_metadata = spark.read.format('bigquery') \
      .option("query", sql)\
      .load()

    # rename columns
    print("Changing column names")
    clearance_items_metadata = clearance_items_metadata.withColumnRenamed('effectivedate','startDate')\
      .withColumnRenamed('expirationdate','endDate')\
      .withColumn('timeZone', lit("UTC"))\
      .withColumnRenamed('originalamount','basePrice')\
      .withColumnRenamed('discountedamount','discountValue')\
      .withColumnRenamed('itemnbr','itemId')\
      .withColumnRenamed('clubnbr','clubs')
    print("Total number of clearance items: "+str(clearance_items_metadata.count()))
    clearance_items_metadata = clearance_items_metadata.withColumn('productId', lit(None))

    # add default values
    print("Setting default values")
    clearance_items_metadata = clearance_items_metadata.withColumn("savingsId", concat(clearance_items_metadata.clubs, clearance_items_metadata.itemId))\
      .withColumn("savingsType", lit("Clearance"))\
      .withColumn("applicableChannels", F.array([]).cast(string_array_type))\
      .withColumn("discountType", lit("AMOUNT_OFF"))\
      .withColumn("eventTag", lit(0))\
      .withColumn("members", F.array([]).cast(string_array_type))\
      .withColumn("items", lit("abc,DiscountedItem,xyz"))\
      .withColumn("clubOverrides", lit(",,"))\
      .withColumn("clubs", F.array([clearance_items_metadata.clubs]))
    clearance_items_metadata = clearance_items_metadata.withColumn("clubs", col("clubs").cast(ArrayType(IntegerType())))

    print("Fetching active products from cdp tables")
    sql_2 = """select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
                join `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD` t2
                on t1.PROD_ID = t2.PROD_ID
                where t2.PROD_STATUS_CD = 'ACTIVE'"""
    cdp_items_list = spark.read.format('bigquery') \
      .option("query", sql_2)\
      .load()
    
    # compute product count for each item
    cdp_items_list_grouped = cdp_items_list.groupBy("ITEM_NBR").agg(countDistinct("PROD_ID").alias("ProductCount"))
    cdp_items_list_grouped = cdp_items_list_grouped.withColumnRenamed('ITEM_NBR','itemId')
    
    # for any items with multiple product mapping pick only the first entry
    cdp_items_list = cdp_items_list.drop_duplicates(subset=['ITEM_NBR'])

    # join to fetch item-product mapping only for relevant items
    print("Extracting only required product ids")
    clearance_items_metadata = clearance_items_metadata.join(cdp_items_list, clearance_items_metadata.itemId == cdp_items_list.ITEM_NBR, 'left')

    # copy product ids from prod-item mapping results to clearance items df using coalesce
    clearance_items_metadata = clearance_items_metadata.withColumn('productId', coalesce(clearance_items_metadata['productId'], clearance_items_metadata['PROD_ID']))
    
    # join to fetch product count for every item
    print("Computing no of products per item in cdp..")
    clearance_items_metadata = clearance_items_metadata.join(cdp_items_list_grouped, "itemId", "left")
    multiple_product_mapping_df = clearance_items_metadata.filter(clearance_items_metadata["ProductCount"] > 1)
    count = multiple_product_mapping_df.count()
    print("No of items with multiple products:", count)
    print("computing null product ids..")
    null_products_df = clearance_items_metadata.where(F.col('productId').isNull())
    print("number of null values: "+str(null_products_df.count()))

    # Drop the redundant columns
    clearance_items_metadata = clearance_items_metadata.drop('PROD_ID').drop('ITEM_NBR')

    clearance_items_metadata = clearance_items_metadata.withColumn("productItemMappingStatus", when(F.col("ProductCount") > 1, lit("1-to-multiple"))
            .when(F.col('productId').isNull(),lit("missing"))
            .otherwise(lit("normal")))

    # replace null values with empty string to facilitate safe write into parquet
    clearance_items_metadata = clearance_items_metadata.na.fill(value="",subset=["productId"])

    # convert a string items column to array of struct
    item_schema = StructType([
        StructField('itemId', StringType(), False),
        StructField('productId', StringType(), False),
        StructField('itemType', StringType(), False)
    ])
    clearance_items_metadata = clearance_items_metadata.withColumn('items', F.from_json(col("items"), item_schema))\
      .withColumn("items", F.col("items").withField("itemId", clearance_items_metadata.itemId))\
      .withColumn("items", F.col("items").withField("productId", clearance_items_metadata.productId))\
      .withColumn("items", F.col("items").withField("itemType", F.lit("DiscountedItem")))\
      .withColumn("items", F.col("items").withField("productItemMappingStatus", clearance_items_metadata.productItemMappingStatus))\
      .drop("itemId")
    clearance_items_metadata = clearance_items_metadata.withColumn("items", F.array([clearance_items_metadata.items]))
    clearance_items_metadata = clearance_items_metadata.drop('productId').drop('ProductCount').drop('productItemMappingStatus')

    # convert a string clobOverrides column to array of struct
    print("Grouping members, items and club overrides")
    club_overrides_schema = StructType([
        StructField('clubNumber', IntegerType(), False),
        StructField('clubStartDate', StringType(), False),
        StructField('clubEndDate', StringType(), False)
    ])
    clearance_items_metadata = clearance_items_metadata.withColumn('clubOverrides', F.from_json(col("clubOverrides"), club_overrides_schema))\
      .withColumn("clubOverrides", F.col("clubOverrides").withField("clubNumber", F.lit(0)))\
      .withColumn("clubOverrides", F.col("clubOverrides").withField("clubStartDate", F.lit("")))\
      .withColumn("clubOverrides", F.col("clubOverrides").withField("clubEndDate", F.lit("")))
    clearance_items_metadata = clearance_items_metadata.withColumn("clubOverrides", F.array([clearance_items_metadata.clubOverrides]))
    clearance_items_metadata.printSchema()

    print("Writing clearance items as parquet")
    clearance_items_metadata = clearance_items_metadata.repartition(1)
    clearance_items_metadata.write.mode("append").parquet(os.environ.get('SAVINGS_DS_BUCKET')+str(today))


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--savings_ds_bucket", required=True)
    return parser.parse_args()


def main():
    write_clearance_items()


if __name__ == "__main__":
    args = _parse_arguments()
    os.environ["SAVINGS_DS_BUCKET"] = args.savings_ds_bucket
    main()

# spark-submit --master local --queue default --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2 --verbose /data-generator/sams_data_generator/ClearanceProcessor.py
