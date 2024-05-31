# logic for pulling offer data

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,lit,collect_list,struct,coalesce,date_format,when,array_contains,countDistinct, isnan, when
from pyspark.sql.types import StringType, LongType, ArrayType, IntegerType, DecimalType
import pyspark.sql.types as T
from datetime import date
import google_crc32c
import os
import argparse
from utility.common_code import get_product_item_mapping

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName("OfferProcessor") \
  .getOrCreate()
print("setup spark session")

# prepare to read bigquery
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationProject","stage-sams-offerbank")
spark.conf.set("materializationDataset","stage_sams_offerbank")

today = date.today()
# prepare to cast to array types
int_array_type = T.ArrayType(T.IntegerType()) 
string_array_type = T.ArrayType(T.StringType())

def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response

    payload = response.payload.data.decode("UTF-8")
    print(f"Retrieved the secret {secret_id} from secret manager")
    return payload

def write_broadreach_offers(jdbc_url, jdbc_user, jdbc_password):
    print("Fetching Broadreach offers from the DB")
    # query to fetch all offer metadata
    query= "(select o.offer_id, o.offer_source, o.start_datetime, o.end_datetime, o.time_zone, o.discount_type, o.discount_value, o.applicable_channel, o.club_list, o.labels, oi.item_number, oi.product_id, oi.item_type, mo.membership_id, oc.club_number as exclusive_club_number, oc.start_datetime as exclusive_club_startdate, oc.end_datetime as exclusive_club_enddate\
        from public.offers o\
        left join public.member_offers mo\
        on o.offer_id = mo.offer_id\
        left join public.offer_items_v2 oi\
        on o.offer_id = oi.offer_id\
        left join public.club_overrides oc\
        on o.offer_id = oc.offer_id\
        where o.start_datetime <= now() and o.end_datetime >= now() and o.offer_source = 'BROADREACH' and o.discount_value > 0 and discount_value != 'NaN' and oi.item_number IS NOT NULL and o.discount_type in ('DOLLAR_OFF_EACH','AMOUNT_OFF','PERCENT_OFF')) as items"
    offer_metadata = spark.read\
                    .format("jdbc")\
                    .option("url", jdbc_url) \
                    .option("driver", 'org.postgresql.Driver')\
                    .option("dbtable", query) \
                    .option("user", jdbc_user)\
                    .option("password", jdbc_password)\
                    .load()
    print("Initiating BroadReach offers write..")
    transform_and_write_offer_metadata(offer_metadata)

def transform_and_write_offer_metadata(offer_metadata): 
    # rename columns
    print("Changing column names")
    offer_metadata = offer_metadata.withColumnRenamed('offer_id','savingsId')\
        .withColumnRenamed('offer_source','savingsType')\
        .withColumnRenamed('start_datetime','startDate')\
        .withColumnRenamed('end_datetime','endDate')\
        .withColumnRenamed('time_zone', 'timeZone')\
        .withColumnRenamed('discount_type', 'discountType')\
        .withColumnRenamed('discount_value','discountValue')\
        .withColumnRenamed('applicable_channel','applicableChannels')\
        .withColumnRenamed('club_list','clubs')

    # set default values
    print("Setting default values")
    offer_metadata = offer_metadata.withColumn("exclusive_club_startdate",  date_format(col("exclusive_club_startdate"), "yyyy-MM-dd hh:mm"))\
        .withColumn("exclusive_club_enddate",  date_format(col("exclusive_club_enddate"), "yyyy-MM-dd hh:mm"))\
        .withColumn("labels", F.when(F.col("labels").isNull(), F.array([]).cast(string_array_type)).otherwise(F.col("labels")))\
        .withColumn("eventTag", lit(0))\
        .withColumn("basePrice", lit(0))
    offer_metadata = offer_metadata.na.fill(value=0,subset=["exclusive_club_number"])\
        .na.fill(value="",subset=["exclusive_club_startdate"])\
        .na.fill(value="",subset=["exclusive_club_enddate"])\
        .na.fill(value="DiscountedItem",subset=["item_type"])

    offer_metadata = offer_metadata.withColumn('basePrice', col('basePrice').cast(DecimalType(38,9)))\
        .withColumn('discountValue', col('discountValue').cast(DecimalType(38,9)))\
        .withColumn('savingsId', col('savingsId').cast(StringType()))\
        .withColumn('startDate', col('startDate').cast(StringType()))\
        .withColumn('endDate', col('endDate').cast(StringType()))\
        .withColumn('exclusive_club_number', col('exclusive_club_number').cast(IntegerType()))\
        .withColumn('item_number', col('item_number').cast(LongType()))

    # set event tag based on label
    offer_metadata = offer_metadata.withColumn("eventTag", when(array_contains(col("labels"),"event"),lit(2))
        .when(array_contains(col("labels"),"ISB"),lit(1))
        .otherwise(lit(0)))

    # cdp_items_list = get_product_item_mapping(spark)
    print("Fetching active products from cdp tables")
    sql_2 = """select t1.PROD_ID, t1.ITEM_NBR FROM `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.CLUB_ITEM_GRP` t1
                join `prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.PROD` t2
                on t1.PROD_ID = t2.PROD_ID
                where t2.PROD_STATUS_CD = 'ACTIVE'"""
    cdp_items_list = spark.read.format('bigquery') \
      .option("query", sql_2)\
      .load()
    cdp_items_list_grouped = cdp_items_list.groupBy("ITEM_NBR").agg(countDistinct("PROD_ID").alias("ProductCount"))
    cdp_items_list_grouped = cdp_items_list_grouped.withColumnRenamed('ITEM_NBR','item_number')
    cdp_items_list = cdp_items_list.drop_duplicates(subset=['ITEM_NBR'])
    
    print("Extracting only required product ids")
    offer_metadata = offer_metadata.join(cdp_items_list, offer_metadata.item_number == cdp_items_list.ITEM_NBR, 'left')

    # join to fetch product-item mapping for only relevant items in offers
    print("computing no of items with multiple products..")
    offer_metadata = offer_metadata.join(cdp_items_list_grouped, "item_number", "left")
    multiple_product_mapping_df = offer_metadata.filter(offer_metadata["ProductCount"] > 1)
    count = multiple_product_mapping_df.count()
    print("No of items with multiple products:", count)

    # set empty product ids with null
    offer_metadata = offer_metadata.withColumn("product_id", \
        when(col("product_id")=="" ,None) \
            .otherwise(col("product_id")))

    # Use the coalesce function to fill null product ids in df2 with valid product ids from df1
    offer_metadata = offer_metadata.withColumn('product_id', coalesce(offer_metadata['product_id'], offer_metadata['PROD_ID']))

    # Drop the redundant columns and replace null values with empty string to facilitate safe write into parquet
    offer_metadata = offer_metadata.drop('PROD_ID').drop('ITEM_NBR').drop("labels")

    print("computing null product ids..")
    null_products_df = offer_metadata.where(F.col('product_id').isNull())
    print("number of null values: "+str(null_products_df.count()))

    offer_metadata = offer_metadata.withColumn("productItemMappingStatus", when(F.col("ProductCount") > 1, lit("1-to-multiple"))
            .when(F.col('product_id').isNull(),lit("missing"))
            .otherwise(lit("normal")))

    offer_metadata = offer_metadata.na.fill(value="",subset=["product_id"])
    offer_metadata = offer_metadata.withColumnRenamed('item_number','itemId')\
        .withColumnRenamed('product_id','productId')\
        .withColumnRenamed('item_type','itemType')\
        .withColumnRenamed('exclusive_club_number','clubNumber')\
        .withColumnRenamed('exclusive_club_startdate','clubStartDate')\
        .withColumnRenamed('exclusive_club_enddate','clubEndDate')

    # group members, items and club overrides for an offer
    print("Grouping members, items and club overrides")
    offer_metadata = offer_metadata\
        .groupBy("savingsId","savingsType","startDate","endDate","timeZone","discountType","basePrice","discountValue","applicableChannels","clubs","eventTag")\
        .agg(collect_list("membership_id").alias("members"), \
            collect_list(struct("itemId","productId","itemType","productItemMappingStatus").alias("items")), \
            collect_list(struct("clubNumber","clubStartDate","clubEndDate").alias("clubOverrides")))

    # rename grouped columns
    offer_metadata = offer_metadata.withColumnRenamed("collect_list(struct(itemId, productId, itemType, productItemMappingStatus) AS items)", "items")
    offer_metadata = offer_metadata.withColumnRenamed("collect_list(struct(clubNumber, clubStartDate, clubEndDate) AS clubOverrides)", "clubOverrides")
    offer_metadata.printSchema()

    print("Writing offers as parquet")

    offer_metadata = offer_metadata.repartition(1)
    offer_metadata.write.mode("append").parquet(os.environ.get('SAVINGS_DS_BUCKET')+str(today))

def write_personalized_offers(jdbc_url, jdbc_user, jdbc_password):

    # get offer-member count
    print("Fetching offer-member count for personalized offers")
    query = "(select om.offer_id as offer_id, count(1) as number_of_members\
            from public.member_offers om left join public.offers o\
            on o.offer_id = om.offer_id\
            where o.offer_source in ('Tetris','TIO') and o.start_datetime <= now() and o.end_datetime >= now()\
            group by om.offer_id\
            order by number_of_members ASC) as items"
    offer_member_count = spark.read\
                    .format("jdbc")\
                    .option("url", jdbc_url) \
                    .option("driver", 'org.postgresql.Driver')\
                    .option("dbtable", query) \
                    .option("user", jdbc_user)\
                    .option("password", jdbc_password)\
                    .load()

    def write_all_personalized_offers_with_less_members(offers_with_less_members):
        offer_id_list = offers_with_less_members.select("offer_id").rdd.map(lambda x: x[0]).collect()
        offer_ids = ",".join(map(str,offer_id_list))

        print("Fetching all personalized offers with lesser members")
        query= "(select o.offer_id, o.offer_source, o.start_datetime, o.end_datetime, o.time_zone, o.discount_type, o.discount_value, o.applicable_channel, o.club_list, o.labels, oi.item_number, oi.product_id, oi.item_type, mo.membership_id, oc.club_number as exclusive_club_number, oc.start_datetime as exclusive_club_startdate, oc.end_datetime as exclusive_club_enddate\
        from public.offers o\
        left join public.member_offers mo\
        on o.offer_id = mo.offer_id\
        left join public.offer_items_v2 oi\
        on o.offer_id = oi.offer_id\
        left join public.club_overrides oc\
        on o.offer_id = oc.offer_id\
        where o.start_datetime <= now() and o.end_datetime >= now() and o.offer_id in ("+offer_ids+") and o.discount_value > 0 and discount_value != 'NaN' and o.offer_source in ('Tetris','TIO') and oi.item_number IS NOT NULL and o.discount_type in ('DOLLAR_OFF_EACH','AMOUNT_OFF','PERCENT_OFF')) as items"
        offer_metadata = spark.read\
                        .format("jdbc")\
                        .option("url", jdbc_url) \
                        .option("driver", 'org.postgresql.Driver')\
                        .option("dbtable", query) \
                        .option("user", jdbc_user)\
                        .option("password", jdbc_password)\
                        .load()
        print("Initiating Personalized offers write for lesser members..")
        transform_and_write_offer_metadata(offer_metadata)

     # if member count > x, write offers individually
    offers_with_less_members = offer_member_count.filter(offer_member_count.number_of_members <= 50000)
    write_all_personalized_offers_with_less_members(offers_with_less_members)

    def write_each_personalized_offer_with_more_members(offers_with_more_members):
        offer_id_list = offers_with_more_members.select("offer_id").rdd.map(lambda x: x[0]).collect()
        offer_id_list = map(str, offer_id_list)

        for offer_id in offer_id_list:
            print("Fetching each personalized offers with more members")
            query= "(select o.offer_id, o.offer_source, o.start_datetime, o.end_datetime, o.time_zone, o.discount_type, o.discount_value, o.applicable_channel, o.club_list, o.labels, oi.item_number, oi.product_id, oi.item_type, mo.membership_id, oc.club_number as exclusive_club_number, oc.start_datetime as exclusive_club_startdate, oc.end_datetime as exclusive_club_enddate\
            from public.offers o\
            left join public.member_offers mo\
            on o.offer_id = mo.offer_id\
            left join public.offer_items_v2 oi\
            on o.offer_id = oi.offer_id\
            left join public.club_overrides oc\
            on o.offer_id = oc.offer_id\
            where o.start_datetime <= now() and o.end_datetime >= now() and o.discount_value > 0 and discount_value != 'NaN' and o.offer_id = "+offer_id+" and o.offer_source in ('Tetris','TIO') and oi.item_number IS NOT NULL and o.discount_type in ('DOLLAR_OFF_EACH','AMOUNT_OFF','PERCENT_OFF')) as items"
            offer_metadata = spark.read\
                            .format("jdbc")\
                            .option("url", jdbc_url) \
                            .option("driver", 'org.postgresql.Driver')\
                            .option("dbtable", query) \
                            .option("user", jdbc_user)\
                            .option("password", jdbc_password)\
                            .load()
            print("Initiating Personalized offers write for more members..")
            transform_and_write_offer_metadata(offer_metadata)

    offers_with_more_members = offer_member_count.filter(offer_member_count.number_of_members > 50000)
    write_each_personalized_offer_with_more_members(offers_with_more_members)


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--savings_ds_bucket", required=True)
    return parser.parse_args()


def main():
    # read offer metadata from db
    print("pulling the postgres secrets from secret manager..")
    # env = os.environ.get('ENV')
    # project_id = os.environ.get('PROJECT_ID')
    env = 'Prod'
    project_id = 'prod-sams-offerbank'
    try:
        jdbc_url = access_secret_version(project_id,env+'PostgresJdbcUrl','latest')
        jdbc_user = access_secret_version(project_id,env+'PostgresRWUser','latest')
        jdbc_password = access_secret_version(project_id,env+'PostgresRWPassword','latest')
    except Exception as error:
        raise Exception(f"Error while fetching secrets from secret manager: {error} ")
    write_broadreach_offers(jdbc_url, jdbc_user, jdbc_password)
    write_personalized_offers(jdbc_url, jdbc_user, jdbc_password)


if __name__ == "__main__":
    args = _parse_arguments()
    os.environ["ENV"] = args.env
    os.environ["PROJECT_ID"] = args.project_id
    os.environ["SAVINGS_DS_BUCKET"] = args.savings_ds_bucket
    main()

# spark-submit --master local --queue default --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2,org.postgresql:postgresql:42.3.6 --verbose /data-generator/sams_data_generator/OfferProcessor.py
