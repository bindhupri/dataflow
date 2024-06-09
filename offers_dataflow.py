import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import psycopg2
from google.cloud import secretmanager
import urllib.parse

def access_secret_version(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Function to fetch offer metadata
def fetch_offer_metadata(jdbc_url, jdbc_user, jdbc_password):
    query = """
        SELECT * FROM public.club_overrides
    """

    def read_from_postgres():
        parsed_url = urllib.parse.urlparse(jdbc_url)
        conn = psycopg2.connect(
            host="34.132.207.75",
            port=5432,
            dbname="postgres",
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
    google_cloud_options.temp_location = 'gs://cdp_bucket_check/tmp/'
    google_cloud_options.staging_location = 'gs://cdp_bucket_check/staging/'

    # Set standard options
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    return options

def dict_to_csv(dict_data):
    output = ','.join(map(str, dict_data.values()))
    return output

def run():
    # Fetch secrets from Secret Manager
    project_id = "stalwart-coast-421315"
    jdbc_url_secret_id = "devPostgresJdbcUrl"
    jdbc_user_secret_id = "devPostgresRWUser"
    jdbc_password_secret_id = "devPostgresRWPassword"

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
        
        # Write to GCS as CSV
        output_path = 'gs://cdp_bucket_check/output/offer_metadata.csv'
        offer_metadata | 'WriteToGCS' >> beam.io.WriteToText(output_path, file_name_suffix='.csv', header='offer_id,club_number,start_datetime,end_datetime,time_zone,create_datetime,modified_datetime')


run()
