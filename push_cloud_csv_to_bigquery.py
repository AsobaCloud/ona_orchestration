import json
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

# Construct a BigQuery client object.
client = bigquery.Client()


def get_schema_json(path):
    with open(path) as file:
        s = json.load(file)
    return s


def get_sqltype(dtype):
    if dtype == 'int':
        return 'INT64'
    if dtype == 'float':
        return 'FLOAT64'
    if dtype == 'string':
        return 'STRING'
    if dtype == 'dateTime':
        return 'DATETIME'


def get_bigquery_schema(fields):
    schema = []
    for f in fields:
        dtype = f["constraints"]["type"].split('#')[1]
        sqltype = get_sqltype(dtype)
        if f["constraints"]["required"]:
            schema.append(
                bigquery.SchemaField(
                    f["name"],
                    sqltype,
                    "Required"
                )
            )
        else:
            schema.append(
                bigquery.SchemaField(
                    f["name"],
                    sqltype
                )
            )
    return schema


def trigger_load_csv_job(bq_schema, uri, table_id):
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        skip_leading_rows=1,
        null_marker='NA'
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    return load_job


s = get_schema_json('map_schema.json')
bq_schema = get_bigquery_schema(s['fields'])
uri = "gs://asoba-pipeline/property_assessments_SFBayArea.csv"
table_id = "asoba-241019.ingest_tax_assessor." + uri.split('/')[-1].split('.')[0]

load_job = trigger_load_csv_job(bq_schema, uri, table_id)

try:
    load_job.result()
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
except BadRequest:
    print(load_job.errors)
