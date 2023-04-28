
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from sys import argv
options = beam.options.pipeline_options.PipelineOptions()
gcloud_options = options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions)
gcloud_options.job_name = 'jobname'
gcloud_options.project = 'project'
gcloud_options.region = 'us-central1'
gcloud_options.staging_location = 'gs://......'
gcloud_options.temp_location = 'gs://......'
worker_options = options.view_as(beam.options.pipeline_options.WorkerOptions)
worker_options.disk_size_gb = 25
worker_options.max_num_workers = 2
# worker_options.num_workers = 2
# worker_options.machine_type = 'n1-standard-8'
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'


p1 = beam.Pipeline(options=options)

# Read froM GCS and write to BigQuery
(p1 | 'read' >> beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
 | 'write1' >> beam.io.WriteToBigQuery(
            'project:dataset.table', schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
 )

p1.run()  # .wait_until_finish()
