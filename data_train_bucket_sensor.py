import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class data_train_bucket_sensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    """
    # template_fields = ('bucket', 'object')
    # ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self, bucket, input_files, output_files, google_cloud_conn_id='google_cloud_storage_default', delegate_to=None, *args, **kwargs):
        super(data_train_bucket_sensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.input_files = input_files
        self.output_files = output_files
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):

        all_files_exists = True
        list_missing_input_files = list()
        list_missing_output_files = list()
        for input_file in self.input_files:
            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_conn_id,
                                          delegate_to=self.delegate_to)
            if not hook.exists(self.bucket, input_file["feedName"]):
                list_missing_input_files.append(input_file)

        for output_file in self.output_files:
            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_conn_id,
                                          delegate_to=self.delegate_to)
            if not hook.exists(self.bucket, output_file["feedName"]):
                list_missing_output_files.append(output_file)

        if len(list_missing_input_files) > 0:
            logging.info("===========================================================================")
            logging.info(f"missing below input files from upstream in bucket {self.bucket}::")
            for missing_input in list_missing_input_files:
                logging.info(missing_input["feedName"])
            all_files_exists = False
            logging.info("===========================================================================")

        if len(list_missing_output_files) > 0:
            logging.info("===========================================================================")
            logging.info(f"Below output files are yet to be generated in bucket {self.bucket} ::")
            for missing_output in list_missing_output_files:
                logging.info(missing_output["feedName"])
            all_files_exists = False
            logging.info("===========================================================================")

        return all_files_exists
