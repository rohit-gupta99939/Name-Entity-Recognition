import os
import sys
from zipfile import ZipFile
from ner.configuration.gcloud import GCloud
from ner.constant import *
from ner.entity.artifact_entry  import DataIngestionArtifacs
from ner.entity.config_entry import DataIngestionConfig
from ner.exception import NerException
from ner.logger import logging

class DataIngestion:
    def __init__(
        self, data_ingestion_config : DataIngestionConfig, gcloud : GCloud
        )-> None:
        self.data_ingestion_config = data_ingestion_config
        self.gcloud = gcloud


    def get_data_from_gcp(self, bucket_name: str, file_name: str, path: str)-> ZipFile:
        """
        This method is used to get data from gcloud bucket
        """
        logging.info("Getting data from gcloud bucket")
        try:
            self.gcloud.sync_folder_from_gcloud(gcp_bucket_url= bucket_name, filename=file_name, destination=path)
            logging.info(f"Data from gcloud bucket: {file_name} is downloaded")
        except Exception as e:
            raise NerException(e,sys) from e
        

    def extract_data(self, input_file_path: str, output_file_path: str)->None:
        """
        This method is used to extract data from zip file
        """
        logging.info("Extracting data from zip file")
        try:
            with ZipFile(input_file_path, 'r') as zip:
                zip.extractall(output_file_path)
                logging.info(f"Data from zip file: {input_file_path} is extracted")
        except Exception as e:
            raise NerException(e,sys) from e
        

    def initiate_data_ingestion(self)-> DataIngestionArtifacs:
        """
        This method is used to initiate data ingestion
        """
        logging.info("Initiating data ingestion")
        try:
            os.makedirs(
                self.data_ingestion_config.data_ingestion_artifacts_dir, exist_ok=True
            )
            logging.info(f"Creating directory: {os.path.basename(self.data_ingestion_config.data_ingestion_artifacts_dir)}")

            #Geting data from Gcp
            self.get_data_from_gcp(
                bucket_name= BUCKET_NAME,
                file_name= GCP_DATA_FILE_NAME,
                path= self.data_ingestion_config.gcp_data_file_path
            )

            logging.info(f"Got the file from Gcp: {os.path.basename(self.data_ingestion_config.data_ingestion_artifacts_dir)}")
            
            self.extract_data(
                input_file_path= self.data_ingestion_config.gcp_data_file_path,
                output_file_path= self.data_ingestion_config.output_file_path,
            )
            logging.info(f"Extracted the file from Gcp: {os.path.basename(self.data_ingestion_config.data_ingestion_artifacts_dir)}")
            
            data_ingestion_artifact = DataIngestionArtifacs(
                zip_data_file_path= self.data_ingestion_config.gcp_data_file_path,
                csv_data_file_path= self.data_ingestion_config.csv_data_file_name,
            )

            return data_ingestion_artifact
        except Exception as e:
            raise NerException(e,sys) from e