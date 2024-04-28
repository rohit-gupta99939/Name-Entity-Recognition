import sys
from ner.components.data_ingestion import DataIngestion
from ner.components.data_transformation import DataTransformation
from ner.components.model_trainer import ModelTraining
from ner.components.model_evaluation import ModelEvaluation
from ner.components.model_pusher import ModelPusher
from ner.configuration.gcloud import GCloud
from ner.constant import *

from ner.entity.artifact_entry import (
    DataIngestionArtifacs,DataTransformationArtifacts, ModelTrainingArtifacts,ModelEvaluationArtifacts,ModelPusherArtifacts
)

from ner.entity.config_entry import (
    DataIngestionConfig,DataTransformationConfig, ModelTrainingConfig, ModelPusherConfig,ModelEvalConfig,ModelPredictorConfig
)

from ner.logger import logging
from ner.exception import NerException

class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_training_config = ModelTrainingConfig()
        self.model_evaluation_config = ModelEvalConfig()
        self.model_pusher_config = ModelPusherConfig()
        self.gcloud = GCloud()


    def start_data_ingestion(self)-> DataIngestionArtifacs:
        try:
            data_ingestion = DataIngestion(data_ingestion_config= self.data_ingestion_config, gcloud=self.gcloud)
            data_ingestion_artifacs = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifacs
        except NerException as e:
            raise NerException(e,sys) from e
        

    def start_data_transformation(
            self, data_ingestion_artifact : DataIngestionArtifacs )-> DataTransformationArtifacts:
        logging.info("Starting data transformation")
        try:
            data_transformation = DataTransformation(
                data_transformation_config= self.data_transformation_config, 
                data_ingestion_artifacts= data_ingestion_artifact)
            
            data_transformation_artifacts = data_transformation.initiate_data_transformation()
            return data_transformation_artifacts
        except Exception as e:
            raise NerException(e,sys) from e


    def start_model_training(
            self, data_transformation_artifact : DataTransformationArtifacts)-> ModelTrainingArtifacts:
        logging.info("Starting model training")
        try:
            model_training = ModelTraining(
                model_trainer_config= self.model_training_config, 
                data_transformation_artifacts= data_transformation_artifact)
            
            model_training_artifacts = model_training.initiate_model_training()
            logging.info("completed model training")
            return model_training_artifacts
        except Exception as e:
            raise NerException(e,sys) from e


    def start_model_evaluation(
            self,
            data_transformation_artifact: DataTransformationArtifacts,
            model_trainer_artifact: ModelTrainingArtifacts)-> ModelEvaluationArtifacts:
        try:
            logging.info("Executing model evaluation")

            model_evaluation = ModelEvaluation(
                data_transformation_artifacts= data_transformation_artifact,
                model_training_artifacts= model_trainer_artifact, 
                model_evaluation_config= self.model_evaluation_config)
            
            model_evaluation_artifacts = model_evaluation.initiate_model_evaluation()
            logging.info("completed model evaluation")
            return model_evaluation_artifacts
        
        except Exception as e:
            raise NerException(e,sys) from e
        


    def start_model_pusher(
            self,
            model_evaluation_artifact: ModelEvaluationArtifacts)-> ModelPusherArtifacts:
        try:
            logging.info("Starting model pusher")
            model_pusher = ModelPusher(
                model_evaluation_artifact= model_evaluation_artifact, 
                model_pusher_config= self.model_pusher_config)
            
            model_pusher_artifacts = model_pusher.initiate_model_pusher()
            logging.info("completed model pusher")
            return model_pusher_artifacts
        
        except Exception as e:
            raise NerException(e,sys) from e


    def run_pipeline(self)-> None:
        try:
            logging.info("started data ingestion =============================")
            data_ingestion_artifacs = self.start_data_ingestion()
            data_transformation_artifacts = self.start_data_transformation(
                data_ingestion_artifact = data_ingestion_artifacs
            )
            model_training_artifacts = self.start_model_training(
                data_transformation_artifact = data_transformation_artifacts
            )
            model_evaluation_artifacts = self.start_model_evaluation(
                data_transformation_artifact = data_transformation_artifacts,
                model_trainer_artifact = model_training_artifacts
            )
            model_pusher_artifacts = self.start_model_pusher(
                model_evaluation_artifact = model_evaluation_artifacts
            )

        except NerException as e:
            raise NerException(e,sys) from e
