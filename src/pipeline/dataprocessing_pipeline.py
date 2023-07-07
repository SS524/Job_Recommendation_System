
import os
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd

from src.components.data_ingestion import DataIngestion
from src.components.data_transformation import DataTransformation



if __name__=='__main__':
    obj=DataIngestion()
    data_path=obj.initiate_data_ingestion()
    data_transformation = DataTransformation()
    refdf_path,processeddf_path=data_transformation.initaite_data_transformation(data_path)
  



