import sys
from dataclasses import dataclass

import numpy as np 
import pandas as pd

from src.exception import CustomException
from src.logger import logging
import os
from src.utils import save_object,text_preprocessing




@dataclass
class DataTransformationConfig:
    reference_df_obj_file_path=os.path.join('../../artifacts','reference_df.pkl')
    processed_df_obj_file_path=os.path.join('../../artifacts','processed_df.pkl')



class DataTransformationTools:
    @staticmethod
    def skills_processing(x):
        try:
            skills=[]
            skill_arr=x.split(",")
            for skill in skill_arr:
                skills.append("".join(skill.split()))
            logging.info("Skills processing completed")
            return ", ".join(skills).strip()
        except Exception as e:
            logging.info("Exception occured while processing skills")
            raise CustomException(e,sys)

    
    @staticmethod
    def job_title_processing(x):
        try:
            return "".join(x.split(" ")).strip()
        except Exception as e:
            logging.info("Exception occured while processing job title")
            raise CustomException(e,sys)


    @staticmethod
    def experience_processing(x):
        try:
            range_of_years=x.split()[0]
            avg_years_of_exp_req=(float(range_of_years.split("-")[0])+float(range_of_years.split("-")[1]))/2
            if avg_years_of_exp_req > 0 and avg_years_of_exp_req <= 2:
                return "Entry"
            elif avg_years_of_exp_req > 2 and avg_years_of_exp_req <= 6:
                return "Associate"
            elif avg_years_of_exp_req > 6 and avg_years_of_exp_req <= 10:
                return "MidSenior"
            elif avg_years_of_exp_req > 10 and avg_years_of_exp_req <= 16:
                return "Senior"
            elif avg_years_of_exp_req > 16:
                return "Executive"
        except Exception as e:
            logging.info("Exception occured while processing experience")
            raise CustomException(e,sys)

    

    


    
class DataTransformation:
    def __init__(self):
        self.data_transformation_config=DataTransformationConfig()
    
    def initaite_data_transformation(self,data_path):
        try:
            # Reading data
            df = pd.read_csv(data_path)
            logging.info('Data reading completed')

            df.drop("Unnamed: 0",axis=1,inplace=True)
            logging.info("Index column is removed")

            df=df.dropna(axis=0)
            logging.info("Null values are removed")

            df=df.drop_duplicates()
            logging.info("Duplicate rows are removed")

            ref_df=df.copy()
            logging.info("Reference dataframe is stored")

            df.drop("Company_name",axis=1,inplace=True)
            logging.info("Company column is removed")

            df['Skills']=df['Skills'].apply(lambda x:DataTransformationTools.skills_processing(x))
            logging.info("Skill processing completed")

            df['Job_title']=df['Job_title'].apply(lambda x:DataTransformationTools.job_title_processing(x))
            logging.info("Job title processing completed")

            df['Experience']=df['Experience'].apply(lambda x: DataTransformationTools.experience_processing(x))
            logging.info("Experience processing completed")

            df['Tags']=df['Job_title']+" "+df['Skills']+" "+df['Experience']
            logging.info("Tags column created")

            df=df.drop(["Job_title","Skills","Experience"],axis=1)
            logging.info("Dropping job_title, skills, experience columns")

            df['Tags']=df['Tags'].apply(lambda x: text_preprocessing(x))
            logging.info("Text preprocessing done")

            final_processed_df=df.copy()
            logging.info("Storing final df reference")

            save_object(
                file_path=self.data_transformation_config.reference_df_obj_file_path,
                obj=ref_df
            )
            save_object(
                file_path=self.data_transformation_config.processed_df_obj_file_path,
                obj=final_processed_df
            )
            logging.info('reference and processed dataframes saved in pickle file')

            return (
                self.data_transformation_config.reference_df_obj_file_path,
                self.data_transformation_config.processed_df_obj_file_path
            )
            
        except Exception as e:
            logging.info("Exception occured in the initiate_datatransformation")

            raise CustomException(e,sys)