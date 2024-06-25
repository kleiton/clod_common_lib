import logging
import os
import pandas as pd

from src.COSManipulation import COSManipulation
from src.Logging import Logging
from src.MongoManipulation import MongoManipulation
from src.FileManipulation import FileManipulation
from src.PandasDFManipulation import PandasDFManipulation
from src.PostgresManipulation import PostgresManipulation
from src.Utils import Utils

class Main:
    def __init__(self):
        pass

    @staticmethod
    def run():
        Logging.initiate_log(log_file_mode='w', log_handle_mode='y')
        cos_manipulation = COSManipulation()
        
        mongo_manipulation = MongoManipulation()
        mongo_manipulation.initiate_connection()

        file_manipulation = FileManipulation()

        pandas_manipulation = PandasDFManipulation()
        
        postgres_manipulation = PostgresManipulation()
        postgres_manipulation.connect_postgres()
        
        utils = Utils()

if __name__ == '__main__':
    main = Main()
    main.run()