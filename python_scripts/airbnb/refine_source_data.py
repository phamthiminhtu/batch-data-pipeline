import os
import logging
import pandas as pd
import numpy as np
import glob


def add_ingestion_timestamp(
        input_path,
        output_path,
        columns,
        **kwargs
    ):
    glob_list = glob.glob(f'{input_path}*')
    for file in glob_list:
        # generate dataframe by combining files
        df = pd.read_csv(file)

        # replace NaN with None
        # need to cast to object before converting NaN to None
        # https://github.com/pandas-dev/pandas/issues/42423
        df = df.astype(object).where(df.notnull(), None)
        # add ingestion_timestamp
        df['ingestion_timestamp'] = pd.Timestamp("now")
        columns += ',ingestion_timestamp'

        output_file_path = f'{output_path}/{file}'
        logging.info(f"Successfully saved file at: <{output_file_path}>")
        df.to_csv(output_file_path)
