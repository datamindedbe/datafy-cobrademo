import logging

import pandas as pd
import requests
from typing import Optional

from cobra.evaluation import generate_pig_tables
from cobra.preprocessing import PreProcessor
from cobrademo.storage import s3_root
from cobrademo.jobs import entrypoint
import awswrangler as wr


@entrypoint("pig_tables")
def run(env: str, date: str):
    basetable = wr.s3.read_parquet(s3_root(env) + f"/{date}/basetable", dataset=True)
    pig_tables = get_pig_tables(basetable)

    wr.s3.to_parquet(
        df=pig_tables,
        dataset=True,
        database="cobra",
        path=s3_root(env) + f"/{date}/pig_tables",
        table="pig_tables",
        mode='overwrite'
    )


def get_pig_tables(basetable):
    target_column_name = "target"
    predictor_list = [col for col in basetable.columns
                      if col.endswith("_bin") or col.endswith("_processed")]

    pig_tables = generate_pig_tables(
        basetable[basetable["split"] == "selection"],
        id_column_name='id',
        target_column_name=target_column_name,
        preprocessed_predictors=predictor_list)
    return pig_tables