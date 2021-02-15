import logging

import pandas as pd
import requests
from typing import Optional

from cobra.evaluation import generate_pig_tables
from cobra.preprocessing import PreProcessor
from cobrademo.jobs import entrypoint
from cobrademo.storage import s3_root, s3_bucket
import awswrangler as wr


@entrypoint("preprocessing")
def run(env: str, date: str):
    df = wr.s3.read_csv(f"s3://{s3_bucket(env)}/cobra/input/earnings_dataset.csv", delimiter=";")

    basetable = preprocess(df)

    wr.s3.to_parquet(
        df=basetable,
        dataset=True,
        database="cobra",
        path=s3_root(env) + f"/{date}/basetable",
        table="basetable",
        mode='overwrite'
    )


def preprocess(df):
    # create instance of PreProcessor from parameters
    df.columns = df.columns.str.replace("-", "_")
    df.columns = df.columns.str.lower()
    preprocessor = PreProcessor.from_params(
        n_bins=10, strategy="quantile", serialization_path="pipeline.json"
    )

    # split data into train-selection-validation set
    basetable = preprocessor.train_selection_validation_split(
        data=df,
        target_column_name="target",
        train_prop=0.8,
        selection_prop=0.1,
        validation_prop=0.1,
    )

    basetable.head(n=5)
    # We need to create a list of variables by their datatype
    continuous_vars = [
        "age",
        "education_num",
        "capital_gain",
        "capital_loss",
        "hours_per_week",
    ]

    discrete_vars = [
        "workclass",
        "fnlwgt",
        "education",
        "marital_status",
        "occupation",
        "relationship",
        "race",
        "sex",
        "native_country",
    ]

    target_column_name = "target"
    # fit the pipeline
    preprocessor.fit(
        basetable[basetable["split"] == "train"],
        continuous_vars=continuous_vars,
        discrete_vars=discrete_vars,
        target_column_name=target_column_name,
    )

    # transform the data (e.g. perform discretisation, incidence replacement, ...)
    basetable = preprocessor.transform(
        basetable, continuous_vars=continuous_vars, discrete_vars=discrete_vars
    )
    return basetable
