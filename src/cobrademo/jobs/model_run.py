import tempfile

import awswrangler as wr
import boto3
import joblib
from cobra.evaluation import plot_variable_importance, plot_performance_curves, plot_univariate_predictor_quality, \
    plot_correlation_matrix
from cobra.model_building import univariate_selection, ForwardFeatureSelection
from cobrademo.storage import s3_root, s3_prefix, s3_bucket
from cobrademo.jobs import entrypoint
from cobrademo.jobs.preprocessing import preprocess


@entrypoint("model_run")
def run(env: str, date: str):
    df = wr.s3.read_csv(f"s3://{s3_bucket(env)}/cobra/input/earnings_dataset.csv", delimiter=";")
    basetable = preprocess(df)
    # basetable = wr.s3.read_parquet(s3_root(env) + f"/{date}/basetable", dataset=True)
    model = load_model(date, env)
    predictions = run_model(basetable, model)
    wr.s3.to_parquet(
        df=predictions,
        dataset=True,
        database="cobra",
        path=s3_root(env) + f"/{date}/predictions",
        table="predictions",
        mode='overwrite'
    )


def load_model(date, env):
    s3 = boto3.client('s3')
    key = s3_prefix(env) + f'/{date}/model.pkl'
    s3_response_object = s3.get_object(Bucket='datafy-training', Key=key)
    binary_model = s3_response_object['Body'].read()
    with tempfile.TemporaryFile() as fp:
        fp.write(binary_model)
        fp.seek(0)
        model = joblib.load(fp)
    print(model)
    return model


def run_model(X, model):
    X['prediction'] = model.score_model(X)
    predictions = X
    return predictions
