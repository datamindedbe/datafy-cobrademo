import tempfile

import awswrangler as wr
import boto3
import joblib
from cobra.evaluation import plot_variable_importance, plot_performance_curves, plot_univariate_predictor_quality, \
    plot_correlation_matrix
from cobra.model_building import univariate_selection, ForwardFeatureSelection
from cobrademo.storage import s3_root, s3_prefix
from cobrademo.jobs import entrypoint


@entrypoint("model_training")
def run(env: str, date: str):
    basetable = wr.s3.read_parquet(s3_root(env) + f"/{date}/basetable", dataset=True)
    basetable = fix_types(basetable)
    preselected_predictors = feature_selection(basetable)
    model = train_model(basetable, preselected_predictors)

    s3 = boto3.client('s3')
    key = s3_prefix(env) + f'/{date}/model.pkl'
    print(key)
    with tempfile.TemporaryFile() as fp:
        joblib.dump(model, fp)
        fp.seek(0)
        s3.put_object(Body=fp.read(), Bucket='datafy-training', Key=key)
    wr.s3.to_parquet(
        df=basetable,
        dataset=True,
        database="cobra",
        path=s3_root(env) + f"/{date}/basetable",
        table="basetable",
        mode='overwrite'
    )


def fix_types(basetable):
    int64_conversion = {c: 'int64' for c in basetable.select_dtypes(include=['Int64']).columns}
    basetable = basetable.astype(int64_conversion)
    return basetable


def feature_selection(basetable):
    target_column_name = "target"
    preprocessed_predictors = [
        col for col in basetable.columns.tolist() if "_enc" in col
    ]

    df_auc = univariate_selection.compute_univariate_preselection(
        target_enc_train_data=basetable[basetable["split"] == "train"],
        target_enc_selection_data=basetable[basetable["split"] == "selection"],
        predictors=preprocessed_predictors,
        target_column=target_column_name,
        preselect_auc_threshold=0.53,
        preselect_overtrain_threshold=0.05,
    )
    plot_univariate_predictor_quality(df_auc)
    # get a list of predictors selected by the univariate selection
    preselected_predictors = univariate_selection.get_preselected_predictors(df_auc)

    # compute correlations between preprocessed predictors
    df_corr = univariate_selection.compute_correlations(
        basetable[basetable["split"] == "train"], preprocessed_predictors
    )
    plot_correlation_matrix(df_corr)
    print("preselected predictors: " + ", ".join(preselected_predictors))
    return preselected_predictors


def train_model(basetable, preselected_predictors):
    target_column_name = "target"
    forward_selection = ForwardFeatureSelection(max_predictors=30,
                                                pos_only=True)

    forward_selection.fit(basetable[basetable["split"] == "train"],
                          target_column_name,
                          preselected_predictors)

    performances = (forward_selection
                    .compute_model_performances(basetable, target_column_name))
    plot_performance_curves(performances)

    # after plotting the performances and selecting the model,
    model = forward_selection.get_model_from_step(4)

    # we have chosen model with 5 variables, which we extract as follows
    final_predictors = model.predictors

    # we can also compute and plot the importance of each predictor in the model:
    variable_importance = model.compute_variable_importance(
        basetable[basetable["split"] == "selection"])

    # this is correlation of the model score and predictor
    plot_variable_importance(variable_importance)
    return model
