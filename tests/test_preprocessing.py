import pandas as pd
from cobrademo.jobs import preprocessing


def test_basetable_is_created_from_csv_file():
    df = pd.read_csv("tests/resources/earnings_dataset.csv")
    basetable = preprocessing.preprocess(df)
    assert basetable is not None
