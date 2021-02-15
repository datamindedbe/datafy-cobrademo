import boto3
from cobrademo.jobs import preprocessing, model_training, pig_tables, model_run

boto3.setup_default_session(profile_name="datafydemo")
env = "local"
date = "2020-02-12"
print("preprocessing...")
preprocessing.run(env, date)
print("generating pig_tables...")
pig_tables.run(env, date)
print("model_training...")
model_training.run(env, date)
print("Evaluation...")
model_run.run(env, date)
print("Done.")