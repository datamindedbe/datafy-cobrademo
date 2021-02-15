
def s3_bucket(env: str):
    return 'datafy-training'


def s3_prefix(env: str):
    return 'cobra/results/prototype'


def s3_root(env: str):
    return "s3://" + s3_bucket(env) + '/' + s3_prefix(env)
