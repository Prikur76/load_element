from environs import Env

env = Env()
env.read_env()


S3_CONFIG = {
    'aws_access_key_id': env.str('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': env.str('AWS_SECRET_ACCESS_KEY'),
    'endpoint_url': env.str('S3_ENDPOINT_URL'),
    'region_name': env.str('REGION_NAME')
}
S3_BUCKET = env.str('BUCKET_NAME', default='my-bucket')

element_params = {
    'username': env.str('LOGIN'),
    'password': env.str('PASSWORD'),
    'base_url': env.str('BASE_URL')
}
