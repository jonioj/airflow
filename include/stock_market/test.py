from minio import Minio


client = Minio(
    endpoint='127.0.0.1:9000',
    access_key='minio',
    secret_key='minio123',
    secure=False

)
bucket_name = 'stock_market'
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
