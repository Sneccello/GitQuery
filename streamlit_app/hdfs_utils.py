
from hdfs import InsecureClient

from config import Config


def get_client(config: Config) -> InsecureClient:
    return InsecureClient(f'http://{config.HDFS_HOST}:{config.HDFS_HTTP_PORT}', user='service')

def upload_to_hdfs(config: Config, source_path: str, destination_path: str):
    client = get_client(config)
    client.delete(destination_path)
    client.upload(destination_path, source_path)

def list_hdfs(config, _dir='/'):

    client = get_client(config)

    if client.status(config.HDFS_GITLOGS_PATH, strict=False) is None:
        client.makedirs(config.HDFS_GITLOGS_PATH)

    if client.status(config.HDFS_SPARK_OUTPUT_ROOT, strict=False) is None:
        client.makedirs(config.HDFS_SPARK_OUTPUT_ROOT)

    files = client.list(_dir)
    return files

def remove_path_if_exists(config, root_dir):
    client = get_client(config)
    if client.status(root_dir, strict=False) is not None:
        client.delete(root_dir, recursive=True)

