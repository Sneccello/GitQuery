from typing import List

from hdfs import InsecureClient

from session_utils import Config


def get_client(config: Config):
    return InsecureClient(f'http://{config.HDFS_HOST}:{config.HDFS_HTTP_PORT}', user='service')

def upload_to_hdfs(config: Config, source_path: str, destination_path: str):
    client = get_client(config)
    client.delete(destination_path)
    client.upload(destination_path, source_path)

def list_hdfs(config, _dir='/'):
    files = get_client(config).list(_dir)
    return files


def get_rdd_folders(hdfs_list: List[str]):
    return [item for item in hdfs_list if not item.endswith(".gitlog")]