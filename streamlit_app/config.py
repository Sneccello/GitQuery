import dataclasses
import json


@dataclasses.dataclass
class Config:
    APP_NAME: str
    SPARK_MASTER: str
    HDFS_HOST: str
    HDFS_RPC_PORT: str
    HDFS_HTTP_PORT: str
    HDFS_GITLOGS_PATH: str
    HDFS_SPARK_OUTPUT_ROOT: str

    @staticmethod
    def from_json(filepath):
        with open(filepath, 'r') as f:
            config_data = json.load(f)
            return Config(**config_data)
