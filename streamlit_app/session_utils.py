import dataclasses
import enum
import json
import streamlit as st


@dataclasses.dataclass
class Config:
    APP_NAME: str
    SPARK_MASTER: str
    HDFS_HOST: str
    HDFS_RPC_PORT: str
    HDFS_HTTP_PORT: str
    HDFS_GITLOGS_PATH: str

    @staticmethod
    def from_json(filepath):
        with open(filepath, 'r') as f:
            config_data = json.load(f)
            return Config(**config_data)

@st.cache_data
def get_config():
    return Config.from_json('config.json')



class SessionMetaKeys(enum.Enum):
    HDFS_LIST_RESULT = enum.auto()
    SELECTED_REPOSITORIES = enum.auto()
