import dataclasses
import enum
import json
import os

import streamlit as st
from pyspark.sql import SparkSession


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

@st.cache_resource
def get_spark_session():
    config = get_config()
    session = SparkSession.builder \
        .master(config.SPARK_MASTER) \
        .appName(config.APP_NAME) \
        .getOrCreate()
    for file in os.listdir(os.getcwd()):
        if file.endswith('.py'):
            session.sparkContext.addPyFile(file)
    return session


class SessionMetaKeys(enum.Enum):
    HDFS_LIST_RESULT = enum.auto()
    SELECTED_REPOSITORIES = enum.auto()
