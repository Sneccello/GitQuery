import os

import streamlit as st
from pyspark.sql import SparkSession

from config import Config
from hdfs_utils import list_hdfs, get_rdd_folders


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


class SessionMeta:
    _HDFS_LIST_RESULT = 'key-hdfs_list_result'
    _SELECTED_REPOSITORIES = 'key-_hdfs_list_result'

    @staticmethod
    def setup():
        SessionMeta.set_last_hdfs_list_result(
            list_hdfs(get_config(), get_config().HDFS_GITLOGS_PATH)
        )

        SessionMeta.set_selected_repositories(
            get_rdd_folders(SessionMeta.get_last_hdfs_list_result())
        )


    @staticmethod
    def get_last_hdfs_list_result():
        return st.session_state[SessionMeta._HDFS_LIST_RESULT]

    @staticmethod
    def set_last_hdfs_list_result(ls):
        st.session_state[SessionMeta._HDFS_LIST_RESULT] = ls

    @staticmethod
    def get_selected_repositories():
        return st.session_state[SessionMeta._SELECTED_REPOSITORIES]

    @staticmethod
    def set_selected_repositories(repositories):
        st.session_state[SessionMeta._SELECTED_REPOSITORIES] = get_rdd_folders(repositories)

