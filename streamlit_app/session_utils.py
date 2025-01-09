import enum
import os
from typing import Optional

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from config import Config
from consts import DEFAULT_SQL_QUERY
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


class QueryNames(enum.Enum):
    COMMITS_PER_AUTHOR = enum.auto()
    FILECHANGES_PER_COMMIT = enum.auto()
    COMMITS_PER_REPO = enum.auto()
    FILE_STATUS_COUNTS = enum.auto()
    COMMIT_ACTIVITY = enum.auto()

class SessionMeta:
    _HDFS_LIST_REPO_RESULT = 'key-hdfs_list_result'
    _SELECTED_REPOSITORIES = 'key-_hdfs_list_result'
    _QUERY_RESULTS = 'key-query_results'
    _USER_SQL_QUERY = 'key-sql_query'
    _USER_SQL_RESULT = 'key-sql_result'

    @staticmethod
    def setup():
        if SessionMeta._HDFS_LIST_REPO_RESULT not in st.session_state:
            SessionMeta.set_last_hdfs_repo_list_result(
                list_hdfs(get_config(), get_config().HDFS_GITLOGS_PATH)
            )

            SessionMeta.set_selected_repositories(
                get_rdd_folders(SessionMeta.get_last_hdfs_repo_list_result())
            )

            st.session_state[SessionMeta._QUERY_RESULTS] = dict()

            st.session_state[SessionMeta._USER_SQL_QUERY] = DEFAULT_SQL_QUERY
            st.session_state[SessionMeta._USER_SQL_RESULT] = None


    @staticmethod
    def get_last_hdfs_repo_list_result():
        return st.session_state[SessionMeta._HDFS_LIST_REPO_RESULT]

    @staticmethod
    def set_last_hdfs_repo_list_result(ls):
        st.session_state[SessionMeta._HDFS_LIST_REPO_RESULT] = get_rdd_folders(ls)

    @staticmethod
    def get_selected_repositories():
        return st.session_state[SessionMeta._SELECTED_REPOSITORIES]

    @staticmethod
    def set_selected_repositories(repositories):
        st.session_state[SessionMeta._SELECTED_REPOSITORIES] = get_rdd_folders(repositories)

    @staticmethod
    def set_query_results(query_name: QueryNames, result: pd.DataFrame):
        st.session_state[SessionMeta._QUERY_RESULTS][query_name] = result

    @staticmethod
    def get_query_results(query_name: QueryNames) -> Optional[pd.DataFrame]:
        return st.session_state[SessionMeta._QUERY_RESULTS].get(query_name)

    @staticmethod
    def get_user_sql_query():
        return st.session_state[SessionMeta._USER_SQL_QUERY]

    @staticmethod
    def set_user_sql_query(query: str):
        st.session_state[SessionMeta._USER_SQL_QUERY] = query

    @staticmethod
    def set_user_sql_result(df: pd.DataFrame):
        st.session_state[SessionMeta._USER_SQL_RESULT] = df

    @staticmethod
    def get_user_sql_result() -> Optional[pd.DataFrame]:
        return st.session_state[SessionMeta._USER_SQL_RESULT]

    @staticmethod
    def all_plots_available():
        return all([SessionMeta.get_query_results(result_enum) is not None for result_enum in QueryNames])