import enum
import os
import re
from typing import Optional, List

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from config import Config
from consts import DEFAULT_SQL_QUERY
from hdfs_utils import list_hdfs
from spark_utils import COLUMNS, read_all_records


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
    COMMITS_PER_REPO = enum.auto()
    FILE_STATUS_COUNTS = enum.auto()
    COMMIT_ACTIVITY = enum.auto()
    ACTIVE_AUTHORS = enum.auto()

class SessionHandler:
    _HDFS_LIST_REPO_RESULT = 'key-hdfs_list_result'
    _HDFS_FILE_SIZES = 'key-hdfs_file_sizes'
    _SELECTED_REPOSITORIES = 'key-selected_repositories'
    _QUERY_RESULTS = 'key-query_results'
    _USER_SQL_TABLE_SAMPLE = 'key-user_table_sample'
    _USER_SQL_TABLE_DTYPES = 'key-user_table_dtypes'
    _USER_SQL_QUERY = 'key-sql_query'
    _USER_SQL_RESULT = 'key-sql_result'

    @staticmethod
    def setup():
        if SessionHandler._HDFS_LIST_REPO_RESULT not in st.session_state:
            SessionHandler.set_last_hdfs_repo_list_result(
                list_hdfs(get_config(), get_config().HDFS_SPARK_OUTPUT_ROOT)
            )

            SessionHandler.set_selected_repositories(
                SessionHandler.get_last_hdfs_repo_list_result()
            )
        if SessionHandler._QUERY_RESULTS not in st.session_state:
            st.session_state[SessionHandler._QUERY_RESULTS] = dict()
        if SessionHandler._USER_SQL_QUERY not in st.session_state:
            st.session_state[SessionHandler._USER_SQL_QUERY] = DEFAULT_SQL_QUERY
        if SessionHandler._HDFS_FILE_SIZES not in st.session_state:
            st.session_state[SessionHandler._HDFS_FILE_SIZES] = dict()


    @staticmethod
    def get_last_hdfs_repo_list_result():
        return st.session_state[SessionHandler._HDFS_LIST_REPO_RESULT]

    @staticmethod
    def set_last_hdfs_repo_list_result(ls):
        st.session_state[SessionHandler._HDFS_LIST_REPO_RESULT] = ls

    @staticmethod
    def get_selected_repositories():
        return st.session_state.get(SessionHandler._SELECTED_REPOSITORIES, [])

    @staticmethod
    def set_selected_repositories(hdfs_partitions):
        res = []
        for partition in hdfs_partitions:
            if m := re.match(f'{COLUMNS.REPO_ID.value}=(.+)', partition):
                res.append(m[1])
            elif m := re.match(f'.+_.+', partition):
                res.append(m[0])
        st.session_state[SessionHandler._SELECTED_REPOSITORIES] = list(set(res))

    @staticmethod
    def set_query_results(query_name: QueryNames, result: pd.DataFrame):
        st.session_state[SessionHandler._QUERY_RESULTS][query_name] = result

    @staticmethod
    def get_query_results(query_name: QueryNames) -> Optional[pd.DataFrame]:
        return st.session_state[SessionHandler._QUERY_RESULTS].get(query_name)

    @staticmethod
    def get_user_sql_query():
        return st.session_state[SessionHandler._USER_SQL_QUERY]

    @staticmethod
    def set_user_sql_query(query: str):
        st.session_state[SessionHandler._USER_SQL_QUERY] = query

    @staticmethod
    def set_user_sql_result(df: pd.DataFrame):
        st.session_state[SessionHandler._USER_SQL_RESULT] = df

    @staticmethod
    def get_user_sql_result() -> Optional[pd.DataFrame]:
        return st.session_state.get(SessionHandler._USER_SQL_RESULT)

    @staticmethod
    def get_spark_table_sample() -> Optional[pd.DataFrame]:
        return st.session_state.get(SessionHandler._USER_SQL_TABLE_SAMPLE)

    @staticmethod
    def set_spark_table_sample(df: pd.DataFrame):
        st.session_state[SessionHandler._USER_SQL_TABLE_SAMPLE] = df

    @staticmethod
    def get_spark_table_dtypes() -> Optional[List]:
        return st.session_state.get(SessionHandler._USER_SQL_TABLE_DTYPES)

    @staticmethod
    def set_spark_table_dtypes(df: pd.DataFrame):
        st.session_state[SessionHandler._USER_SQL_TABLE_DTYPES] = df

    @staticmethod
    def unpersist_spark_basetable():
        read_all_records(get_spark_session(), get_config(), SessionHandler.get_last_hdfs_repo_list_result()).unpersist()

    @staticmethod
    def set_hdfs_file_sizes(sizes: dict):
        st.session_state[SessionHandler._HDFS_FILE_SIZES] = sizes

    @staticmethod
    def get_hdfs_file_sizes():
        return st.session_state[SessionHandler._HDFS_FILE_SIZES]

def spark_repo_partition_to_repo_id(hdfs_partitions):
    folders = filter_for_repo_folders(hdfs_partitions)
    return [re.match(f'{COLUMNS.REPO_ID.value}=(.*)', folder)[1] for folder in folders]

def filter_for_repo_folders(hdfs_list: List[str]):
    return [item for item in hdfs_list if re.match(f'{COLUMNS.REPO_ID.value}=', item) is not None]
