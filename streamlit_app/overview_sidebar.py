import os
import re
import shutil
import subprocess
import time
from typing import List

import requests
import streamlit as st

from consts import PARTITION_COLUMNS
from git_utils import create_gitlog_file, get_repo_id, get_git_repo_link
from hdfs_utils import upload_to_hdfs, list_hdfs, remove_path_if_exists, get_file_size_mb
from session_utils import get_config, get_spark_session, SessionHandler, spark_repo_partition_to_repo_id
from spark_utils import create_gitlog_rdd, COLUMNS, get_output_root_folder


def format_time_elapsed(seconds: float) -> str:
    partial_seconds = round(seconds - int(seconds), 1)
    minutes, seconds = divmod(int(seconds), 60)
    seconds += partial_seconds
    return f"{minutes}m{seconds}s" if minutes > 0 else f"{seconds}s"

def display_load_workflow(repo_link: str, partition_by: List):
    temp_dir_name = f"temp-dir-{str(hash(repo_link))}-{time.time()}"
    temp_dir = os.path.join('/tmp/GitQuery/', temp_dir_name)

    start = time.time()
    with st.spinner('Cloning Repository...'):
        subprocess.run(['git', 'clone', repo_link, temp_dir], check=True)
    st.write(f'Cloning Repository... DONE ({format_time_elapsed(time.time()-start)})')

    start = time.time()
    with st.spinner('Generating Gitlog file...'):
        output_filepath = f"{get_repo_id(repo_link)}.gitlog"
        create_gitlog_file(temp_dir, output_filepath)
        shutil.rmtree(temp_dir)
    st.write(f'Generating Gitlog file... DONE ({format_time_elapsed(time.time()-start)})')

    start = time.time()
    with st.spinner('Uploading Gitlog to HDFS...'):
        upload_to_hdfs(
            get_config(),
            output_filepath,
            os.path.join(get_config().HDFS_GITLOGS_PATH, output_filepath)
        )
        subprocess.run(['rm', output_filepath], check=True)
    st.write(f'Uploading Gitlog to HDFS... DONE ({format_time_elapsed(time.time()-start)})')

    start = time.time()
    with st.spinner('Transforming textfile with Spark... (might take a while)'):
        config = get_config()
        repo_id = get_repo_id(repo_link)
        remove_path_if_exists(config, f'{get_output_root_folder(config)}{COLUMNS.REPO_ID.value}={repo_id}')
        create_gitlog_rdd(get_spark_session() ,get_config(), get_repo_id(repo_link), partition_by)
    st.write(f'Transforming textfile with Spark... DONE ({format_time_elapsed(time.time()-start)})')



def repo_looks_valid(repo_link):

    github_gitlab_regex = \
        '^(https:\/\/(?:www\.)?(github\.com|gitlab\.com)\/[A-Za-z0-9_\-]+\/[A-Za-z0-9_\-]+(?:\.git)?\/?)$'

    if not re.match(github_gitlab_regex, repo_link):
        return False
    try:
        response = requests.get(repo_link)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def display_add_workflow():
    st.markdown("<h2 style='color:#8e44ad;'>📥 Load Repository</h2>", unsafe_allow_html=True)
    repo_input = st.text_input(
        "Load Repository",
        placeholder="https://github.com/streamlit/streamlit",
        label_visibility="collapsed"
    )



    st.write("**Data will be partitioned by**:")
    for i, col in enumerate(PARTITION_COLUMNS, 1):
        st.write(f"{i}. {col}")

    start_load = st.button("🚀 Start Ingest Job")
    if start_load:
        repo_link = get_git_repo_link(repo_input)
        is_ok = repo_looks_valid(repo_link)
        if not is_ok:
            st.error(f'"{repo_link}" does not look like a valid Github/Gitlab repository')
            return
        display_load_workflow(repo_link, PARTITION_COLUMNS)
        refresh_hdfs()

def refresh_hdfs():

    new_list = list_hdfs(get_config(), get_config().HDFS_SPARK_OUTPUT_ROOT)

    SessionHandler.set_last_hdfs_repo_list_result(new_list)

    SessionHandler.set_selected_repositories(
        list(set(new_list + SessionHandler.get_selected_repositories()))
    )
    return new_list

def display_hdfs_list():
    st.markdown("<h2 style='color:#3498db;'>📂 Loaded Repositories</h2>", unsafe_allow_html=True)
    refresh = st.button("🔄 Refresh HDFS")
    if refresh:
        with st.spinner('Refreshing HDFS...'):
            refresh_hdfs()

    repo_names = spark_repo_partition_to_repo_id(SessionHandler.get_last_hdfs_repo_list_result())
    config = get_config()
    for repo_name in repo_names:
        hdfs_url = f"http://localhost:{config.HDFS_HTTP_PORT}/explorer.html#/{config.HDFS_SPARK_OUTPUT_ROOT}/repo_id={repo_name}"
        size = get_file_size_mb(config, f'{config.HDFS_GITLOGS_PATH}/{repo_name}.gitlog')
        st.markdown(f"[{repo_name}]({hdfs_url}) ({size}MB)")


def render_sidebar():
    with st.sidebar:
        display_add_workflow()
        st.divider()
        display_hdfs_list()
