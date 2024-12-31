import os
import subprocess

import streamlit as st

from git_utils import clone_repository, create_gitlog_file, get_repo_id, get_git_clone_link
from hdfs_utils import upload_to_hdfs, list_hdfs
from session_utils import get_config, SessionMetaKeys
from spark_utils import create_gitlog_rdd


def display_load_workflow(repo_link: str):
    temp_dir_name = f"temp-dir-{str(hash(repo_link))}"
    temp_dir = os.path.join(os.getcwd(), temp_dir_name)


    with st.spinner('Cloning Repository...'):
        clone_repository(repo_link, temp_dir)

    with st.spinner('Creating Gitlog files...'):
        output_filename = f"{get_repo_id(repo_link)}.gitlog"
        create_gitlog_file(temp_dir, output_filename)
        subprocess.run(['rm', '-rf', temp_dir_name], check=True)

    with st.spinner('Uploading Gitlog to HDFS...'):
        upload_to_hdfs(
            get_config(),
            output_filename,
            os.path.join(get_config().HDFS_GITLOGS_PATH, output_filename)
        )

    with st.spinner('Creating gitlog RDD with PySpark'):
        create_gitlog_rdd(get_config(), get_repo_id(repo_link))


def display_add_workflow():
    st.title("Load Repository")
    repo_input = st.text_input(
        "Load Repository",
        placeholder="https://github.com/Sneccello/WordMaze",
        label_visibility="collapsed"
    )
    start_load = st.button("Start Job")
    if start_load:
        repo_link = get_git_clone_link(repo_input)
        display_load_workflow(repo_link)
        st.success("RDD Added")
        refresh_hdfs()
        st.rerun()

def refresh_hdfs():
    res = list_hdfs(get_config(), get_config().HDFS_GITLOGS_PATH)
    st.session_state[SessionMetaKeys.HDFS_LIST_RESULT] = res


def display_hdfs_list():
    st.title("HDFS List")
    refresh = st.button("Refresh HDFS")
    if refresh:
        with st.spinner('Refreshing HDFS...'):
            refresh_hdfs()
    for file_or_dir in st.session_state[SessionMetaKeys.HDFS_LIST_RESULT]:
        st.write(file_or_dir)


def render_sidebar():
    with st.sidebar:
        display_add_workflow()
        st.divider()
        display_hdfs_list()
