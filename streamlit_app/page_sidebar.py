import os
import subprocess
import threading
import time

import git
import streamlit as st

from git_utils import create_gitlog_file, get_repo_id, get_git_repo_link, CloneProgress
from hdfs_utils import upload_to_hdfs, list_hdfs, get_rdd_folders
from session_utils import get_config, SessionMetaKeys, get_spark_session
from spark_utils import create_gitlog_rdd


def clone_repo_thread(repo_url, clone_dir, progress_obj):
    try:
        # Clone the repository with the CloneProgress object
        git.Repo.clone_from(repo_url, clone_dir, progress=progress_obj)
    except Exception as e:
        st.error(f"Error during clone: {e}")

def display_load_workflow(repo_link: str):
    temp_dir_name = f"temp-dir-{str(hash(repo_link))}"
    temp_dir = os.path.join(os.getcwd(), temp_dir_name)

    cloning_progress = st.progress(0, text='Cloning Repository...')

    progress_obj = CloneProgress()
    clone_thread = threading.Thread(target=clone_repo_thread, args=(repo_link, temp_dir, progress_obj))
    clone_thread.start()

    while True:
        if progress_obj.progress >= 100:
            cloning_progress.progress(100)
            break
        else:
            cloning_progress.progress(progress_obj.progress / 100, text='Cloning Repository...')
        time.sleep(1)

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
        create_gitlog_rdd(get_spark_session() ,get_config(), get_repo_id(repo_link))


def display_add_workflow():
    st.title("Load Repository")
    repo_input = st.text_input(
        "Load Repository",
        placeholder="https://github.com/Sneccello/WordMaze",
        label_visibility="collapsed"
    )
    start_load = st.button("Start Spark Job")
    if start_load:
        repo_link = get_git_repo_link(repo_input)
        display_load_workflow(repo_link)
        st.success("RDD Added")
        refresh_hdfs()
        st.rerun()

def refresh_hdfs():
    res = list_hdfs(get_config(), get_config().HDFS_GITLOGS_PATH)
    st.session_state[SessionMetaKeys.HDFS_LIST_RESULT] = res

def display_hdfs_list():
    st.title("Loaded Repositories")
    refresh = st.button("Refresh HDFS")
    if refresh:
        with st.spinner('Refreshing HDFS...'):
            refresh_hdfs()

    rdd_folders = get_rdd_folders(st.session_state[SessionMetaKeys.HDFS_LIST_RESULT])
    config = get_config()
    for rdd_dir in rdd_folders:
        hdfs_url = f"http://localhost:{config.HDFS_HTTP_PORT}/explorer.html#/{config.HDFS_GITLOGS_PATH}/{rdd_dir}"
        st.markdown(f"[{rdd_dir}]({hdfs_url})")


def render_sidebar():
    with st.sidebar:
        display_add_workflow()
        st.divider()
        display_hdfs_list()
