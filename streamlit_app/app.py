import streamlit as st
from hdfs_utils import list_hdfs, get_rdd_folders
from page_plots import display_commit_activity, display_commits_per_repo, display_commits_per_author, \
    display_filechanges_per_repo
from page_sidebar import render_sidebar
from session_utils import get_config, SessionMetaKeys


#TODO 24/12/31 13:23:09 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.


def setup():

    st.set_page_config(
        page_title="GitInsights",
        page_icon="🧊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if SessionMetaKeys.HDFS_LIST_RESULT not in st.session_state:
        st.session_state[SessionMetaKeys.HDFS_LIST_RESULT] = list_hdfs(get_config(), get_config().HDFS_GITLOGS_PATH)

    if SessionMetaKeys.SELECTED_REPOSITORIES not in st.session_state:
        st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES] =\
            get_rdd_folders(st.session_state[SessionMetaKeys.HDFS_LIST_RESULT])


def display_filter():
    hdfs_list = st.session_state[SessionMetaKeys.HDFS_LIST_RESULT]

    rdd_folders = get_rdd_folders(hdfs_list)

    st.write("## Select Repositories To Analyze")
    options = st.multiselect(
        "What Repositories You Would like to Analyze?",
        rdd_folders,
        rdd_folders,
        label_visibility="collapsed"
    )
    if options != st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES]:
        st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES] = options
        st.rerun()

def display_default_plots():

    display_commit_activity()

    col1, _,  col2 = st.columns([10, 1, 10])

    with col1:
        display_commits_per_author()
        display_filechanges_per_repo()
    with col2:
        display_commits_per_repo()


def main():

    setup()
    render_sidebar()
    display_filter()
    display_default_plots()


if __name__ == "__main__":
    main()