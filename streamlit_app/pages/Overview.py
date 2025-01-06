import streamlit as st

from hdfs_utils import get_rdd_folders
from overview_plots import display_commit_activity, display_commits_per_repo, display_commits_per_author, \
    display_filechanges_per_repo, file_status_heatmap
from overview_sidebar import render_sidebar
from session_utils import SessionMeta


def display_filter():

    available_folders = get_rdd_folders(SessionMeta.get_last_hdfs_list_result())

    st.write("## Select Repositories To Analyze")
    options = st.multiselect(
        "What Repositories You Would like to Analyze?",
        available_folders,
        SessionMeta.get_selected_repositories(),
        label_visibility="collapsed"
    )
    if options != SessionMeta.get_selected_repositories():
        SessionMeta.set_selected_repositories(options)
        st.rerun()


def display_default_plots():

    if not SessionMeta.get_selected_repositories():
        st.write("##### No data found :( Add and/or Select repositories to visualize!")
        return

    display_commit_activity()

    col1, _,  col2 = st.columns([10, 1, 10])

    with col1:
        display_commits_per_author()
        display_filechanges_per_repo()
    with col2:
        display_commits_per_repo()
        file_status_heatmap()


def main():
    SessionMeta.setup()
    render_sidebar()
    display_filter()
    display_default_plots()


if __name__ == "__main__":
    main()