import streamlit as st

from hdfs_utils import get_rdd_folders
from overview_plots import display_commit_activity, display_commits_per_repo, display_commits_per_author, \
    display_file_changes_per_commit, display_file_status_counts, refresh_commits_per_author, refresh_commit_activity, \
    refresh_file_status_counts, refresh_file_changes_per_commit, refresh_commits_per_repo
from overview_sidebar import render_sidebar
from session_utils import SessionMeta


def display_filter():

    available_folders = get_rdd_folders(SessionMeta.get_last_hdfs_repo_list_result())

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

    refresh = st.button("Refresh Plots")

    if refresh or not SessionMeta.all_plots_available():
        refresh_functions = [
            refresh_commits_per_author,
            refresh_commits_per_repo,
            refresh_commit_activity,
            refresh_file_changes_per_commit,
            refresh_file_status_counts
        ]
        QUERYING_WITH_SPARK = "Querying with Spark.. (See Jobs at [http://localhost:4040](http://localhost:4040))"

        query_pbar = st.progress(0, text=QUERYING_WITH_SPARK)
        n_refresh = len(refresh_functions)
        for idx, fn in enumerate(refresh_functions):
            fn()
            percentage = (idx+1) / n_refresh
            query_pbar.progress(percentage, text=QUERYING_WITH_SPARK)
        query_pbar.progress(100, text='Querying with Spark done!')
        st.rerun()

def display_default_plots():

    if not SessionMeta.all_plots_available():
        st.write("##### No data found :( Add repositories to visualize and refresh!")
        return

    display_commit_activity()

    col1, _,  col2 = st.columns([10, 1, 10])

    with col1:
        display_commits_per_author()
        display_file_changes_per_commit()
    with col2:
        display_file_status_counts()
        display_commits_per_repo()


def main():
    SessionMeta.setup()
    render_sidebar()
    display_filter()
    display_default_plots()


if __name__ == "__main__":
    main()