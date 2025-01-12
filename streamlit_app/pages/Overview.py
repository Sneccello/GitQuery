import streamlit as st

from overview_plots import display_commit_activity, display_commits_per_repo, display_commits_per_author, \
    display_file_changes_per_commit, display_file_status_counts, refresh_commits_per_author, refresh_commit_activity, \
    refresh_file_status_counts, refresh_file_changes_per_commit, refresh_commits_per_repo
from overview_sidebar import render_sidebar
from session_utils import SessionHandler, spark_repo_partition_to_repo_id, QueryNames, get_spark_session, get_config
from spark_utils import read_all_records


def display_filter():

    available_repos = spark_repo_partition_to_repo_id(SessionHandler.get_last_hdfs_repo_list_result())

    st.write("## Select Repositories To Analyze")
    options = st.multiselect(
        "What Repositories You Would like to Analyze?",
        available_repos,
        SessionHandler.get_selected_repositories(),
        label_visibility="collapsed"
    )
    if options != SessionHandler.get_selected_repositories():
        SessionHandler.set_selected_repositories(options)
        st.rerun()

    refresh = st.button("Refresh Plots")

    if refresh or not SessionHandler.all_plots_available():
        SessionHandler.unpersist_spark_basetable()

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

    display_commit_activity()

    col1, _,  col2 = st.columns([10, 1, 10])

    with col1:
        display_commits_per_author()
        display_file_changes_per_commit()
    with col2:
        display_file_status_counts()
        display_commits_per_repo()


def main():
    SessionHandler.setup()
    render_sidebar()
    if not SessionHandler.get_selected_repositories():
        st.write("##### No data found :( Add repositories to visualize and refresh!")
        return
    display_filter()
    display_default_plots()


if __name__ == "__main__":
    main()