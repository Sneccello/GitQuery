import streamlit as st

from overview_plots import display_commit_activity, display_commits_per_author, \
    display_file_status_counts, refresh_plot_data, display_active_authors
from overview_sidebar import render_sidebar
from session_utils import SessionHandler, spark_repo_partition_to_repo_id, QueryNames


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

    if refresh:
        refresh_plot_data()
        st.rerun()

def display_default_plots():


    display_active_authors()
    col1, _,  col2 = st.columns([10, 1, 10])

    with col1:
        display_commits_per_author()
    with col2:
        display_file_status_counts()

    display_commit_activity()

def main():
    SessionHandler.setup()
    render_sidebar()
    if not SessionHandler.get_selected_repositories():
        st.write("##### No data found :( Add repositories to visualize and refresh!")
        return

    display_filter()
    if SessionHandler.get_query_results(QueryNames.COMMIT_ACTIVITY) is None:
        refresh_plot_data()

    display_default_plots()


if __name__ == "__main__":
    main()