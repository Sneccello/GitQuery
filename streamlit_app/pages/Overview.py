import streamlit as st

from overview_plots import display_commit_activity, display_commits_per_repo, display_commits_per_author, \
    display_file_changes_per_commit, display_file_status_counts,  refresh_plot_data
from overview_sidebar import render_sidebar
from session_utils import SessionHandler, spark_repo_partition_to_repo_id



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
        refresh_plot_data()
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