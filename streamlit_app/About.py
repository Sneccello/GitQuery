import streamlit as st
from hdfs_utils import get_rdd_folders, list_hdfs
from session_utils import get_config, SessionMetaKeys

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
        st.write("rload")
        st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES] =\
            get_rdd_folders(st.session_state[SessionMetaKeys.HDFS_LIST_RESULT])


def main():

    setup()

    st.write("About")



if __name__ == '__main__':
    main()
