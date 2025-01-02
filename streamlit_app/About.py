import streamlit as st
from session_utils import SessionMeta


def setup():

    st.set_page_config(
        page_title="GitInsights",
        page_icon="🧊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    SessionMeta.setup()

def main():

    setup()

    st.write("About")



if __name__ == '__main__':
    main()
