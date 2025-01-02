import streamlit as st
from session_utils import SessionMeta


@st.cache_data
def read_readme_file():
    with open("README.md", "r") as f:
        return f.read()

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

    content = read_readme_file()
    st.write(content)

if __name__ == '__main__':
    main()
