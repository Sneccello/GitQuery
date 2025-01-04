import re
import streamlit as st
from session_utils import SessionMeta

def display_markdown_with_images(markdown_string):
    parts = re.split(r"!\[(.*?)\]\((.*?)\)", markdown_string)
    for i, part in enumerate(parts):
        if i % 3 == 0:
            st.markdown(part)
        elif i % 3 == 1:
            title = part
        else:
            st.image(part)

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
    display_markdown_with_images(content)

if __name__ == '__main__':
    main()
