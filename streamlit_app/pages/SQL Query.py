import streamlit as st
from code_editor import code_editor
from pyspark.shell import spark

from pages.OverView import display_filter
from session_utils import get_spark_session, get_config, SessionMetaKeys
from spark_utils import get_normalized_df

def main():
    TABLE_NAME = 'commits'
    DEFAULT_QUERY = """
           -- List authors with most files created
           SELECT author, count(*) as n_created_files
           FROM (
               SELECT author, explode(files) as files FROM commits
           )
           WHERE files.status = 'A'
           group by author
           ORDER BY n_created_files DESC
           LIMIT 5 
           """

    st.title('SQL Editor')
    st.write('_Run SQL commands directly on the Spark Dataframe_')
    display_filter()
    commits = get_normalized_df(get_spark_session(), get_config(),
                                st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES])

    commits.createOrReplaceTempView(TABLE_NAME)
    st.write("## Normalized Dataframe")
    st.write(commits.limit(5))

    st.write('## Your SQL Query')
    response = code_editor(
        DEFAULT_QUERY,
        lang="sql",
        height=len(DEFAULT_QUERY.split('\n')),
        ghost_text='asda',
        completions=[TABLE_NAME]
    )
    run = st.button('Run Query (CTRL + Enter)')
    st.write('## Query Result')
    if run or response['type'] == 'submit' or response['type'] == '':  # first run
        query = response['text'] or DEFAULT_QUERY
        commits.createOrReplaceTempView("commits")
        query_df = spark.sql(query)
        st.write(query_df)


if __name__ == '__main__':
    main()