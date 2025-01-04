import streamlit as st
from code_editor import code_editor
from pyspark.errors import ParseException
from pyspark.shell import spark

from pages.Overview import display_filter
from session_utils import get_spark_session, get_config, SessionMeta
from spark_utils import get_normalized_df, COLUMNS

TABLE_NAME = 'commits'
DEFAULT_QUERY = \
        """
       SELECT author, count(*) as n_created_files
       FROM (
           SELECT author, explode(files) as files FROM commits
       )
       WHERE files.status = 'A'
       group by author
       ORDER BY n_created_files DESC
       LIMIT 5;
       
       -- Select Merge commits
       --SELECT * from commits
       --where size(parents) > 1
       """

def display_editor_space():

    if not SessionMeta.get_selected_repositories():
        st.write("##### No data found :( Add and/or Select repositories to visualize!")

    commits = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())

    commits.createOrReplaceTempView(TABLE_NAME)
    st.write("## Normalized Dataframe")
    st.write(commits.limit(5))
    with st.expander('Column Datatypes Hints', expanded=False, ):
        st.write(commits.dtypes)

    st.write('## Your SQL Query')
    response = code_editor(
        DEFAULT_QUERY,
        lang="sql",
        height=len(DEFAULT_QUERY.split('\n')),
        ghost_text='asda',
        completions=[TABLE_NAME, *COLUMNS.get_values()]
    )

    st.write("_Press CTRL+ENTER to RUN_")

    st.write('## Query Result')
    if response['type'] == 'submit' or response['type'] == '':  # first run
        query = response['text'] or DEFAULT_QUERY

        with st.expander('Explain Query Plan', expanded=False):
            st.write("Plan:")
            st.write(f"_{response['text']}_")
            try:
                st.write(spark.sql("EXPLAIN FORMATTED " + query).collect()[0].plan)
            except ParseException as e:
                st.write("Failed to analyze plan")
                st.write(e)
                st.write("Query: \n" + query)

        commits.createOrReplaceTempView("commits")
        query_df = spark.sql(query)
        st.write(query_df)

def main():

    SessionMeta.setup()
    st.title('SQL Editor')
    st.write('_Run SQL commands directly on the Spark Dataframe_')
    display_filter()
    display_editor_space()



if __name__ == '__main__':
    main()