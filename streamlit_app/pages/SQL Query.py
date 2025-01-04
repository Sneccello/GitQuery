import json

import streamlit as st
from code_editor import code_editor
from pyspark.errors import ParseException
from pyspark.shell import spark

from pages.Overview import display_filter
from session_utils import get_spark_session, get_config, SessionMeta
from spark_utils import read_all_records, COLUMNS

TABLE_NAME = 'commits'
DEFAULT_QUERY = \
        """
       SELECT author, count(*) as n_created_files
       FROM (
           SELECT author, explode(files) as files FROM commits
       )
       WHERE files.status = 'A'
       GROUP BY author
       ORDER BY n_created_files DESC
       LIMIT 5;
       
       -- Select Merge commits
       --SELECT * from commits
       --where size(parents) > 1
       """

def display_sidebar(dtype_hints):
    with st.sidebar:
        st.markdown("<h2 style='color: lightblue;'>Hints ℹ️</h2>", unsafe_allow_html=True)
        st.markdown(f"<p style='color: lightblue;'>SQL Table name: <em>{TABLE_NAME}</em></p>", unsafe_allow_html=True)
        with st.expander('Column Datatypes', expanded=True):
            st.write(dtype_hints)

def display_editor_space():

    if not SessionMeta.get_selected_repositories():
        st.write("##### No data found :( Add and/or Select repositories to visualize!")

    commits = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    display_sidebar(commits.dtypes)

    commits.createOrReplaceTempView(TABLE_NAME)
    st.markdown("<h2 style='color: #89CFF0;'>🌐 DataFrame</h2>", unsafe_allow_html=True)  # Light blue
    st.write(commits.limit(5))


    st.markdown("<h2 style='color: #B39DDB;'>📝 Your SQL Query</h2>", unsafe_allow_html=True)  # Soft lavender
    response = code_editor(
        DEFAULT_QUERY,
        lang="sql",
        height=len(DEFAULT_QUERY.split('\n')),
        completions=[TABLE_NAME, *COLUMNS.get_values()]
    )

    st.markdown("<h5 style='color: #B39DDB;'>Press CTRL+ENTER to RUN</h3>", unsafe_allow_html=True)

    st.markdown("<h2 style='color: #FFB74D;'>📊 Query Result</h2>", unsafe_allow_html=True)  # Muted orange
    if response['type'] == 'submit' or response['type'] == '':  # first run
        query = response['text'] or DEFAULT_QUERY

        with st.expander('Explain Query Plan', expanded=False):
            st.write("Plan:")
            st.write(f"_{response['text']}_")
            try:
                plan = spark.sql("EXPLAIN FORMATTED " + query).collect()[0].plan
                plan_str = str(plan)

                highlighted_plan = plan_str.replace("PartitionFilters",
                                                    "<span style='color:red'>PartitionFilters</span>")
                highlighted_plan = highlighted_plan.replace("ShuffleExchange",
                                                            "<span style='color:blue'>ShuffleExchange</span>")
                highlighted_plan = highlighted_plan.replace("BroadcastHashJoin",
                                                            "<span style='color:green'>BroadcastHashJoin</span>")
                highlighted_plan = highlighted_plan.replace("Repartition",
                                                            "<span style='color:orange'>Repartition</span>")
                highlighted_plan = highlighted_plan.replace("Scan parquet",
                                                            "<span style='color:#733b73'>Scan parquet</span>")
                highlighted_plan = highlighted_plan.replace("Filter",
                                                            "<span style='color:coral'>Filter</span>")
                highlighted_plan = highlighted_plan.replace("PartitionPruning",
                                                            "<span style='color:darkcyan'>PartitionPruning</span>")
                highlighted_plan = highlighted_plan.replace("LocalShuffleReader",
                                                            "<span style='color:teal'>LocalShuffleReader</span>")

                # Render the highlighted plan
                st.markdown(highlighted_plan, unsafe_allow_html=True)
            except ParseException as e:
                st.write("Failed to analyze plan")
                st.write(e)
                st.write("Query: \n" + query)

        commits.createOrReplaceTempView(TABLE_NAME)
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