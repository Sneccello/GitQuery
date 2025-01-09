import json

import streamlit as st
from code_editor import code_editor
from pyspark.errors import ParseException
from pyspark.shell import spark

from consts import UER_SQL_TABLE_NAME
from session_utils import get_spark_session, get_config, SessionMeta
from spark_utils import read_all_records, COLUMNS

def display_sidebar(dtype_hints):
    with st.sidebar:
        st.markdown("<h2 style='color: lightblue;'>SQL Hints ℹ️</h2>", unsafe_allow_html=True)
        st.markdown(f"<p style='color: lightblue;'>SQL Table name: <em>{UER_SQL_TABLE_NAME}</em></p>", unsafe_allow_html=True)
        with st.expander(f'{UER_SQL_TABLE_NAME} Table Schema', expanded=True):
            st.write(dtype_hints)

def display_explain_query():
    query = SessionMeta.get_user_sql_query()
    with st.expander('Explain Query Plan', expanded=False):
        st.write("Plan:")
        try:
            plan = spark.sql("EXPLAIN FORMATTED " + query).collect()[0].plan
            plan_str = str(plan)

            highlighted_plan = plan_str.replace("PartitionFilters",
                                                "<span style='color:red'>PartitionFilters</span>")
            highlighted_plan = highlighted_plan.replace("Exchange",
                                                        "<span style='color:#f54290'>ShuffleExchange</span>")
            highlighted_plan = highlighted_plan.replace("Scan parquet",
                                                        "<span style='color:#733b73'>Scan parquet</span>")

            st.markdown(highlighted_plan, unsafe_allow_html=True)
        except ParseException as e:
            st.write("Failed to analyze plan")
            st.write(e)
            st.write("Query: \n" + query)

def display_query_results(query):
    with st.spinner('Running query...'):
        SessionMeta.set_user_sql_query(query)
        result_df = spark.sql(query)
        SessionMeta.set_user_sql_result(result_df.toPandas())
        display_explain_query()
        st.write(result_df)

def display_editor_space():

    if not SessionMeta.get_selected_repositories():
        st.write("##### No data found :( Add and/or Select repositories to visualize!")
        return

    with st.spinner('Reading HDFS into Spark dataframe...'):
        commits = read_all_records(get_spark_session(), get_config(), SessionMeta.get_last_hdfs_repo_list_result())
        display_sidebar(commits.dtypes)
        commits.createOrReplaceTempView(UER_SQL_TABLE_NAME)
        st.markdown("<h2 style='color: #89CFF0;'>🌐 DataFrame</h2>", unsafe_allow_html=True)  # Light blue
        st.write(commits.limit(5))

    st.markdown("<h2 style='color: #B39DDB;'>📝 Your SQL Query</h2>", unsafe_allow_html=True)  # Soft lavender
    response = code_editor(
        SessionMeta.get_user_sql_query(),
        lang="sql",
        height=len(SessionMeta.get_user_sql_query().split('\n')),
        completions=[UER_SQL_TABLE_NAME, *COLUMNS.get_values()]
    )
    st.markdown("<h5 style='color: #B39DDB;'>Press CTRL+ENTER to RUN</h3>", unsafe_allow_html=True)
    st.markdown("<h2 style='color: #FFB74D;'>📊 Query Result</h2>", unsafe_allow_html=True)

    if SessionMeta.get_user_sql_result() is None:
        display_query_results(SessionMeta.get_user_sql_query())

    elif response['type'] == 'submit':
        user_query = response['text']
        display_query_results(user_query)






def main():

    SessionMeta.setup()
    st.title('SQL Editor')
    st.write('_Run SQL commands directly on the Spark Dataframe_')
    display_editor_space()



if __name__ == '__main__':
    main()