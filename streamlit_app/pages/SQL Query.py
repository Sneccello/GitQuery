import json

import streamlit as st
from code_editor import code_editor
from pyspark.errors import ParseException
from pyspark.shell import spark

from consts import UER_SQL_TABLE_NAME, DEFAULT_SQL_QUERY
from session_utils import get_spark_session, get_config, SessionHandler
from spark_utils import read_all_records, COLUMNS

def display_sidebar(dtype_hints):
    with st.sidebar:
        st.markdown("<h2 style='color: lightblue;'>SQL Hints ℹ️</h2>", unsafe_allow_html=True)
        st.markdown(f"<p style='color: lightblue;'>SQL Table name: <em>{UER_SQL_TABLE_NAME}</em></p>", unsafe_allow_html=True)
        with st.expander(f'{UER_SQL_TABLE_NAME} Table Schema', expanded=True):
            st.write(dtype_hints)

def display_explain_query():
    query = SessionHandler.get_user_sql_query()
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
    with st.spinner('Running query...(See Jobs at [http://localhost:4040](http://localhost:4040)'):
        SessionHandler.set_user_sql_query(query)
        result_df = spark.sql(query)
        SessionHandler.set_user_sql_result(result_df.toPandas())
        st.write(result_df)
        display_explain_query()

def refresh_spark_table():
    with st.spinner('Reading HDFS into Spark Dataframe...'):
        commits = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
        commits.createOrReplaceTempView(UER_SQL_TABLE_NAME)
        SessionHandler.set_spark_table_dtypes(commits.dtypes)
        SessionHandler.set_spark_table_sample(commits.limit(5).toPandas())

    st.rerun()
def display_editor_space():

    if not SessionHandler.get_selected_repositories():
        st.write("##### No data found :( Add and/or Select repositories to visualize!")
        return

    st.markdown(f"<h2 style='color: #89CFF0;'>🌐 \"{UER_SQL_TABLE_NAME}\" Spark Dataframe</h2>", unsafe_allow_html=True)
    refresh_spark = st.button("Refresh view")

    if SessionHandler.get_spark_table_sample() is None or refresh_spark:
        refresh_spark_table()

    display_sidebar(SessionHandler.get_spark_table_dtypes())
    st.write(SessionHandler.get_spark_table_sample())

    st.markdown("<h2 style='color: #B39DDB;'>📝 Your SQL Query</h2>", unsafe_allow_html=True)
    response = code_editor(
        DEFAULT_SQL_QUERY,
        lang="sql",
        height=max(10, len(SessionHandler.get_user_sql_query().split('\n'))),
        completions=[UER_SQL_TABLE_NAME, *COLUMNS.get_values()],
        buttons=[{
            "name": "Run Query",
            "feather": "PlayCircle",
            'hasText': True,
            "alwaysOn": True,
            "commands": ["submit"],
            "style": {"top": "0.46rem", "right": "0.4rem"},
            "bindKey": { 'win': 'Ctrl-Enter', 'mac': 'Command-Enter' }
        }]
    )
    st.markdown("<h2 style='color: #FFB74D;'>📊 Query Result</h2>", unsafe_allow_html=True)

    if response['type'] == 'submit':
        try:
            SessionHandler.set_user_sql_query(response['text'])
            display_query_results(SessionHandler.get_user_sql_query())
        except Exception as e:
            st.write('oyyoy')
            st.error(e)




def main():

    SessionHandler.setup()
    st.title('SQL Editor')
    st.write('_Run SQL commands directly on the Spark Dataframe_')
    display_editor_space()



if __name__ == '__main__':
    main()