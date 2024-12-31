import plotly.express as px
import streamlit as st
from session_utils import get_config, SessionMetaKeys, get_spark_session
from spark_utils import get_union_df
from pyspark.sql import functions as F

def display_commits_per_author():
    df = get_union_df(get_spark_session(), get_config(), st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES])
    df = df.groupby('author').count()
    fig = px.pie(df, names='author', values='count', title='Commits Per Author')
    st.plotly_chart(fig)

def display_commits_per_repo():
    df = get_union_df(get_spark_session(), get_config(), st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES])
    df = df.groupby('repo_id').count()
    fig = px.bar(df, x='repo_id', y='count', title='Repository Commits')
    st.plotly_chart(fig)

def display_commit_activity():
    df = get_union_df(get_spark_session(), get_config(), st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES])

    df = df.groupBy("repo_id", "year_month").count().orderBy("year_month")
    fig = px.line(df, x='year_month', y='count', color='repo_id', title='Commits Over Time')
    st.plotly_chart(fig)

def display_filechanges_per_repo():
    df = get_union_df(get_spark_session(), get_config(), st.session_state[SessionMetaKeys.SELECTED_REPOSITORIES])
    df = df.withColumn("Modified Files", F.size(F.col("files")))

    fig = px.box(df, x='repo_id', y="Modified Files")

    st.plotly_chart(fig)