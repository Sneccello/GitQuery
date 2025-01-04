import plotly.express as px
import streamlit as st
from session_utils import get_config, get_spark_session, SessionMeta
from spark_utils import get_normalized_df
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, concat, col, lit

def display_commits_per_author():
    df = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.groupby('author').count()
    fig = px.pie(df, names='author', values='count', title='Commits Per Author')
    st.plotly_chart(fig)

def display_commits_per_repo():
    df = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.groupby('repo_id').count()
    fig = px.bar(df, x='repo_id', y='count', title='Repository Commits')
    st.plotly_chart(fig)

def display_commit_activity():
    df = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())

    df = df.groupBy("repo_id", "year_month").count().orderBy("year_month")
    fig = px.line(df, x='year_month', y='count', color='repo_id', title='Commits Over Time')
    st.plotly_chart(fig)

def display_filechanges_per_repo():
    df = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.withColumn("Modified Files Per Commit", F.size(F.col("files")))

    fig = px.box(df, x='repo_id', y="Modified Files Per Commit", title='File Changes Per Commit')
    st.plotly_chart(fig)


def display_top_contributors():
    df = get_normalized_df(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())

    df = df.groupby('repo_id', 'author') \
        .agg(F.count('*').alias('Commit Count')) \
        .orderBy(F.desc('Commit Count')) \
        .limit(5)

    df = df.withColumn("Contributor", concat(col("repo_id"), lit("/"), col("author")))
    fig = px.bar(df, x='Contributor', y='Commit Count', title='Top Contributors')
    st.plotly_chart(fig)
