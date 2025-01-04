import streamlit as st
from session_utils import SessionMeta, get_spark_session, get_config
from spark_utils import read_all_records
from pyspark.sql import functions as F
from pyspark.sql.functions import col,explode
import plotly.express as px
import plotly.graph_objects as go


def display_commits_per_author():
    df = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.groupby('author').count()
    fig = px.pie(df, names='author', values='count', title='Commits Per Author')
    st.plotly_chart(fig)

def display_commits_per_repo():
    df = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.groupby('repo_id').count()
    fig = px.bar(df, x='repo_id', y='count', title='Repository Commits')
    st.plotly_chart(fig)

def display_commit_activity():
    df = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())

    df = df.groupBy("repo_id", "year_month").count().orderBy("year_month")
    fig = px.line(df, x='year_month', y='count', color='repo_id', title='Commits Over Time')
    st.plotly_chart(fig)

def display_filechanges_per_repo():
    df = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())
    df = df.withColumn("Modified Files Per Commit", F.size(F.col("files")))

    fig = px.box(df, x='repo_id', y="Modified Files Per Commit", title='File Changes Per Commit')
    st.plotly_chart(fig)



def file_status_heatmap():
    df = read_all_records(get_spark_session(), get_config(), SessionMeta.get_selected_repositories())

    STATUSES = {
        "M": "Modified",
        "R": "Renamed",
        "A": "Added",
        "D": "Deleted"
    }

    abs_counts = (df
          .withColumn('file', explode('files'))
          .withColumn('file_status', col('file')['status'])
          .groupby('repo_id')
          .pivot('file_status')
          .count()
          .toPandas())

    abs_counts.set_index('repo_id', inplace=True)
    percentages = abs_counts.div(abs_counts.sum(axis=1), axis=0) * 100

    fig = go.Figure(data=go.Heatmap(
        z=percentages,
        y=percentages.index,
        x=percentages.columns.map(lambda c: STATUSES[c]),
        text=abs_counts,
        texttemplate="%{text}",
        textfont={"size": 20}),
        layout={'title':"File Status Changes"}
    )

    st.plotly_chart(fig)