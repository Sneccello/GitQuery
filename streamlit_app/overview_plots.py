import pandas as pd
import streamlit as st
from session_utils import SessionHandler, get_spark_session, get_config, QueryNames
from spark_utils import read_all_records
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, desc
import plotly.express as px
import plotly.graph_objects as go

STATUSES = {
    "M": "Modified",
    "R": "Renamed",
    "A": "Added",
    "D": "Deleted"
}

def refresh_commits_per_author():
    TOP_AUTHORS = 10
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
    top_authors = df.groupBy('author').count().orderBy(desc('count')).limit(TOP_AUTHORS).toPandas()

    count = df.count()

    if (others := count - top_authors['count'].sum()) > 0:
        new_row = pd.DataFrame([{'author': 'Others', 'count': others}])
        top_authors = pd.concat([top_authors, new_row])

    SessionHandler.set_query_results(QueryNames.COMMITS_PER_AUTHOR, top_authors)

def display_commits_per_author():
    top_authors = SessionHandler.get_query_results(QueryNames.COMMITS_PER_AUTHOR)

    fig = px.pie(top_authors, names='author', values='count', title='Commits To All Repos Per Author')
    fig.update_traces(textinfo='none')
    st.plotly_chart(fig)

def refresh_commits_per_repo():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
    df = df.groupby('repo_id').count().toPandas()
    SessionHandler.set_query_results(QueryNames.COMMITS_PER_REPO, df)

def display_commits_per_repo():
    df = SessionHandler.get_query_results(QueryNames.COMMITS_PER_REPO)
    fig = px.bar(df, x='repo_id', y='count', title='Repository Commits')
    st.plotly_chart(fig)

def refresh_commit_activity():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("date", F.date_trunc("month", "date")) \
        .groupBy("repo_id", "date") \
        .count() \
        .orderBy("date") \

    df = df.select('date', 'count', 'repo_id').toPandas()

    SessionHandler.set_query_results(QueryNames.COMMIT_ACTIVITY, df)

def display_commit_activity():
    df = SessionHandler.get_query_results(QueryNames.COMMIT_ACTIVITY)
    fig = px.line(df, x='date', y='count', color='repo_id', title='Commits Over Time')
    st.plotly_chart(fig)

def refresh_file_changes_per_commit():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())

    df = (df.withColumn("Modified Files Per Commit", F.size(F.col("files")))
          .select("Modified Files Per Commit", "repo_id")
          .toPandas())

    SessionHandler.set_query_results(QueryNames.FILECHANGES_PER_COMMIT, df)

def display_file_changes_per_commit():
    df = SessionHandler.get_query_results(QueryNames.FILECHANGES_PER_COMMIT)
    fig = px.box(df, x='repo_id', y="Modified Files Per Commit", title='File Changes Per Commit')
    st.plotly_chart(fig)


def refresh_file_status_counts():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
    abs_counts = (df
          .withColumn('file', explode('files'))
          .withColumn('file_status', col('file')['status'])
          .groupby('repo_id')
          .pivot('file_status')
          .count()
          .toPandas())

    SessionHandler.set_query_results(QueryNames.FILE_STATUS_COUNTS, abs_counts)

def display_file_status_counts():

    abs_counts = SessionHandler.get_query_results(QueryNames.FILE_STATUS_COUNTS)
    abs_counts = abs_counts.set_index('repo_id').fillna(0)
    percentages = abs_counts.div(abs_counts.sum(axis=1), axis=0) * 100
    percentages = percentages[list(STATUSES.keys())]
    fig = go.Figure(
        data=go.Heatmap(
            z=percentages,
            y=percentages.index,
            x=percentages.columns.map(lambda c: STATUSES[c.upper()]),
            text=abs_counts[list(STATUSES.keys())],
            texttemplate="%{text}",
            textfont={"size": 20},
            colorbar={"title": 'Row-wise %'}
        ),
        layout={'title':"File Status Changes"},
    )

    st.plotly_chart(fig)


def refresh_plot_data():
    SessionHandler.unpersist_spark_basetable()

    refresh_functions = [
        refresh_commits_per_author,
        refresh_commits_per_repo,
        refresh_commit_activity,
        refresh_file_changes_per_commit,
        refresh_file_status_counts
    ]
    QUERYING_WITH_SPARK = "Querying with Spark.. (See Jobs at [http://localhost:4040](http://localhost:4040))"

    query_pbar = st.progress(0, text=QUERYING_WITH_SPARK)
    n_refresh = len(refresh_functions)
    for idx, fn in enumerate(refresh_functions):
        fn()
        percentage = (idx + 1) / n_refresh
        query_pbar.progress(percentage, text=QUERYING_WITH_SPARK)
    query_pbar.progress(100, text='Querying with Spark done!')