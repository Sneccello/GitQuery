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
    TOP_AUTHORS = 20
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


def refresh_commit_activity():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())
    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("date", F.date_trunc("month", "date")) \
        .groupBy("repo_id", "date") \
        .count() \
        .orderBy("date") \

    df = df.select('date', 'count', 'repo_id').toPandas()
    df['count'] = df.sort_values('date').groupby('repo_id')['count'].cumsum()

    SessionHandler.set_query_results(QueryNames.COMMIT_ACTIVITY, df)

def display_commit_activity():
    df = SessionHandler.get_query_results(QueryNames.COMMIT_ACTIVITY)
    fig = px.line(df, x='date', y='count', color='repo_id', title='Commits Over Time')
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
        refresh_commit_activity,
        refresh_file_status_counts,
        refresh_active_authors
    ]
    QUERYING_WITH_SPARK = "Creating Plots... (See Spark jobs at [http://localhost:4040](http://localhost:4040))"

    query_pbar = st.progress(0, text=QUERYING_WITH_SPARK)
    n_refresh = len(refresh_functions)
    for idx, fn in enumerate(refresh_functions):
        fn()
        percentage = (idx + 1) / n_refresh
        query_pbar.progress(percentage, text=QUERYING_WITH_SPARK)
    query_pbar.progress(100, text='Creating Plots... DONE!')


def refresh_active_authors():
    df = read_all_records(get_spark_session(), get_config(), SessionHandler.get_selected_repositories())

    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("month",
                       F.date_trunc("month", "date")
                       )

    distinct_authors_per_month = (
        df
        .groupBy("repo_id","month")
        .agg(F.countDistinct("author").alias("active authors"))
        .orderBy("month")
    ).toPandas()

    SessionHandler.set_query_results(QueryNames.ACTIVE_AUTHORS, distinct_authors_per_month)


def display_active_authors():
    active_authors = SessionHandler.get_query_results(QueryNames.ACTIVE_AUTHORS)

    fig = px.line(active_authors, x='month', y='active authors', color='repo_id', title='Active Authors Over Time')

    st.plotly_chart(fig)

