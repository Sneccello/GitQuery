import os
from typing import List

from pyspark.sql.functions import regexp_extract, to_timestamp, col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from pyspark.sql import functions as F, SparkSession


def get_union_df(spark_session, config, repositories: List[str]):
    dfs = []

    for repo_id in repositories:
        df = spark_session.read.json(f"{get_gitlogs_hdfs_folder(config)}/{repo_id}/*.json")
        df = df.withColumn("repo_id", lit(repo_id))
        dfs.append(df)

    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.union(other_df)

    df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss Z"))
    df = df.withColumn("year_month", F.date_format("date", "yyyy-MM"))

    return df



def process_partition(iterator):
    result = []

    for row in iterator:
        record = dict()

        assert len(row.lines) >= 5, f'invalid commit: {row.lines}'
        record['commitHash'] = row.commitHash
        record['parent'] = row.lines[1].lstrip('parents: ').strip()
        record['message'] = row.lines[2].lstrip('message: ').strip()
        record['author'] = row.lines[3].lstrip('author: ').strip()
        record['date'] = row.lines[4].lstrip('date: ').strip()
        record['files'] = [{'status': f.strip()[0], 'filename': f[1:].strip()} for f in row.lines[5:]]
        result.append(record)
    return result


def get_gitlogs_hdfs_folder(config):
    return f"hdfs://{config.HDFS_HOST}:{config.HDFS_RPC_PORT}{config.HDFS_GITLOGS_PATH}"

def load_gitlog_file(spark_session, config: "config.Config", repo_id: str):
    df = spark_session.read.text(
        f"{get_gitlogs_hdfs_folder(config)}/{repo_id}.gitlog"
    )
    return df.rdd.zipWithIndex().map(lambda x: (x[0][0], x[1])).toDF(["line", "lineIdx"])

def create_gitlog_rdd(spark_session, config, repo_id):

    gitlogs_df = load_gitlog_file(spark_session, config, repo_id)

    commit_hash_pattern = r"commit: ([a-f0-9]{40})"
    df_commit_indices = gitlogs_df.withColumn("commitHash", regexp_extract("line", commit_hash_pattern, 1))
    df_commit_indices = df_commit_indices[df_commit_indices.commitHash != ""]
    df_commit_indices = df_commit_indices.select("lineIdx", "commitHash")
    windowSpec = Window.orderBy("lineIdx")
    df_commit_indices = df_commit_indices.withColumn("nextCommitIdx", lag("lineIdx", -1, default=gitlogs_df.count()).over(windowSpec))

    lines = gitlogs_df.alias("lines")
    commits = df_commit_indices.alias("commits")
    join_condition = (F.col("commits.lineIdx") <= F.col("lines.lineIdx")) & (F.col("lines.lineIdx") < F.col("commits.nextCommitIdx"))
    joined_df = lines.join(commits, join_condition)
    joined_df = joined_df.select("lines.lineIdx", "commits.commitHash", "lines.line")
    joined_df = joined_df[joined_df.line != '']

    commits = (
        joined_df
        .orderBy("commitHash", "lineIdx")
        .groupBy("commitHash")
        .agg(F.collect_list("line").alias("lines"))
    )
    commits = commits.rdd.mapPartitions(process_partition).toDF()

    output_path = f"h{get_gitlogs_hdfs_folder(config)}/{repo_id}"
    commits.write.mode("overwrite").json(output_path)