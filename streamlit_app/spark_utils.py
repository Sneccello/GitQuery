import datetime
import enum
from typing import List

from pyspark.sql.functions import regexp_extract, to_timestamp, col, lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F


class COLUMNS(enum.Enum):
    AUTHOR = 'author'
    COMMIT_HASH = 'commit_hash'
    DATE = 'date'
    TIMESTAMP = 'timestamp'
    FILES = 'files'
    MESSAGE = 'message'
    PARENTS = 'parents'
    REPO_ID = 'repo_id'


    @staticmethod
    def get_values():
        return [c.value for c in COLUMNS]

def read_all_records(spark_session, config, repositories: List[str]):

    df = spark_session.read.parquet(get_output_root_folder(config))

    df = df.filter(df.repo_id.isin(repositories))

    df = df.withColumn("date",
                       to_timestamp(F.concat_ws(" ", df["date"], df["timestamp"]), "yyyy-MM-dd HH:mm:ssZ")
                       )

    df.drop('timestamp')

    return df

def process_partition(iterator):
    result = []

    for row in iterator:
        record = dict()

        assert len(row.lines) >= 5, f'invalid commit: {row.lines}'
        record['commit_hash'] = row.commit_hash
        record['parents'] = row.lines[1].lstrip('parents: ').strip().split()
        record['message'] = row.lines[2].lstrip('message: ').strip()
        record['author'] = row.lines[3].lstrip('author: ').strip()

        iso_date = row.lines[4].lstrip('date: ').strip()
        dt = datetime.datetime.fromisoformat(iso_date)
        day = dt.strftime('%Y-%m-%d')
        timestamp = dt.strftime('%H:%M:%S%z')
        record['date'] = day
        record['timestamp'] = timestamp

        record['files'] = [{'status': f.strip()[0], 'filename': f[1:].strip()} for f in row.lines[5:]]
        result.append(record)
    return result


def get_gitlogs_hdfs_folder(config):
    return f"hdfs://{config.HDFS_HOST}:{config.HDFS_RPC_PORT}{config.HDFS_GITLOGS_PATH}"

def get_output_root_folder(config):
    return f"hdfs://{config.HDFS_HOST}:{config.HDFS_RPC_PORT}{config.HDFS_SPARK_OUTPUT_ROOT}/"

def create_gitlog_rdd(spark_session, config, repo_id, partition_by: str):

    gitlog = spark_session.read.text(
        f"{get_gitlogs_hdfs_folder(config)}/{repo_id}.gitlog"
    )

    gitlog = (gitlog.rdd.zipWithIndex()
        .map(lambda values_key: (values_key[1], values_key[0][0]))
        .toDF(["gitlog_line_idx", "gitlog_line"])
              )

    commit_hash_pattern = r"commit: ([a-f0-9]{40})"

    commits = gitlog.withColumn("commit_hash", regexp_extract("gitlog_line", commit_hash_pattern, 1))
    commits = commits[commits.commit_hash != ""]
    commits = commits.select("gitlog_line_idx", "commit_hash")
    commits = commits.orderBy("gitlog_line_idx")

    commits = commits.rdd.zipWithIndex().map(
        lambda values_key: (values_key[1], values_key[0][0], values_key[0][1])).toDF(
        ["commit_idx", "gitlog_line_idx", 'commit_hash'])

    commits = commits.alias('commits1').join(
        commits.alias('commits2'),
        F.col("commits1.commit_idx") == F.col("commits2.commit_idx") - 1,
        how="left"
    ).select(
        col("commits1.gitlog_line_idx").alias('commit_line_idx'),
        "commits1.commit_hash",
        col("commits2.gitlog_line_idx").alias("next_commit_line_idx")
    )

    join_condition = (F.col("commits.commit_line_idx") <= F.col("gitlog.gitlog_line_idx")) & (
                F.col("gitlog.gitlog_line_idx") < F.col("commits.next_commit_line_idx"))

    commits_with_gitlog_lines = gitlog.alias('gitlog').join(commits.alias('commits'), join_condition).select(
        "commit_hash",
        "gitlog_line",
        "gitlog_line_idx"
    )

    commits_with_gitlog_lines = commits_with_gitlog_lines.repartition('commit_hash')

    commits_with_gitlog_lines = commits_with_gitlog_lines[commits_with_gitlog_lines.gitlog_line != '']

    windowSpec = Window.partitionBy("commit_hash").orderBy("gitlog_line_idx")

    commits_with_gitlog_lines_ordered = commits_with_gitlog_lines.withColumn(
        'lines', F.collect_list('gitlog_line').over(windowSpec)
    ).groupBy('commit_hash') \
        .agg(F.max('lines').alias('lines'))

    commits_with_gitlog_lines_ordered = commits_with_gitlog_lines_ordered.rdd.mapPartitions(process_partition).toDF()

    commits_with_gitlog_lines_ordered = commits_with_gitlog_lines_ordered.withColumn("repo_id", lit(repo_id))
    commits_with_gitlog_lines_ordered = commits_with_gitlog_lines_ordered.select(
        *COLUMNS.get_values()
    )

    (commits_with_gitlog_lines_ordered
     .write
     .partitionBy(COLUMNS.REPO_ID.value, partition_by)
     .mode("overwrite")
     .parquet(get_output_root_folder(config)))