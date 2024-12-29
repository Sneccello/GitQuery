from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import regexp_extract
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("Simple Spark Example") \
    .getOrCreate()

# Load data from a CSV file into a DataFrame
df = spark.read.text("hdfs://namenode:9000/user/root/gitlogs/Sneccello_WordMaze.gitlog")
df = df.rdd.zipWithIndex().map(lambda x: (x[0][0], x[1])).toDF(["line", "lineIdx"])


commit_hash_pattern = r"commit ([a-f0-9]{40})"

df_commit_indices = df.withColumn("commitHash", regexp_extract("line", commit_hash_pattern, 1))
df_commit_indices = df_commit_indices[df_commit_indices.commitHash != ""]
df_commit_indices = df_commit_indices.select("lineIdx", "commitHash")

windowSpec = Window.orderBy("lineIdx")
df_commit_indices = df_commit_indices.withColumn("nextCommitIdx", lag("lineIdx", -1, default=df.count()).over(windowSpec))


lines = df.alias("lines")
commits = df_commit_indices.alias("commits")

# Define the join condition
join_condition = (F.col("commits.lineIdx") <= F.col("lines.lineIdx")) & (F.col("lines.lineIdx") < F.col("commits.nextCommitIdx"))

# Perform the join
joined_df = lines.join(commits, join_condition)

# Show the result
joined_df.show()