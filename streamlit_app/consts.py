UER_SQL_TABLE_NAME = 'commits'

PARTITION_COLUMNS = ["repo_id", 'author_first_char']

DEFAULT_SQL_QUERY = \
"""
--select authors with the most files created across all repositories
SELECT author, count(*) as n_created_files
FROM (
    SELECT author, explode(files) as file FROM commits
    )
WHERE file.status = 'A'
GROUP BY author
ORDER BY n_created_files DESC
LIMIT 5;

----Select Merge commits
--SELECT * from commits
--WHERE size(parents) > 1
"""
