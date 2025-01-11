UER_SQL_TABLE_NAME = 'commits'

DEFAULT_SQL_QUERY = \
    """
   SELECT author, count(*) as n_created_files
   FROM (
       SELECT author, explode(files) as files FROM commits
   )
   WHERE files.status = 'A'
   GROUP BY author
   ORDER BY n_created_files DESC
   LIMIT 5;

   ----Select Merge commits
   --SELECT * from commits
   --WHERE size(parents) > 1
   """
