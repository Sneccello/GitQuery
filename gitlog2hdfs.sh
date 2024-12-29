#!/bin/bash

GIT_URL=https://github.com/Sneccello/WordMaze.git #$1

REPO_OWNER=$(echo "$GIT_URL" | sed -E 's/.*\/([^\/]+)\/([^\/]+)\.git/\1/')
REPO_NAME=$(echo "$GIT_URL" | sed -E 's/.*\/([^\/]+)\/([^\/]+)\.git/\2/')
REPO_ID="${REPO_OWNER}_${REPO_NAME}"


OUTPUT_FILE="${REPO_ID}.gitlog"
OUTPUT_PATH="/tmp/$OUTPUT_FILE"
docker exec datanode rm -rf /tmp/$REPO_ID
docker exec datanode git clone "$GIT_URL" /tmp/$REPO_ID

docker exec -it datanode bash -c "
cd /tmp/${REPO_ID} &&
git log --name-status --pretty=format:'commit %H%nparent %P%nmessage %s' > $OUTPUT_PATH"

docker exec datanode hdfs dfs -mkdir -p gitlogs
docker exec datanode hdfs dfs -put -f $OUTPUT_PATH "gitlogs/${OUTPUT_FILE}"

docker exec datanode rm -rf /tmp/$REPO_ID
docker exec datanode rm $OUTPUT_PATH
