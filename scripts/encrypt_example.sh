#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
export JIRA_NO_INCREMENTAL=1
# curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"Jira:https://jira.riscv.org"}}}' | jq -rS '.' || exit 1
../insights-datasource-github/encrypt "`cat ./secrets/user.secret`" > ./secrets/user.encrypted.secret || exit 2
../insights-datasource-github/encrypt "`cat ./secrets/token.secret`" > ./secrets/token.encrypted.secret || exit 3
./jira --jira-url='https://jira.riscv.org' --jira-debug=1 --jira-es-url="${ESURL}" --jira-user="`cat ./secrets/user.encrypted.secret`" --jira-token="`cat ./secrets/token.encrypted.secret`" --jira-stream="${STREAM}" $* 2>&1 | tee run.log
#./jira --jira-url='https://jira.acumos.org' --jira-debug=1 --jira-es-url="${ESURL}" --jira-user='' --jira-token='' --jira-stream="${STREAM}" $* 2>&1 | tee run2.log
