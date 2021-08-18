#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./jira --jira-url='https://jira.lfnetworking.org' --jira-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --jira-user="`cat ./secrets/user.secret`" --jira-token="`cat ./secrets/token.secret`" $*
