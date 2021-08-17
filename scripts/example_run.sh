#!/bin/bash
clear; JIRA_TAGS="c,d,e" ./scripts/jira.sh --jira-date-from "2021-01" --jira-date-to "2021-08" --jira-pack-size=100 --jira-tags="a,b,c" --jira-project=ONAP --jira-debug=2
