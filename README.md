# insights-connector-jira
Jira data source V2

# Environment Variables

- `AWS_REGION`,`AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` : For AWS config to initialize firehose stream

- run `make swagger` to generate models.
- run `make` to build app.
- run `./scripts/example_run.sh` to try it.
- example [JSON](https://github.com/LF-Engineering/insights-connector-jira/blob/main/exampleOutput.json) generated by this tool:
  