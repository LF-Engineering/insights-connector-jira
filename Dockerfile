FROM alpine:3.14

WORKDIR /app
ENV JIRA_URL='<JIRA-URL>'
ENV JIRA_USER='<JIRA-USER>'
ENV JIRA_TOKEN='<JIRA-TOKEN>'
ENV ES_URL='<ES-URL>'
ENV STAGE='<STAGE>'
RUN apk update
RUN apk add --no-cache bash
RUN ls -ltra
COPY jira ./
CMD ./jira --jira-url=${JIRA_URL} --jira-es-url="${ES_URL}" --jira-user="${JIRA_USER}" --jira-token="${JIRA_TOKEN}}" $*
