GO_BIN_FILES=cmd/jira/jira.go

#for race CGO_ENABLED=1
GO_ENV=GOOS=linux CGO_ENABLED=0
# GO_BUILD=go build -ldflags '-s -w' -race
GO_BUILD=go build -ldflags '-s -w'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*'
BINARIES=jira
COMMIT=`git rev-parse --short HEAD`
VERSION=`git describe --tags --always | cut -d- -f1`
LDFLAGS=-ldflags "-s -w -extldflags '-static' -X github.com/LF-Engineering/insights-connector-jira/build.GitCommit=$(COMMIT) \
  -X github.com/LF-Engineering/insights-connector-jira/build.Version=$(VERSION)"
all: check ${BINARIES}
jira: ${GO_BIN_FILES}
	 ${GO_ENV} ${GO_BUILD} -o jira ${LDFLAGS} ${GO_BIN_FILES}
fmt: ${GO_BIN_FILES}
	${GO_FMT} ${GO_BIN_FILES}
lint: ${GO_BIN_FILES}
	go get -u golang.org/x/lint/golint
	${GO_LINT} ${GO_BIN_FILES}
vet: ${GO_BIN_FILES}
	go mod vendor
	${GO_VET} ${GO_BIN_FILES}
imports: ${GO_BIN_FILES}
	go install golang.org/x/tools/cmd/goimports@latest
	${GO_IMPORTS} ${GO_BIN_FILES}
errcheck: ${GO_BIN_FILES}
	go install github.com/kisielk/errcheck@latest
	${GO_ERRCHECK} ${GO_BIN_FILES}
check: fmt lint imports vet errcheck
clean:
	rm -rf ${BINARIES}
.PHONY: all
