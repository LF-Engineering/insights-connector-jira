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
all: check ${BINARIES}
jira: ${GO_BIN_FILES}
	 ${GO_ENV} ${GO_BUILD} -o jira ${GO_BIN_FILES}
fmt: ${GO_BIN_FILES}
	${GO_FMT} ${GO_BIN_FILES}
lint: ${GO_BIN_FILES}
	${GO_LINT} ${GO_BIN_FILES}
vet: ${GO_BIN_FILES}
	${GO_VET} ${GO_BIN_FILES}
imports: ${GO_BIN_FILES}
	${GO_IMPORTS} ${GO_BIN_FILES}
errcheck: ${GO_BIN_FILES}
	${GO_ERRCHECK} ${GO_BIN_FILES}
check: fmt lint imports vet errcheck
setup_swagger:
	go install github.com/go-swagger/go-swagger/cmd/swagger
swagger: clean_swagger
	swagger -q generate model -t gen -f swagger/jira.yaml
clean_swagger:
	rm -rf ./gen
	mkdir gen
clean:
	rm -rf ${BINARIES}
.PHONY: all
