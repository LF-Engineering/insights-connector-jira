package main

import (
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/dev-analytics-libraries/emoji"
	"github.com/LF-Engineering/insights-datasource-jira/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/go-openapi/strfmt"
	jsoniter "github.com/json-iterator/go"
	// jsoniter "github.com/json-iterator/go"
)

type DSJira struct {
	URL string // Jira URL, for example https://jira.onap.org
}

// AddFlags - add Jira specific flags
func (j *DSJira) AddFlags() {
	// FIXME
	/*
		j.FlagURL = flag.String("rocketchat-url", "", "RocketChat server URL, for example https://chat.hyperledger.org")
		j.FlagChannel = flag.String("rocketchat-channel", "", "RocketChat channel, for example sawtooth")
		j.FlagUser = flag.String("rocketchat-user", "", "User: API user ID")
		j.FlagToken = flag.String("rocketchat-token", "", "Token: API token")
		j.FlagMaxItems = flag.Int("rocketchat-max-items", RocketchatDefaultMaxItems, "max items to retrieve from API via a single request - defaults to 100")
		j.FlagMinRate = flag.Int("rocketchat-min-rate", RocketchatDefaultMinRate, "min API points, if we reach this value we wait for refresh, default 10")
		j.FlagWaitRate = flag.Bool("rocketchat-wait-rate", false, "will wait for rate limit refresh if set, otherwise will fail is rate limit is reached")
	*/
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs(ctx *shared.Ctx) (err error) {
	// FIXME
	/*
		// Jira Server URL
		if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
			j.URL = *j.FlagURL
		}
		if ctx.EnvSet("URL") {
			j.URL = ctx.Env("URL")
		}
	*/
	// NOTE: don't forget this
	gJiraMetaData.Project = ctx.Project
	gJiraMetaData.Tags = ctx.Tags
	return
}

// Init - initialize Jira data source
func (j *DSJira) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("Jira")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate()
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		m := &models.Data{}
		shared.Printf("Jira: %+v\nshared context: %s\nModel: %+v", j, ctx.Info(), m)
	}
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSJira) EnrichItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	// FIXME
	/*
		defer func() {
			gM.Lock()
			defer gM.Unlock()
			gRa = append(gRa, item)
			gRi = append(gRi, rich)
		}()
		jsonBytes, _ := jsoniter.Marshal(item)
		shared.Printf("%s\n", string(jsonBytes))
	*/
	rich = make(map[string]interface{})
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// GetModelData - return data in swagger format
func (j *DSJira) GetModelData(ctx *shared.Ctx, docs []interface{}) (data *models.Data) {
	data = &models.Data{
		DataSource: JiraDataSource,
		MetaData:   gJiraMetaData,
		Endpoint:   j.URL,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		// Event
		// FIXME
		var updatedOn time.Time
		event := &models.Event{}
		data.Events = append(data.Events, event)
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
	}
	return
}

// JiraEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSJira) JiraEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
  // FIXME
	shared.Printf("input processing(%d/%d/%v)\n", len(items), len(*docs), final)
	outputDocs := func() {
		if len(*docs) > 0 {
			// actual output
			shared.Printf("output processing(%d/%d/%v)\n", len(items), len(*docs), final)
			data := j.GetModelData(ctx, *docs)
			// FIXME: actual output to some consumer...
			jsonBytes, err := jsoniter.Marshal(data)
			if err != nil {
				shared.Printf("Error: %+v\n", err)
				return
			}
			shared.Printf("%s\n", string(jsonBytes))
			*docs = []interface{}{}
			gMaxUpstreamDtMtx.Lock()
			defer gMaxUpstreamDtMtx.Unlock()
			shared.SetLastUpdate(ctx, j.Endpoint(), gMaxUpstreamDt)
		}
	}
	if final {
		defer func() {
			outputDocs()
		}()
	}
	// NOTE: non-generic code starts
	if ctx.Debug > 0 {
		shared.Printf("jira enrich items %d/%d func\n", len(items), len(*docs))
	}
}

// Sync - sync Jira data source
func (j *DSJira) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.Endpoint(), ctx.DateFrom, thrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.Endpoint())
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.Endpoint(), ctx.DateFrom, thrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.Endpoint(), ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
  // FIXME
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.Endpoint(), gMaxUpstreamDt)
	return
}

func main() {
	var (
		ctx  shared.Ctx
		jira DSJira
	)
	err := jira.Init(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
	err = jira.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
}
