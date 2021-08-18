package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/insights-datasource-jira/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	jsoniter "github.com/json-iterator/go"
	// jsoniter "github.com/json-iterator/go"
)

const (
	// JiraAPIRoot - main API path
	JiraAPIRoot = "/rest/api/2"
	// JiraAPISearch - search API subpath
	JiraAPISearch = "/search"
	// JiraAPIField - field API subpath
	JiraAPIField = "/field"
	// JiraAPIIssue - issue API subpath
	JiraAPIIssue = "/issue"
	// JiraAPIComment - comments API subpath
	JiraAPIComment = "/comment"
	// JiraBackendVersion - backend version
	JiraBackendVersion = "0.1.1"
	// JiraDefaultSearchField - default search field
	JiraDefaultSearchField = "item_id"
	// JiraFilterByProjectInComments - filter by project when searching for comments
	JiraFilterByProjectInComments = false
	// JiraDropCustomFields - drop custom fields from raw index
	JiraDropCustomFields = false
	// JiraMapCustomFields - run custom fields mapping
	JiraMapCustomFields = true
	// ClosedStatusCategoryKey - issue closed status key
	ClosedStatusCategoryKey = "done"
	// JiraRichAuthorField - rich index author field
	JiraRichAuthorField = "reporter"
	// JiraDefaultPageSize - API page size
	JiraDefaultPageSize = 500
)

var (
	// JiraSearchFields - extra search fields
	JiraSearchFields = map[string][]string{
		"project_id":   {"fields", "project", "id"},
		"project_key":  {"fields", "project", "key"},
		"project_name": {"fields", "project", "name"},
		"issue_key":    {"key"},
	}
	// JiraRoles - roles defined for Jira backend
	JiraRoles = []string{"assignee", "reporter", "creator", "author", "updateAuthor"}
	// JiraCategories - categories defined for Jira
	JiraCategories = map[string]struct{}{"issue": {}}
	// JiraKeepCustomFiled - we're dropping all but those custom fields
	JiraKeepCustomFiled = map[string]struct{}{"Story Points": {}, "Sprint": {}}
	gMaxUpstreamDt      time.Time
	gMaxUpstreamDtMtx   = &sync.Mutex{}
	// JiraDataSource - constant
	JiraDataSource = &models.DataSource{Name: "Jira", Slug: "jira"}
	gJiraMetaData  = &models.MetaData{BackendName: "jira", BackendVersion: JiraBackendVersion}
)

// DSJira - DS implementation for Jira
type DSJira struct {
	// FIXME - change back to URL, but now rename to remember that origin is URL + (project if set)
	JiraURL      string // Jira URL, for example https://jira.onap.org
	User         string // If user is provided then we assume that we don't have base64 encoded user:token yet
	Token        string // If user is not specified we assume that token already contains "<username>:<your-api-token>"
	PageSize     int    // Max API page size, defaults to JiraDefaultPageSize
	FlagURL      *string
	FlagUser     *string
	FlagToken    *string
	FlagPageSize *int
}

// AddFlags - add Jira specific flags
func (j *DSJira) AddFlags() {
	j.FlagURL = flag.String("jira-url", "", "Jira URL, for example https://jira.onap.org")
	j.FlagPageSize = flag.Int("jira-page-size", JiraDefaultPageSize, fmt.Sprintf("Max API page size, defaults to JiraDefaultPageSize (%d)", JiraDefaultPageSize))
	j.FlagUser = flag.String("jira-user", "", "User: if user is provided then we assume that we don't have base64 encoded user:token yet")
	j.FlagToken = flag.String("jira-token", "", "Token: if user is not specified we assume that token already contains \"<username>:<your-api-token>\"")
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs(ctx *shared.Ctx) (err error) {
	// Jira Server URL
	if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
		j.JiraURL = *j.FlagURL
	}
	if ctx.EnvSet("URL") {
		j.JiraURL = ctx.Env("URL")
	}

	// Page size
	passed := shared.FlagPassed(ctx, "page-size")
	if passed {
		j.PageSize = *j.FlagPageSize
	}
	if ctx.EnvSet("PAGE_SIZE") {
		pageSize, err := strconv.Atoi(ctx.Env("PAGE_SIZE"))
		shared.FatalOnError(err)
		if pageSize > 0 {
			j.PageSize = pageSize
		}
	} else if !passed {
		j.PageSize = JiraDefaultPageSize
	}

	// SSO User
	if shared.FlagPassed(ctx, "user") && *j.FlagUser != "" {
		j.User = *j.FlagUser
	}
	if ctx.EnvSet("USER") {
		j.User = ctx.Env("USER")
	}
	if j.User != "" {
		shared.AddRedacted(j.User, false)
	}

	// SSO Token
	if shared.FlagPassed(ctx, "token") && *j.FlagToken != "" {
		j.Token = *j.FlagToken
	}
	if ctx.EnvSet("TOKEN") {
		j.Token = ctx.Env("TOKEN")
	}
	if j.Token != "" {
		shared.AddRedacted(j.Token, false)
	}

	// SSO: Handle either user,token pair or just a token
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
		shared.AddRedacted(j.Token, false)
	}
	// NOTE: don't forget this
	gJiraMetaData.Project = ctx.Project
	gJiraMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSJira) Validate() (err error) {
	if strings.HasSuffix(j.JiraURL, "/") {
		j.JiraURL = j.JiraURL[:len(j.JiraURL)-1]
	}
	if j.JiraURL == "" {
		err = fmt.Errorf("Jira URL must be set")
	}
	return
}

// Endpoint - used to mark (set/get) last update
// JiraURL is not enough, because if you filter per project (if iinternal --jira-project-filter flag is set) then those dates can be different
func (j *DSJira) Endpoint(ctx *shared.Ctx) string {
	if ctx.ProjectFilter && ctx.Project != "" {
		return j.JiraURL + " " + ctx.Project
	}
	return j.JiraURL
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
	endpoint := j.Endpoint(ctx)
	data = &models.Data{
		DataSource: JiraDataSource,
		MetaData:   gJiraMetaData,
		Endpoint:   endpoint,
	}
	source := data.DataSource.Slug
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		// Event
		// FIXME
		fmt.Printf("%s: %+v\n", source, doc)
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
			shared.SetLastUpdate(ctx, j.Endpoint(ctx), gMaxUpstreamDt)
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
	return
}

// Sync - sync Jira data source
func (j *DSJira) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.Endpoint(ctx), ctx.DateFrom, thrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.Endpoint(ctx))
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.Endpoint(ctx), ctx.DateFrom, thrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.Endpoint(ctx), ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
	// FIXME
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.Endpoint(ctx), gMaxUpstreamDt)
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
