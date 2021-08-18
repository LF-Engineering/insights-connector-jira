package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

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

// JiraField - informatin about fields present in issues
type JiraField struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Custom bool   `json:"custom"`
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

// GetFields - implement get fields for jira datasource
func (j *DSJira) GetFields(ctx *shared.Ctx) (customFields map[string]JiraField, err error) {
	url := j.JiraURL + JiraAPIRoot + JiraAPIField
	method := "GET"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	var resp interface{}
	// Week for caching fields, they don't change that often
	cacheFor := time.Duration(168) * time.Hour
	resp, _, _, _, err = shared.Request(ctx, url, method, headers, []byte{}, []string{}, nil, nil, map[[2]int]struct{}{{200, 200}: {}}, map[[2]int]struct{}{{200, 200}: {}}, true, &cacheFor, false)
	if err != nil {
		return
	}
	var fields []JiraField
	err = jsoniter.Unmarshal(resp.([]byte), &fields)
	if err != nil {
		return
	}
	customFields = make(map[string]JiraField)
	for _, field := range fields {
		if !field.Custom {
			continue
		}
		customFields[field.ID] = field
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

// ItemID - return unique identifier for an item
func (j *DSJira) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})["id"].(string)
	if !ok {
		shared.Fatalf("jira: ItemID() - cannot extract id from %+v", item)
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSJira) ItemUpdatedOn(item interface{}) time.Time {
	fields, ok := item.(map[string]interface{})["fields"].(map[string]interface{})
	if !ok {
		shared.Fatalf("jira: ItemUpdatedOn() - cannot extract fields from %+v", shared.DumpKeys(item))
	}
	sUpdated, ok := fields["updated"].(string)
	if !ok {
		shared.Fatalf("jira: ItemUpdatedOn() - cannot extract updated from %+v", shared.DumpKeys(fields))
	}
	updated, err := shared.TimeParseES(sUpdated)
	shared.FatalOnError(err)
	return updated
}

// GenSearchFields - generate extra search fields
func (j *DSJira) GenSearchFields(ctx *shared.Ctx, issue interface{}, uuid string) (fields map[string]interface{}) {
	searchFields := JiraSearchFields
	fields = make(map[string]interface{})
	fields[JiraDefaultSearchField] = uuid
	for field, keyAry := range searchFields {
		value, ok := shared.Dig(issue, keyAry, false, true)
		if ok {
			fields[field] = value
		}
	}
	if ctx.Debug > 1 {
		shared.Printf("returning search fields %+v\n", fields)
	}
	return
}

// AddMetadata - add metadata to the item
func (j *DSJira) AddMetadata(ctx *shared.Ctx, issue interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.JiraURL
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	if ctx.ProjectFilter && ctx.Project != "" {
		tags = append(tags, ctx.Project)
	}
	issueID := j.ItemID(issue)
	updatedOn := j.ItemUpdatedOn(issue)
	uuid := shared.UUIDNonEmpty(ctx, origin, issueID)
	timestamp := time.Now()
	mItem["backend_name"] = "jira"
	mItem["backend_version"] = JiraBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = "issue"
	mItem["search_fields"] = j.GenSearchFields(ctx, issue, uuid)
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	if ctx.Debug > 1 {
		shared.Printf("%s: %s: %v %v\n", origin, uuid, issueID, updatedOn)
	}
	return
}

// ProcessIssue - process a single issue
func (j *DSJira) ProcessIssue(ctx *shared.Ctx, allIssues, allDocs *[]interface{}, allIssuesMtx *sync.Mutex, issue interface{}, customFields map[string]JiraField, from time.Time, to *time.Time, thrN int) (wch chan error, err error) {
	var mtx *sync.RWMutex
	if thrN > 1 {
		mtx = &sync.RWMutex{}
	}
	issueID := j.ItemID(issue)
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Content-Type": "application/json", "Authorization": "Basic " + j.Token}
	} else {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	// Encode search params in query for GET requests
	encodeInQuery := true
	cacheFor := time.Duration(3) * time.Hour
	processIssue := func(c chan error) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		urlRoot := j.JiraURL + JiraAPIRoot + JiraAPIIssue + "/" + issueID + JiraAPIComment
		startAt := int64(0)
		maxResults := int64(j.PageSize)
		epochMS := from.UnixNano() / 1e6
		// Seems like original Jira was using project filter there which is not needed IMHO.
		var jql string
		if JiraFilterByProjectInComments {
			if to != nil {
				epochToMS := (*to).UnixNano() / 1e6
				if ctx.ProjectFilter && ctx.Project != "" {
					jql = fmt.Sprintf(`project = %s AND updated > %d AND updated < %d order by updated asc`, ctx.Project, epochMS, epochToMS)
				} else {
					jql = fmt.Sprintf(`updated > %d AND updated < %d order by updated asc`, epochMS, epochToMS)
				}
			} else {
				if ctx.ProjectFilter && ctx.Project != "" {
					jql = fmt.Sprintf(`project = %s AND updated > %d order by updated asc`, ctx.Project, epochMS)
				} else {
					jql = fmt.Sprintf(`updated > %d order by updated asc`, epochMS)
				}
			}
		} else {
			if to != nil {
				epochToMS := (*to).UnixNano() / 1e6
				jql = fmt.Sprintf(`updated > %d AND updated < %d order by updated asc`, epochMS, epochToMS)
			} else {
				jql = fmt.Sprintf(`updated > %d order by updated asc`, epochMS)
			}
		}
		method := "GET"
		for {
			var payloadBytes []byte
			url := urlRoot
			if encodeInQuery {
				// ?startAt=0&maxResults=100&jql=updated+%3E+0+order+by+updated+asc
				url += fmt.Sprintf(`?startAt=%d&maxResults=%d&jql=`, startAt, maxResults) + neturl.QueryEscape(jql)
			} else {
				payloadBytes = []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,"jql":"%s"}`, startAt, maxResults, jql))
			}
			var res interface{}
			res, _, _, _, e = shared.Request(
				ctx,
				url,
				method,
				headers,
				payloadBytes,
				[]string{},
				map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
				nil,                                 // Error statuses
				map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
				map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
				true,                                // retry
				&cacheFor,                           // cache duration
				false,                               // skip in dry-run mode
			)
			if e != nil {
				return
			}
			comments, ok := res.(map[string]interface{})["comments"].([]interface{})
			if !ok {
				e = fmt.Errorf("unable to unmarshal comments from %+v", shared.DumpKeys(res))
				return
			}
			if ctx.Debug > 1 {
				nComments := len(comments)
				if nComments > 0 {
					shared.Printf("processing %d comments\n", len(comments))
				}
			}
			if thrN > 1 {
				mtx.Lock()
			}
			issueComments, ok := issue.(map[string]interface{})["comments_data"].([]interface{})
			if !ok {
				issue.(map[string]interface{})["comments_data"] = []interface{}{}
			}
			issueComments, _ = issue.(map[string]interface{})["comments_data"].([]interface{})
			if !ok {
				issueComments = comments
			} else {
				issueComments = append(issueComments, comments...)
			}
			issue.(map[string]interface{})["comments_data"] = issueComments
			if thrN > 1 {
				mtx.Unlock()
			}
			totalF, ok := res.(map[string]interface{})["total"].(float64)
			if !ok {
				e = fmt.Errorf("unable to unmarshal total from %+v", shared.DumpKeys(res))
				return
			}
			maxResultsF, ok := res.(map[string]interface{})["maxResults"].(float64)
			if !ok {
				e = fmt.Errorf("unable to maxResults total from %+v", shared.DumpKeys(res))
				return
			}
			total := int64(totalF)
			maxResults = int64(maxResultsF)
			inc := int64(totalF)
			if maxResultsF < totalF {
				inc = int64(maxResultsF)
			}
			startAt += inc
			if startAt >= total {
				startAt = total
				break
			}
			if ctx.Debug > 0 {
				shared.Printf("processing next comments page from %d/%d\n", startAt, total)
			}
		}
		if ctx.Debug > 1 {
			shared.Printf("processed %d comments\n", startAt)
		}
		return
	}
	var ch chan error
	if thrN > 1 {
		ch = make(chan error)
		go func() {
			_ = processIssue(ch)
		}()
	} else {
		err = processIssue(nil)
		if err != nil {
			return
		}
	}
	if thrN > 1 {
		mtx.RLock()
	}
	issueFields, ok := issue.(map[string]interface{})["fields"].(map[string]interface{})
	if thrN > 1 {
		mtx.RUnlock()
	}
	if !ok {
		err = fmt.Errorf("unable to unmarshal fields from issue %+v", shared.DumpKeys(issue))
		return
	}
	if ctx.Debug > 1 {
		shared.Printf("before map custom: %+v\n", shared.DumpPreview(issueFields, 100))
	}
	type mapping struct {
		ID    string
		Name  string
		Value interface{}
	}
	if JiraMapCustomFields {
		m := make(map[string]mapping)
		for k, v := range issueFields {
			customField, ok := customFields[k]
			if !ok {
				continue
			}
			m[k] = mapping{ID: customField.ID, Name: customField.Name, Value: v}
		}
		for k, v := range m {
			if ctx.Debug > 1 {
				prev := issueFields[k]
				shared.Printf("mapping custom fields %s: %+v -> %+v\n", k, prev, v)
			}
			issueFields[k] = v
		}
	}
	if ctx.Debug > 1 {
		shared.Printf("after map custom: %+v\n", shared.DumpPreview(issueFields, 100))
	}
	// Extra fields
	if thrN > 1 {
		mtx.Lock()
	}
	esItem := j.AddMetadata(ctx, issue)
	// Seems like it doesn't make sense, because we just added those custom fields
	if JiraDropCustomFields {
		for k, v := range issueFields {
			if strings.HasPrefix(strings.ToLower(k), "customfield_") {
				mp, _ := v.(mapping)
				_, keep := JiraKeepCustomFiled[mp.Name]
				if !keep {
					delete(issueFields, k)
				}
			}
		}
	}
	if ctx.Debug > 1 {
		shared.Printf("after drop: %+v\n", shared.DumpPreview(issueFields, 100))
	}
	if ctx.Project != "" {
		issue.(map[string]interface{})["project"] = ctx.Project
	}
	esItem["data"] = issue
	if thrN > 1 {
		mtx.Unlock()
		err = <-ch
	}
	if allIssuesMtx != nil {
		allIssuesMtx.Lock()
	}
	*allIssues = append(*allIssues, esItem)
	nIssues := len(*allIssues)
	if nIssues >= ctx.PackSize {
		sendToQueue := func(c chan error) (e error) {
			defer func() {
				if c != nil {
					c <- e
				}
			}()
			e = j.JiraEnrichItems(ctx, thrN, *allIssues, allDocs, false)
			// e = SendToQueue(ctx, j, true, UUID, *allIssues)
			if e != nil {
				shared.Printf("error %v sending %d issues to queue\n", e, len(*allIssues))
			}
			*allIssues = []interface{}{}
			if allIssuesMtx != nil {
				allIssuesMtx.Unlock()
			}
			return
		}
		if thrN > 1 {
			wch = make(chan error)
			go func() {
				_ = sendToQueue(wch)
			}()
		} else {
			err = sendToQueue(nil)
			if err != nil {
				return
			}
		}
	} else {
		if allIssuesMtx != nil {
			allIssuesMtx.Unlock()
		}
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
	var customFields map[string]JiraField
	fieldsFetched := false
	var chF chan error
	getFields := func(c chan error) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
			if ctx.Debug > 0 {
				shared.Printf("got %d custom fields\n", len(customFields))
			}
		}()
		customFields, e = j.GetFields(ctx)
		return
	}
	if thrN > 1 {
		chF = make(chan error)
		go func() {
			_ = getFields(chF)
		}()
	} else {
		err = getFields(nil)
		if err != nil {
			shared.Printf("GetFields error: %+v\n", err)
			return
		}
		fieldsFetched = true
	}
	// '{"jql":"updated > 1601281314000 order by updated asc","startAt":0,"maxResults":400,"expand":["renderedFields","transitions","operations","changelog"]}'
	var (
		from time.Time
		to   *time.Time
	)
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
	} else {
		from = shared.DefaultDateFrom
	}
	to = ctx.DateTo
	url := j.JiraURL + JiraAPIRoot + JiraAPISearch
	startAt := int64(0)
	maxResults := int64(j.PageSize)
	jql := ""
	epochMS := from.UnixNano() / 1e6
	if to != nil {
		epochToMS := (*to).UnixNano() / 1e6
		if ctx.ProjectFilter && ctx.Project != "" {
			jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d AND updated < %d order by updated asc"`, ctx.Project, epochMS, epochToMS)
		} else {
			jql = fmt.Sprintf(`"jql":"updated > %d AND updated < %d order by updated asc"`, epochMS, epochToMS)
		}
	} else {
		if ctx.ProjectFilter && ctx.Project != "" {
			jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
		} else {
			jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
		}
	}
	expand := `"expand":["renderedFields","transitions","operations","changelog"]`
	var (
		allDocs      []interface{}
		allIssues    []interface{}
		allIssuesMtx *sync.Mutex
		escha        []chan error
		eschaMtx     *sync.Mutex
		chE          chan error
	)
	if thrN > 1 {
		chE = make(chan error)
		allIssuesMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	method := "POST"
	var headers map[string]string
	if j.Token != "" {
		// Token should be BASE64("useremail:api_token"), see: https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis
		headers = map[string]string{"Content-Type": "application/json", "Authorization": "Basic " + j.Token}
	} else {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	if ctx.Debug > 0 {
		shared.Printf("requesting issues from: %s\n", from)
	}
	cacheFor := time.Duration(3) * time.Hour
	for {
		payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s,%s}`, startAt, maxResults, jql, expand))
		var res interface{}
		res, _, _, _, err = shared.Request(
			ctx,
			url,
			method,
			headers,
			payloadBytes,
			[]string{},
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
			map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
			true,                                // retry
			&cacheFor,                           // cache duration
			false,                               // skip in dry-run mode
		)
		if err != nil {
			return
		}
		if !fieldsFetched {
			err = <-chF
			if err != nil {
				shared.Printf("GetFields error: %+v\n", err)
				return
			}
			fieldsFetched = true
		}
		processIssues := func(c chan error) (e error) {
			defer func() {
				if c != nil {
					c <- e
				}
			}()
			issues, ok := res.(map[string]interface{})["issues"].([]interface{})
			if !ok {
				e = fmt.Errorf("unable to unmarshal issues from %+v", shared.DumpKeys(res))
				return
			}
			if ctx.Debug > 0 {
				shared.Printf("processing %d issues\n", len(issues))
			}
			for _, issue := range issues {
				var esch chan error
				esch, e = j.ProcessIssue(ctx, &allIssues, &allDocs, allIssuesMtx, issue, customFields, from, to, thrN)
				if e != nil {
					shared.Printf("Error %v processing issue: %+v\n", e, issue)
					return
				}
				if esch != nil {
					if eschaMtx != nil {
						eschaMtx.Lock()
					}
					escha = append(escha, esch)
					if eschaMtx != nil {
						eschaMtx.Unlock()
					}
				}
			}
			return
		}
		if thrN > 1 {
			go func() {
				_ = processIssues(chE)
			}()
			nThreads++
			if nThreads == thrN {
				err = <-chE
				if err != nil {
					return
				}
				nThreads--
			}
		} else {
			err = processIssues(nil)
			if err != nil {
				return
			}
		}
		totalF, ok := res.(map[string]interface{})["total"].(float64)
		if !ok {
			err = fmt.Errorf("unable to unmarshal total from %+v", shared.DumpKeys(res))
			return
		}
		maxResultsF, ok := res.(map[string]interface{})["maxResults"].(float64)
		if !ok {
			err = fmt.Errorf("unable to maxResults total from %+v", shared.DumpKeys(res))
			return
		}
		total := int64(totalF)
		maxResults = int64(maxResultsF)
		inc := int64(totalF)
		if maxResultsF < totalF {
			inc = int64(maxResultsF)
		}
		startAt += inc
		if startAt >= total {
			startAt = total
			break
		}
		if ctx.Debug > 0 {
			shared.Printf("processing next issues page from %d/%d\n", startAt, total)
		}
	}
	for thrN > 1 && nThreads > 0 {
		err = <-chE
		nThreads--
		if err != nil {
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nIssues := len(allIssues)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining issues to send to queue\n", nIssues)
	}
	// NOTE: for all items, even if 0 - to flush the queue
	err = j.JiraEnrichItems(ctx, thrN, allIssues, &allDocs, true)
	//err = SendToQueue(ctx, j, true, UUID, allIssues)
	if err != nil {
		shared.Printf("Error %v sending %d issues to queue\n", err, len(allIssues))
	}
	shared.Printf("processed %d issues\n", startAt)
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
