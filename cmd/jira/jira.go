package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	neturl "net/url"

	"github.com/LF-Engineering/insights-connector-jira/build"
	"github.com/LF-Engineering/insights-datasource-shared/aws"
	"github.com/LF-Engineering/insights-datasource-shared/cache"
	"github.com/LF-Engineering/insights-datasource-shared/cryptography"
	"github.com/LF-Engineering/insights-datasource-shared/http"
	"github.com/LF-Engineering/lfx-event-schema/service"
	"github.com/LF-Engineering/lfx-event-schema/service/insights"
	"github.com/LF-Engineering/lfx-event-schema/service/insights/jira"
	"github.com/LF-Engineering/lfx-event-schema/service/user"
	"github.com/LF-Engineering/lfx-event-schema/utils/datalake"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"

	shared "github.com/LF-Engineering/insights-datasource-shared"
	elastic "github.com/LF-Engineering/insights-datasource-shared/elastic"
	logger "github.com/LF-Engineering/insights-datasource-shared/ingestjob"
	jsoniter "github.com/json-iterator/go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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
	// JiraDataSource - constant
	JiraDataSource = "jira"
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
	// JiraDefaultStream - Stream To Publish reviews
	JiraDefaultStream = "PUT-S3-jira"
	// JiraConnector ...
	JiraConnector = "jira-connector"
	// InProgress status
	InProgress = "in_progress"
	// Failed status
	Failed = "failed"
	// Success status
	Success = "success"
	// JiraIssue type
	JiraIssue = "issue"
	// IssuesCacheFile name
	IssuesCacheFile   = "issues-cache.csv"
	commentsCacheFile = "comments-cache"
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
	// gJiraMetaData  = &models.MetaData{BackendName: "jira", BackendVersion: JiraBackendVersion}
	// gRoleToType = map[string]string{
	// 	"issue_creator":        "jira_issue_created",
	// 	"issue_assignee":       "jira_issue_assignee_added",
	// 	"issue_reporter":       "jira_issue_reporter_added",
	// 	"comment_author":       "jira_comment_created",
	// 	"comment_updateAuthor": "jira_comment_updated",
	// }
	cachedIssues   = make(map[string]EntityCache)
	cachedComments = make(map[string][]EntityCache)
)

// Publisher - for streaming data to Kinesis
type Publisher interface {
	PushEvents(action, source, eventType, subEventType, env string, data []interface{}, endpoint string) (string, error)
}

// DSJira - DS implementation for Jira
type DSJira struct {
	URL          string // Jira URL, for example https://jira.onap.org
	User         string // If user is provided then we assume that we don't have base64 encoded user:token yet
	Token        string // If user is not specified we assume that token already contains "<username>:<your-api-token>"
	PageSize     int    // Max API page size, defaults to JiraDefaultPageSize
	FlagURL      *string
	FlagUser     *string
	FlagStream   *string
	FlagToken    *string
	FlagPageSize *int
	// Publisher & stream
	Publisher
	Stream        string // stream to publish the data
	Logger        logger.Logger
	log           *logrus.Entry
	cacheProvider cache.Manager
	endpoint      string
}

// JiraField - informatin about fields present in issues
type JiraField struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Custom bool   `json:"custom"`
}

// AddPublisher - sets Kinesis publisher
func (j *DSJira) AddPublisher(publisher Publisher) {
	j.Publisher = publisher
}

// PublisherPushEvents - this is a fake function to test publisher locally
// FIXME: don't use when done implementing
func (j *DSJira) PublisherPushEvents(ev, ori, src, cat, env string, v []interface{}) error {
	data, err := jsoniter.Marshal(v)
	j.log.WithFields(logrus.Fields{"operation": "PublisherPushEvents"}).Infof("publish[ev=%s ori=%s src=%s cat=%s env=%s]: %d items: %+v -> %v", ev, ori, src, cat, env, len(v), string(data), err)
	return nil
}

// AddFlags - add Jira specific flags
func (j *DSJira) AddFlags(ctx *shared.Ctx) {
	j.FlagURL = flag.String("jira-url", "", "Jira URL, for example https://jira.onap.org")
	j.FlagPageSize = flag.Int("jira-page-size", JiraDefaultPageSize, fmt.Sprintf("Max API page size, defaults to JiraDefaultPageSize (%d)", JiraDefaultPageSize))
	j.FlagUser = flag.String("jira-user", "", "User: if user is provided then we assume that we don't have base64 encoded user:token yet")
	j.FlagToken = flag.String("jira-token", "", "Token: if user is not specified we assume that token already contains \"<username>:<your-api-token>\"")
	j.FlagStream = flag.String("jira-stream", JiraDefaultStream, "jira kinesis stream name, for example PUT-S3-jira")
	j.AddLogger(ctx)
}

func (j *DSJira) mapRoleType(role string) insights.Role {
	// possible roles:
	// issue: "assignee", "reporter", "creator"
	// comment: "author", "updateAuthor"
	switch role {
	case "creator":
		return insights.AuthorRole
	case "assignee":
		return insights.AssigneeRole
	case "reporter":
		return insights.ReporterRole
	case "author", "updateAuthor":
		return insights.CommenterRole
	default:
		fmt.Printf("WARNING: unknown role '%s'\n", role)
	}
	return insights.Role(role)
}

// GetModelData - return data in lfx-event-schema format
func (j *DSJira) GetModelData(ctx *shared.Ctx, docs []interface{}) (map[string][]interface{}, error) {
	var data = make(map[string][]interface{})
	var err error
	defer func() {
		if err != nil {
			return
		}
		issueBaseEvent := jira.IssueBaseEvent{
			Connector:        insights.JiraConnector,
			ConnectorVersion: JiraBackendVersion,
			Source:           insights.JiraSource,
		}
		issueCommentBaseEvent := jira.IssueCommentBaseEvent{
			Connector:        insights.JiraConnector,
			ConnectorVersion: JiraBackendVersion,
			Source:           insights.JiraSource,
		}
		for k, v := range data {
			switch k {
			case "created":
				baseEvent := service.BaseEvent{
					Type: service.EventType(jira.IssueCreatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: JiraConnector,
						UpdatedBy: JiraConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issue := range v {
					ary = append(ary, jira.IssueCreatedEvent{
						IssueBaseEvent: issueBaseEvent,
						BaseEvent:      baseEvent,
						Payload:        issue.(jira.Issue),
					})
				}
				data[k] = ary
			case "updated":
				baseEvent := service.BaseEvent{
					Type: service.EventType(jira.IssueUpdatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: JiraConnector,
						UpdatedBy: JiraConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issue := range v {
					ary = append(ary, jira.IssueCreatedEvent{
						IssueBaseEvent: issueBaseEvent,
						BaseEvent:      baseEvent,
						Payload:        issue.(jira.Issue),
					})
				}
				data[k] = ary
			case "comment_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(jira.IssueCommentAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: JiraConnector,
						UpdatedBy: JiraConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueComment := range v {
					ary = append(ary, jira.IssueCommentAddedEvent{
						IssueCommentBaseEvent: issueCommentBaseEvent,
						BaseEvent:             baseEvent,
						Payload:               issueComment.(jira.IssueComment),
					})
				}
				data[k] = ary
			case "comment_edited":
				baseEvent := service.BaseEvent{
					Type: service.EventType(jira.IssueCommentEditedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: JiraConnector,
						UpdatedBy: JiraConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueComment := range v {
					ary = append(ary, jira.IssueCommentEditedEvent{
						IssueCommentBaseEvent: issueCommentBaseEvent,
						BaseEvent:             baseEvent,
						Payload:               issueComment.(jira.IssueComment),
					})
				}
				data[k] = ary
			case "comment_deleted":
				baseEvent := service.BaseEvent{
					Type: service.EventType(jira.IssueCommentDeletedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: JiraConnector,
						UpdatedBy: JiraConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, issueComment := range v {
					ary = append(ary, jira.IssueCommentDeletedEvent{
						IssueCommentBaseEvent: issueCommentBaseEvent,
						BaseEvent:             baseEvent,
						Payload:               issueComment.(jira.DeleteIssueComment),
					})
				}
				data[k] = ary
			default:
				err = fmt.Errorf("unknown issue '%s' event", k)
				return
			}
		}
	}()
	source := JiraDataSource
	for _, iDoc := range docs {
		var (
			labels     []string
			components []string
		)
		nComments := 0
		doc, _ := iDoc.(map[string]interface{})
		createdOn, _ := doc["creation_date"].(time.Time)
		// shared.Printf("createdOn: %+v for %+v\n", createdOn, iDoc)
		updatedOn, _ := doc["updated"].(time.Time)
		createdTz := ""
		// docUUID, _ := doc["uuid"].(string)
		//issueID, _ := doc["id"].(string)
		watchers, _ := doc["watchers"].(int)
		isClosed, _ := doc["is_closed"].(bool)
		jiraProjectID, _ := doc["project_id"].(string)
		projectKey, _ := doc["project_key"].(string)
		projectName, _ := doc["project_name"].(string)
		sIssueBody, _ := doc["main_description"].(string)
		//issueURL, _ := doc["url"].(string)
		title, _ := doc["summary"].(string)
		iLabels, okLabels := doc["labels"].([]interface{})
		if okLabels {
			for _, iLabel := range iLabels {
				label, _ := iLabel.(string)
				if label != "" {
					labels = append(labels, label)
				}
			}
		}
		iComponents, okComponents := doc["components"].([]interface{})
		if okComponents {
			for _, iCom := range iComponents {
				com, _ := iCom.(string)
				if com != "" {
					components = append(components, com)
				}
			}
		}
		projectID, err := jira.GenerateJiraProjectID(j.URL, jiraProjectID)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateJiraProjectID(%s,%s): %+v for %+v", jiraProjectID, j.URL, err, doc)
			return nil, err
		}
		sIssueBody, _ = doc["main_description"].(string)
		if createdTz == "" {
			createdTz = "UTC"
		}
		issueKey, _ := doc["key"].(string)
		sIID, _ := doc["id"].(string)
		issueID, err := jira.GenerateJiraIssueID(projectID, sIID)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateJiraIssueID(%s,%s): %+v for %+v", projectID, sIID, err, doc)
			return nil, err
		}
		url, _ := doc["url"].(string)
		state := insights.IssueOpen
		if isClosed {
			state = insights.IssueClosed
		}
		project := jira.Project{
			ID:          projectID,
			ProjectID:   jiraProjectID,
			ProjectKey:  projectKey,
			ProjectName: projectName,
		}
		issueContributors := []insights.Contributor{}
		// shared.Printf("createdOn: %+v, updatedOn: %+v for %s (%s,%s)\n", createdOn, updatedOn, sIID, issueID, issueKey)
		roles, okRoles := doc["roles"].([]map[string]interface{})
		if okRoles {
			for _, role := range roles {
				// possible roles: assignee, reporter, creator
				roleType, _ := role["type"].(string)
				roleValue := j.mapRoleType(roleType)
				name, _ := role["name"].(string)
				username, _ := role["username"].(string)
				email, _ := role["email"].(string)
				avatarURL, _ := role["avatar_url"].(string)
				userID, err := user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v", source, email, name, username, err, doc)
					return nil, err
				}
				contributor := insights.Contributor{
					Role:   roleValue,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Avatar:     avatarURL,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     source,
					},
				}
				issueContributors = append(issueContributors, contributor)
			}
		}
		// Comments start
		uComments := make(map[string]jira.IssueComment)
		oldComments := cachedComments[issueID]
		comments, okComments := doc["issue_comments"].([]map[string]interface{})
		if okComments {
			for _, comment := range comments {
				var (
					commentBody *string
					commentURL  *string
				)
				commentRoles, okCommentRoles := comment["roles"].([]map[string]interface{})
				if okCommentRoles {
					sCommentBody, _ := comment["body"].(string)
					if sCommentBody != "" {
						commentBody = &sCommentBody
					}
				}
				// Output update comment only if actually updated (comment update date > comment create date)
				dtMap := make(map[string]time.Time)
				for _, roleData := range commentRoles {
					roleType, _ := roleData["type"].(string)
					dt, _ := roleData["dt"].(time.Time)
					dtMap[roleType] = dt
				}
				var updateDt time.Time
				skipUpdate, okUpdate := false, false
				createDt, okCreate := dtMap["author"]
				if okCreate {
					updateDt, okUpdate = dtMap["updateAuthor"]
					if okUpdate && !updateDt.After(createDt) {
						skipUpdate = true
					}
				}
				// fmt.Printf("(%+v,%+v),(%+v,%+v),%+v\n", createDt, okCreate, updateDt, okUpdate, skipUpdate)
				for _, roleData := range commentRoles {
					// possible roles: author, updateAuthor
					roleType, _ := roleData["type"].(string)
					if skipUpdate && roleType == "updateAuthor" {
						continue
					}
					roleValue := j.mapRoleType(roleType)
					name, _ := roleData["name"].(string)
					username, _ := roleData["username"].(string)
					email, _ := roleData["email"].(string)
					avatarURL, _ := roleData["avatar_url"].(string)
					userID, err := user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v", source, email, name, username, err, doc)
						return nil, err
					}
					commentCreatedOn := createDt
					if okUpdate && updateDt.After(updatedOn) {
						// shared.Printf("%s (%s,%s) updatedOn: %+v -> %+v\n", sIID, issueID, issueKey, updatedOn, updateDt)
						updatedOn = updateDt
					}
					// fmt.Printf("(%+v,%+v)\n", commentCreatedOn, updatedOn)
					contributor := insights.Contributor{
						Role:   roleValue,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Avatar:     avatarURL,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					issueContributors = append(issueContributors, contributor)
					commentSID, _ := comment["comment_id"].(string)
					sCommentURL, _ := comment["comment_url"].(string)
					if sCommentURL != "" {
						commentURL = &sCommentURL
					}
					issueCommentID, err := jira.GenerateJiraCommentID(projectID, commentSID)
					if err != nil {
						j.log.WithFields(logrus.Fields{"operation": "GetModelData"}).Errorf("GenerateJiraCommentID(%s,%s): %+v for %+v", projectID, commentSID, err, doc)
						return nil, err
					}
					nComments++
					// shared.Printf("%s (%s,%s) nComments: %d\n", sIID, issueID, issueKey, nComments)
					cBody := ""
					cURL := ""
					if commentBody != nil {
						cBody = *commentBody
					}
					if commentURL != nil {
						cURL = *commentURL
					}
					issueComment := jira.IssueComment{
						ID:      issueCommentID,
						IssueID: issueID,
						Comment: insights.Comment{
							Body:            cBody,
							CommentURL:      cURL,
							SourceTimestamp: commentCreatedOn,
							SyncTimestamp:   time.Now(),
							CommentID:       commentSID,
							Contributor:     contributor,
							Orphaned:        false,
						},
					}
					var key string
					if roleType == "updateAuthor" {
						key = "comment_edited"
					} else {
						key = "comment_added"
					}
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{issueComment}
					} else {
						ary = append(ary, issueComment)
					}
					data[key] = ary
					found := false
					for _, oldc := range oldComments {
						if oldc.EntityID == issueCommentID {
							found = true
							break
						}
					}
					if !found {
						key := "comment_added"
						ary, ok := data[key]
						if !ok {
							ary = []interface{}{issueComment}
						} else {
							ary = append(ary, issueComment)
						}
						data[key] = ary
					}
					uComments[issueCommentID] = issueComment
				}
			}
		}
		for _, comm := range oldComments {
			deleted := true
			edited := false
			for newCommID, commentVal := range uComments {
				if newCommID == comm.EntityID {
					deleted = false
					contentHash := fmt.Sprintf("%x", sha256.Sum256([]byte(commentVal.Body)))
					if contentHash != comm.Hash {
						edited = true
					}
					break
				}
			}
			if deleted {
				rvComm := jira.DeleteIssueComment{
					ID:      comm.EntityID,
					IssueID: issueID,
				}
				key := "comment_deleted"
				ary, ok := data[key]
				if !ok {
					ary = []interface{}{rvComm}
				} else {
					ary = append(ary, rvComm)
				}
				data[key] = ary
			}
			if edited {
				editedComment := jira.IssueComment{
					ID:      comm.EntityID,
					IssueID: issueID,
					Comment: insights.Comment{
						Body:            uComments[comm.EntityID].Body,
						CommentURL:      uComments[comm.EntityID].CommentURL,
						CommentID:       uComments[comm.EntityID].CommentID,
						Contributor:     uComments[comm.EntityID].Contributor,
						SyncTimestamp:   time.Now(),
						SourceTimestamp: uComments[comm.EntityID].SourceTimestamp,
					},
				}
				key := "comment_edited"
				ary, ok := data[key]
				if !ok {
					ary = []interface{}{editedComment}
				} else {
					ary = append(ary, editedComment)
				}
				data[key] = ary
			}
		}
		updatedComments := make([]EntityCache, 0)
		for _, c := range uComments {
			updatedComments = append(updatedComments, EntityCache{
				Timestamp:      fmt.Sprintf("%v", c.SyncTimestamp.Unix()),
				EntityID:       c.ID,
				SourceEntityID: c.CommentID,
				Hash:           fmt.Sprintf("%x", sha256.Sum256([]byte(c.Body))),
				Orphaned:       false,
			})
		}
		cachedComments[issueID] = updatedComments
		// Comments end
		// Final Issue object
		issue := jira.Issue{
			ID:           issueID,
			IssueKey:     issueKey,
			EndpointID:   projectID,
			ServerURL:    j.URL,
			Project:      project,
			Labels:       labels,
			Watchers:     watchers,
			Components:   components,
			Contributors: shared.DedupContributors(issueContributors),
			Issue: insights.Issue{
				Title:         title,
				Body:          sIssueBody,
				IssueID:       sIID,
				IssueURL:      url,
				State:         insights.IssueState(state),
				SyncTimestamp: time.Now(),
				Orphaned:      false,
			},
		}
		isCreated := isKeyCreated(issueID)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "GetModelDataPullRequest"}).Errorf("error getting cache for endpoint %s/%s. error: %+v", j.endpoint, JiraIssue, err)
			return data, err
		}
		key := "updated"
		issue.SourceTimestamp = updatedOn
		if !isCreated {
			issue.SourceTimestamp = createdOn
			key = "created"
		}
		// shared.Printf("%s (%s,%s) final (createdOn, updatedOn, nComments, key): (%+v, %+v, %d, %s)\n", sIID, issueID, issueKey, createdOn, updatedOn, nComments, key)
		ary, ok := data[key]
		if !ok {
			ary = []interface{}{issue}
		} else {
			ary = append(ary, issue)
		}
		data[key] = ary
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
	}
	return data, nil
}

// OutputDocs - send output documents to the consumer
func (j *DSJira) OutputDocs(ctx *shared.Ctx, items []interface{}, docs *[]interface{}, final bool) {
	if len(*docs) > 0 {
		// actual output
		j.log.WithFields(logrus.Fields{"operation": "OutputDocs"}).Infof("output processing(%d/%d/%v)", len(items), len(*docs), final)
		var (
			issuesData map[string][]interface{}
			jsonBytes  []byte
			err        error
		)

		issuesData, err = j.GetModelData(ctx, *docs)
		endpoint := strings.ReplaceAll(j.endpoint, "/", "-")
		if err == nil {
			if j.Publisher != nil {
				insightsStr := "insights"
				issuesStr := "issues"
				envStr := os.Getenv("STAGE")
				for k, v := range issuesData {
					switch k {
					case "created":
						ev, _ := v[0].(jira.IssueCreatedEvent)
						path, err := j.Publisher.PushEvents(ev.Event(), insightsStr, JiraDataSource, issuesStr, envStr, v, endpoint)
						for _, val := range v {
							payload := val.(jira.IssueCreatedEvent).Payload
							b, er := json.Marshal(val.(jira.IssueCreatedEvent))
							if er != nil {
								j.log.WithFields(logrus.Fields{"operation": "GitEnrichItems"}).Errorf("error marshal data for issue %s, error %v", payload.IssueID, err)
								continue
							}

							tStamp := payload.SyncTimestamp.Unix()
							contentHash := fmt.Sprintf("%x", sha256.Sum256(b))
							cachedIssues[payload.ID] = EntityCache{
								Timestamp:      fmt.Sprintf("%v", tStamp),
								EntityID:       payload.ID,
								SourceEntityID: payload.IssueID,
								Hash:           contentHash,
								FileLocation:   path,
							}
						}
					case "updated":
						ev, _ := v[0].(jira.IssueUpdatedEvent)
						_, err = j.Publisher.PushEvents(ev.Event(), insightsStr, JiraDataSource, issuesStr, envStr, v, endpoint)
					case "comment_added":
						ev, _ := v[0].(jira.IssueCommentAddedEvent)
						_, err = j.Publisher.PushEvents(ev.Event(), insightsStr, JiraDataSource, issuesStr, envStr, v, endpoint)
					case "comment_edited":
						ev, _ := v[0].(jira.IssueCommentEditedEvent)
						_, err = j.Publisher.PushEvents(ev.Event(), insightsStr, JiraDataSource, issuesStr, envStr, v, endpoint)
					case "comment_deleted":
						ev, _ := v[0].(jira.IssueCommentDeletedEvent)
						_, err = j.Publisher.PushEvents(ev.Event(), insightsStr, JiraDataSource, issuesStr, envStr, v, endpoint)
					default:
						err = fmt.Errorf("unknown jira issue event type '%s'", k)
					}
					if err != nil {
						break
					}
				}
				err = j.createCacheFile([]EntityCache{}, "")
				if err != nil {
					return
				}

				comB, err := jsoniter.Marshal(cachedComments)
				if err != nil {
					return
				}
				if err = j.cacheProvider.UpdateFileByKey(j.endpoint, commentsCacheFile, comB); err != nil {
					return
				}
			} else {
				jsonBytes, err = jsoniter.Marshal(issuesData)
			}
		}
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "OutputDocs"}).Errorf("Error GetModelData: %+v", err)
			return
		}
		if j.Publisher == nil {
			j.log.WithFields(logrus.Fields{"operation": "OutputDocs"}).Errorf("publisher: %s", string(jsonBytes))
		}
		*docs = []interface{}{}
		gMaxUpstreamDtMtx.Lock()
		defer gMaxUpstreamDtMtx.Unlock()
		err = j.cacheProvider.SetLastSync(j.endpoint, gMaxUpstreamDt)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "OutputDocs"}).Infof("unable to set last sync date to cache.error: %v", err)
		}
	}
}

// AddLogger - adds a logger
func (j *DSJira) AddLogger(ctx *shared.Ctx) {
	client, err := elastic.NewClientProvider(&elastic.Params{
		URL:      os.Getenv("ELASTIC_LOG_URL"),
		Password: os.Getenv("ELASTIC_LOG_PASSWORD"),
		Username: os.Getenv("ELASTIC_LOG_USER"),
	})
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "AddLogger"}).Errorf("AddLogger error: %+v", err)
		return
	}

	logProvider, err := logger.NewLogger(client, os.Getenv("STAGE"))
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "AddLogger"}).Errorf("NewLogger error: %+v", err)
		return
	}

	j.Logger = *logProvider
}

// WriteLog - writes to log
func (j *DSJira) WriteLog(ctx *shared.Ctx, timestamp time.Time, status, message string) error {
	arn, err := aws.GetContainerARN()
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "WriteLog"}).Errorf("getContainerMetadata Error : %+v", err)
		return err
	}
	err = j.Logger.Write(&logger.Log{
		Connector: JiraDataSource,
		TaskARN:   arn,
		Configuration: []map[string]string{
			{
				"JIRA_URL":     j.URL,
				"JIRA_PROJECT": ctx.Project,
				"ProjectSlug":  ctx.Project,
			}},
		Status:    status,
		CreatedAt: timestamp,
		Message:   message,
	})
	return err
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs(ctx *shared.Ctx) error {
	// Cryptography
	encrypt, err := cryptography.NewEncryptionClient()
	if err != nil {
		return err
	}

	// Jira Server URL
	if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
		j.URL = *j.FlagURL
	}
	if ctx.EnvSet("URL") {
		j.URL = ctx.Env("URL")
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
		j.User, err = encrypt.Decrypt(j.User)
		if err != nil {
			return err
		}
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
		j.Token, err = encrypt.Decrypt(j.Token)
		if err != nil {
			return err
		}
		shared.AddRedacted(j.Token, false)
	}

	// SSO: Handle either user,token pair or just a token
	if j.User != "" {
		// If user is specified, then we must calculate base64(user:token) to get a real token
		j.Token = base64.StdEncoding.EncodeToString([]byte(j.User + ":" + j.Token))
		shared.AddRedacted(j.Token, false)
	}

	// jira Kinesis stream
	j.Stream = JiraDefaultStream
	if shared.FlagPassed(ctx, "stream") {
		j.Stream = *j.FlagStream
	}
	if ctx.EnvSet("STREAM") {
		j.Stream = ctx.Env("STREAM")
	}

	return nil
}

// Validate - is current DS configuration OK?
func (j *DSJira) Validate() (err error) {
	j.URL = strings.TrimSuffix(j.URL, "/")
	if j.URL == "" {
		err = fmt.Errorf("jira URL must be set")
	}
	return
}

// Endpoint - used to mark (set/get) last update
// URL is not enough, because if you filter per project (if iinternal --jira-project-filter flag is set) then those dates can be different
func (j *DSJira) Endpoint(ctx *shared.Ctx) string {
	if ctx.ProjectFilter && ctx.Project != "" {
		return j.URL + " " + ctx.Project
	}
	return j.URL
}

// Init - initialize Jira data source
func (j *DSJira) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("Jira")
	j.AddFlags(ctx)
	ctx.Init()

	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}

	err = j.Validate()
	if err != nil {
		return err
	}

	if ctx.Debug > 1 {
		j.log.WithFields(logrus.Fields{"operation": "Init"}).Debugf("Jira: %+v\nshared context: %s", j, ctx.Info())
	}

	if ctx.Debug > 0 {
		j.log.WithFields(logrus.Fields{"operation": "Init"}).Debugf("stream: '%s'", j.Stream)
	}

	if j.Stream != "" {
		sess, err := session.NewSession()
		if err != nil {
			return err
		}
		s3Client := s3.New(sess)
		objectStore := datalake.NewS3ObjectStore(s3Client)
		datalakeClient := datalake.NewStoreClient(&objectStore)
		j.AddPublisher(&datalakeClient)
	}

	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSJira) GetRoleIdentity(ctx *shared.Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	ident := make(map[string]interface{})
	user, ok := shared.Dig(item, []string{role}, false, true)
	/*
		defer func() {
			fmt.Printf("GetRoleIdentity(%s): %v: %+v\n", role, ok, identity)
		}()
	*/
	if !ok {
		return
	}
	data := [][2]string{
		{"name", "displayName"},
		{"username", "name"},
		{"email", "emailAddress"},
		{"tz", "timeZone"},
	}
	any := false
	for _, row := range data {
		iV, ok := shared.Dig(user, []string{row[1]}, false, true)
		if !any && ok {
			v, _ := iV.(string)
			if v != "" {
				any = true
			}
		}
		ident[row[0]] = iV
	}
	if any {
		ident["type"] = role
		identity = ident
	}
	return
}

// EnrichComments - return rich item from raw item for a given author type
func (j *DSJira) EnrichComments(ctx *shared.Ctx, comments []interface{}, item map[string]interface{}) ([]map[string]interface{}, error) {
	var (
		richComments []map[string]interface{}
		err          error
	)

	for _, comment := range comments {
		richComment := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := item[field]
			richComment[field] = v
		}
		fields := []string{"project_id", "project_key", "project_name", "issue_type", "issue_description"}
		for _, field := range fields {
			richComment[field] = item[field]
		}
		// This overwrites project passed from outside, but this was requested, we can comment this out if needed
		if ctx.Project == "" {
			richComment["project"] = item["project_key"]
		} else {
			richComment["project"] = ctx.Project
		}
		richComment["issue_key"] = item["key"]
		richComment["issue_url"] = item["url"]
		authors := []string{"author", "updateAuthor"}
		for _, a := range authors {
			author, ok := shared.Dig(comment, []string{a}, false, true)
			if ok {
				richComment[a], _ = shared.Dig(author, []string{"displayName"}, true, false)
				tz, ok := shared.Dig(author, []string{"timeZone"}, false, true)
				if ok {
					richComment[a+"_tz"] = tz
				}
			} else {
				richComment[a] = nil
			}
		}
		var dt time.Time
		dtMap := map[string]time.Time{}
		for _, field := range []string{"created", "updated"} {
			idt, _ := shared.Dig(comment, []string{field}, true, false)
			dt, err = shared.TimeParseInterfaceString(idt)
			if err != nil {
				richComment[field] = nil
			} else {
				richComment[field] = dt
			}
			if field == "created" {
				dtMap["author"] = dt
			} else {
				dtMap["updateAuthor"] = dt
			}
		}
		cid, _ := shared.Dig(comment, []string{"id"}, true, false)
		richComment["body"], _ = shared.Dig(comment, []string{"body"}, true, false)
		richComment["comment_id"] = cid
		iid, ok := item["id"].(string)
		if !ok {
			err = fmt.Errorf("missing string id field in issue %+v", shared.DumpKeys(item))
			return richComments, err
		}
		comid, ok := cid.(string)
		if !ok {
			err = fmt.Errorf("missing string id field in comment %+v", shared.DumpKeys(comment))
			return richComments, err
		}
		richComment["id"] = fmt.Sprintf("%s_comment_%s", iid, comid)
		richComment["type"] = "comment"
		richComment["comment_url"], _ = shared.Dig(comment, []string{"self"}, true, false)
		roleIdents := []map[string]interface{}{}
		for _, role := range []string{"author", "updateAuthor"} {
			iComment, _ := comment.(map[string]interface{})
			identity := j.GetRoleIdentity(ctx, iComment, role)
			if identity != nil {
				dt := dtMap[role]
				identity["dt"] = dt
				roleIdents = append(roleIdents, identity)
			}
		}
		richComment["roles"] = roleIdents
		richComment["metadata__enriched_on"] = time.Now()
		richComments = append(richComments, richComment)
	}

	return richComments, nil
}

// EnrichItem - return rich item from raw item
func (j *DSJira) EnrichItem(ctx *shared.Ctx, item map[string]interface{}, roles []string) (rich map[string]interface{}, err error) {
	// copy RawFields
	rich = make(map[string]interface{})
	for _, field := range shared.RawFields {
		v := item[field]
		rich[field] = v
	}
	issue, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpKeys(item))
		return
	}
	changes, ok := shared.Dig(issue, []string{"changelog", "total"}, false, false)
	if ok {
		rich["changes"] = changes
	} else {
		// Only evil Jiras do that, for example http://jira.akraino.org
		// Almost the same address works OK https://jira.akraino.org
		rich["changes"] = 0
	}
	fields, ok := issue["fields"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing fields field in issue %+v", shared.DumpKeys(issue))
		return
	}
	created, _ := shared.Dig(fields, []string{"created"}, true, false)
	updated, _ := shared.Dig(fields, []string{"updated"}, false, true)
	var (
		sCreated  string
		createdDt time.Time
		sUpdated  string
		updatedDt time.Time
		e         error
		o         bool
	)
	sCreated, o = created.(string)
	if o {
		createdDt, e = shared.TimeParseES(sCreated)
		if e != nil {
			o = false
		}
	}
	if o {
		sUpdated, o = updated.(string)
	}
	if o {
		updatedDt, e = shared.TimeParseES(sUpdated)
		if e != nil {
			o = false
		}
	}
	rich["creation_date"] = createdDt
	rich["updated"] = updatedDt
	roleIdents := []map[string]interface{}{}
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, fields, role)
		if identity != nil {
			identity["dt"] = createdDt
			roleIdents = append(roleIdents, identity)
		}
	}
	rich["roles"] = roleIdents
	// fmt.Printf("issue created at %+v, roles: roleIdents: %+v\n", createdDt, roleIdents)
	desc, ok := fields["description"].(string)
	if ok {
		rich["main_description_analyzed"] = desc
		if len(desc) > shared.KeywordMaxlength {
			desc = desc[:shared.KeywordMaxlength]
		}
		rich["main_description"] = desc
	}
	rich["issue_type"], _ = shared.Dig(fields, []string{"issuetype", "name"}, true, false)
	rich["issue_description"], _ = shared.Dig(fields, []string{"issuetype", "description"}, true, false)
	labels, ok := fields["labels"]
	if ok {
		rich["labels"] = labels
	}
	priority, ok := shared.Dig(fields, []string{"priority", "name"}, false, true)
	if ok {
		rich["priority"] = priority
	}
	progress, ok := shared.Dig(fields, []string{"progress", "total"}, false, true)
	if ok {
		rich["progress_total"] = progress
	}
	rich["project_id"], _ = shared.Dig(fields, []string{"project", "id"}, true, false)
	rich["project_key"], _ = shared.Dig(fields, []string{"project", "key"}, true, false)
	rich["project_name"], _ = shared.Dig(fields, []string{"project", "name"}, true, false)
	// This overwrites project passed from outside, but this was requested, we can comment this out if needed
	if ctx.Project == "" {
		rich["project"] = rich["project_key"]
	} else {
		rich["project"] = ctx.Project
	}
	resolution, ok := fields["resolution"]
	if ok && resolution != nil {
		rich["resolution_id"], _ = shared.Dig(resolution, []string{"id"}, true, false)
		rich["resolution_name"], _ = shared.Dig(resolution, []string{"name"}, true, false)
		rich["resolution_description"], _ = shared.Dig(resolution, []string{"description"}, true, false)
		rich["resolution_self"], _ = shared.Dig(resolution, []string{"self"}, true, false)
	}
	rich["resolution_date"], _ = shared.Dig(fields, []string{"resolutiondate"}, true, false)
	rich["status_description"], _ = shared.Dig(fields, []string{"status", "description"}, true, false)
	rich["status"], _ = shared.Dig(fields, []string{"status", "name"}, true, false)
	rich["status_category_key"], _ = shared.Dig(fields, []string{"status", "statusCategory", "key"}, true, false)
	rich["is_closed"] = false
	catKey, _ := rich["status_category_key"].(string)
	if catKey == ClosedStatusCategoryKey {
		rich["is_closed"] = true
	}
	rich["summary"], _ = shared.Dig(fields, []string{"summary"}, true, false)
	timeoriginalestimate, ok := shared.Dig(fields, []string{"timeoriginalestimate"}, false, true)
	if ok {
		rich["original_time_estimation"] = timeoriginalestimate
		if timeoriginalestimate != nil {
			fVal, ok := timeoriginalestimate.(float64)
			if ok {
				rich["original_time_estimation_hours"] = int(fVal / 3600.0)
			}
		}
	}
	timespent, ok := shared.Dig(fields, []string{"timespent"}, false, true)
	if ok {
		rich["time_spent"] = timespent
		if timespent != nil {
			fVal, ok := timespent.(float64)
			if ok {
				rich["time_spent_hours"] = int(fVal / 3600.0)
			}
		}
	}
	timeestimate, ok := shared.Dig(fields, []string{"timeestimate"}, false, true)
	if ok {
		rich["time_estimation"] = timeestimate
		if timeestimate != nil {
			fVal, ok := timeestimate.(float64)
			if ok {
				rich["time_estimation_hours"] = int(fVal / 3600.0)
			}
		}
	}
	rich["watchers"], _ = shared.Dig(fields, []string{"watches", "watchCount"}, true, false)
	iKey, _ := shared.Dig(issue, []string{"key"}, true, false)
	key, ok := iKey.(string)
	if !ok {
		err = fmt.Errorf("cannot read key as string from %T %+v", iKey, iKey)
		return
	}
	rich["key"] = key
	iid, ok := issue["id"].(string)
	if !ok {
		err = fmt.Errorf("missing string id field in issue %+v", shared.DumpKeys(issue))
		return
	}
	rich["id"] = iid
	// rich["id"] = fmt.Sprintf("%s_issue_%s_user_%s", rich[UUID], iid, author)
	rich["number_of_comments"] = 0
	comments, ok := issue["comments_data"].([]interface{})
	if ok {
		rich["number_of_comments"] = len(comments)
	}
	origin, ok := rich["origin"].(string)
	if !ok {
		err = fmt.Errorf("cannot read origin as string from rich %+v", rich)
		return
	}
	rich["url"] = origin + "/browse/" + key
	fixVersions, ok := shared.Dig(fields, []string{"fixVersions"}, false, true)
	if ok {
		rels := []interface{}{}
		versions, ok := fixVersions.([]interface{})
		if ok {
			for _, version := range versions {
				name, ok := shared.Dig(version, []string{"name"}, false, true)
				if ok {
					rels = append(rels, name)
				}
			}
		}
		rich["releases"] = rels
	}
	components, ok := shared.Dig(fields, []string{"components"}, false, true)
	if ok {
		coms := []interface{}{}
		cs, ok := components.([]interface{})
		if ok {
			for _, com := range cs {
				name, ok := shared.Dig(com, []string{"name"}, false, true)
				if ok {
					coms = append(coms, name)
				}
			}
		}
		rich["components"] = coms
	}
	for field, fieldValue := range fields {
		if !strings.HasPrefix(strings.ToLower(field), "customfield_") {
			continue
		}
		f, ok := fieldValue.(map[string]interface{})
		if !ok {
			continue
		}
		name, ok := f["Name"]
		if !ok {
			continue
		}
		if name == "Story Points" {
			rich["story_points"] = f["value"]
		} else if name == "Sprint" {
			v, ok := f["value"]
			if !ok {
				continue
			}
			iAry, ok := v.([]interface{})
			if !ok {
				continue
			}
			if len(iAry) == 0 {
				continue
			}
			s, ok := iAry[0].(string)
			if !ok {
				continue
			}
			rich["sprint"] = strings.Split(shared.PartitionString(s, ",name=")[2], ",")[0]
			rich["sprint_start"] = strings.Split(shared.PartitionString(s, ",startDate=")[2], ",")[0]
			rich["sprint_end"] = strings.Split(shared.PartitionString(s, ",endDate=")[2], ",")[0]
			rich["sprint_complete"] = strings.Split(shared.PartitionString(s, ",completeDate=")[2], ",")[0]
		}
	}
	rich["type"] = "issue"
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// GetFields - implement get fields for jira datasource
func (j *DSJira) GetFields(ctx *shared.Ctx) (customFields map[string]JiraField, err error) {
	url := j.URL + JiraAPIRoot + JiraAPIField
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
	j.log.WithFields(logrus.Fields{"operation": "JiraEnrichItems"}).Infof("input processing(%d/%d/%v)", len(items), len(*docs), final)
	if final {
		defer func() {
			j.OutputDocs(ctx, items, docs, final)
		}()
	}

	// NOTE: non-generic code starts
	if ctx.Debug > 0 {
		j.log.WithFields(logrus.Fields{"operation": "JiraEnrichItems"}).Debugf("jira enrich items %d/%d func", len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	nThreads := 0
	roles := []string{"creator", "assignee", "reporter"}
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// NOTE: never refer to _source - we no longer use ES
		doc, ok := item.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = j.EnrichItem(ctx, doc, roles)
		if e != nil {
			return
		}
		defer func() {
			if thrN > 1 {
				mtx.Lock()
			}
			*docs = append(*docs, rich)
			// NOTE: flush here
			if len(*docs) >= ctx.PackSize {
				j.OutputDocs(ctx, items, docs, final)
			}
			if thrN > 1 {
				mtx.Unlock()
			}
		}()

		comms, ok := shared.Dig(doc, []string{"data", "comments_data"}, false, true)
		if !ok {
			return
		}
		comments, _ := comms.([]interface{})
		if len(comments) == 0 {
			return
		}
		var richComments []map[string]interface{}
		richComments, e = j.EnrichComments(ctx, comments, rich)
		if e != nil {
			return
		}
		rich["issue_comments"] = richComments
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
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
		j.log.WithFields(logrus.Fields{"operation": "GenSearchFields"}).Debugf("returning search fields %+v", fields)
	}
	return
}

// AddMetadata - add metadata to the item
func (j *DSJira) AddMetadata(ctx *shared.Ctx, issue interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
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
		j.log.WithFields(logrus.Fields{"operation": "AddMetadata"}).Debugf("%s: %s: %v %v", origin, uuid, issueID, updatedOn)
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
		urlRoot := j.URL + JiraAPIRoot + JiraAPIIssue + "/" + issueID + JiraAPIComment
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
				url += fmt.Sprintf(`?maxResults=%d&jql=`, maxResults) + neturl.QueryEscape(jql)
			} else {
				payloadBytes = []byte(fmt.Sprintf(`{"maxResults":%d,"jql":"%s"}`, maxResults, jql))
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
					j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Infof("processing %d comments", len(comments))
				}
			}
			if thrN > 1 {
				mtx.Lock()
			}

			issueComments, ok := issue.(map[string]interface{})["comments_data"].([]interface{})
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
				j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Debugf("processing next comments page from %d/%d", startAt, total)
			}
		}

		if ctx.Debug > 1 {
			j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Debugf("processed %d comments", startAt)
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
		j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Debugf("before map custom: %+v", shared.DumpPreview(issueFields, 100))
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
				j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Infof("mapping custom fields %s: %+v -> %+v", k, prev, v)
			}
			issueFields[k] = v
		}
	}

	if ctx.Debug > 1 {
		j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Debugf("after map custom: %+v", shared.DumpPreview(issueFields, 100))
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
		j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Debugf("after drop: %+v", shared.DumpPreview(issueFields, 100))
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
				j.log.WithFields(logrus.Fields{"operation": "ProcessIssue"}).Errorf("error %v sending %d issues to queue", e, len(*allIssues))
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

// ItemNullableDate - return date value for a given field name, can be null
func (j *DSJira) ItemNullableDate(item interface{}, field string) *time.Time {
	iWhen, ok := shared.Dig(item, []string{field}, false, true)
	if !ok || iWhen == nil {
		return nil
	}
	sWhen, ok := iWhen.(string)
	if !ok {
		// shared.Printf("ItemNullableDate: incorrect date (non string): %v,%T\n", iWhen, iWhen)
		return nil
	}
	when, err := shared.TimeParseES(sWhen)
	if err != nil {
		// shared.Printf("ItemNullableDate: incorrect date (cannot parse): %s,%v\n", sWhen, err)
		return nil
	}

	return &when
}

// Sync - sync Jira data source
func (j *DSJira) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s fetching from %v (%d threads)", j.Endpoint(ctx), ctx.DateFrom, thrN)
	}

	if ctx.DateFrom == nil {
		cachedLastSync, er := j.cacheProvider.GetLastSync(j.endpoint)
		if er != nil {
			err = er
			return
		}
		ctx.DateFrom = &cachedLastSync
		if ctx.DateFrom != nil {
			j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s resuming from %v (%d threads)", j.Endpoint(ctx), ctx.DateFrom, thrN)
		}
	}
	cachedIssues = make(map[string]EntityCache)
	j.getCachedIssues()
	if err = j.getCachedComments(); err != nil {
		return
	}

	if ctx.DateTo != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("%s fetching till %v (%d threads)", j.Endpoint(ctx), ctx.DateTo, thrN)
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
				j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("got %d custom fields", len(customFields))
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
			j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("GetFields error: %+v", err)
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
	url := j.URL + JiraAPIRoot + JiraAPISearch
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
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("requesting issues from: %s", from)
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
				j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("GetFields error: %+v", err)
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
				j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("processing %d issues", len(issues))
			}

			for _, issue := range issues {
				var esch chan error
				esch, e = j.ProcessIssue(ctx, &allIssues, &allDocs, allIssuesMtx, issue, customFields, from, to, thrN)
				if e != nil {
					j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("Error %v processing issue: %+v", e, issue)
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
			j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("processing next issues page from %d/%d", startAt, total)
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
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Debugf("%d remaining issues to send to queue", nIssues)
	}
	// NOTE: for all items, even if 0 - to flush the queue
	err = j.JiraEnrichItems(ctx, thrN, allIssues, &allDocs, true)
	//err = SendToQueue(ctx, j, true, UUID, allIssues)
	if err != nil {
		j.log.WithFields(logrus.Fields{"operation": "Sync"}).Errorf("Error %v sending %d issues to queue", err, len(allIssues))
	}
	j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("processed %d issues", startAt)
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	if !gMaxUpstreamDt.IsZero() {
		err = j.cacheProvider.SetLastSync(j.endpoint, gMaxUpstreamDt)
		if err != nil {
			j.log.WithFields(logrus.Fields{"operation": "Sync"}).Infof("unable to set last sync date to cache.error: %v", err)
		}
	}
	return
}

func main() {
	var (
		ctx  shared.Ctx
		jira DSJira
	)
	jira.createStructuredLogger()
	err := jira.Init(&ctx)
	if err != nil {
		jira.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("Error Init jira: %+v", err)
		return
	}
	jira.log = jira.log.WithFields(logrus.Fields{"endpoint": jira.URL})
	// Update status to in progress in log clusterx
	timestamp := time.Now()
	shared.SetSyncMode(true, false)
	shared.SetLogLoggerError(false)
	shared.AddLogger(&jira.Logger, JiraDataSource, logger.Internal, []map[string]string{{"JIRA_URL": jira.URL, "JIRA_PROJECT": ctx.Project, "ProjectSlug": ctx.Project}})
	jira.AddCacheProvider()
	projects, err := jira.getProjects()
	if err != nil {
		return
	}
	if os.Getenv("SPAN") != "" {
		tracer.Start(tracer.WithGlobalTag("connector", "jira"))
		defer tracer.Stop()

		sb := os.Getenv("SPAN")
		carrier := make(tracer.TextMapCarrier)
		err = jsoniter.Unmarshal([]byte(sb), &carrier)
		if err != nil {
			return
		}
		sctx, er := tracer.Extract(carrier)
		if er != nil {
			fmt.Println(er)
		}
		if err == nil && sctx != nil {
			span, _ := tracer.StartSpanFromContext(context.Background(), "issue", tracer.ResourceName("connector"), tracer.ChildOf(sctx))
			defer span.Finish()
		}
	}
	err = jira.WriteLog(&ctx, timestamp, logger.InProgress, "jira connector started")
	if err != nil {
		jira.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("WriteLog Error : %+v", err)
		return
	}
	for _, p := range projects {
		ctx.Project = p.ID
		ctx.ProjectFilter = true
		jira.endpoint = strings.ReplaceAll(strings.TrimPrefix(strings.TrimPrefix(jira.URL, "https://"), "http://"), "/", "-") + "/" + p.Key
		jira.log = jira.log.WithFields(logrus.Fields{"project": p.Key})
		err = jira.Sync(&ctx)
		if err != nil {
			jira.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("Error Sync jira: %+v", err)
			// Update status to failed in log cluster
			er := jira.WriteLog(&ctx, timestamp, logger.Failed, err.Error())
			if er != nil {
				jira.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("WriteLog Error : %+v", err)
				shared.FatalOnError(er)
			}
		}
		shared.FatalOnError(err)
	}
	// Update status to done in log cluster
	err = jira.WriteLog(&ctx, timestamp, logger.Done, "")
	if err != nil {
		jira.log.WithFields(logrus.Fields{"operation": "main"}).Errorf("WriteLog Error : %+v", err)
	}
	shared.FatalOnError(err)
}

// createStructuredLogger...
func (j *DSJira) createStructuredLogger() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	log := logrus.WithFields(
		logrus.Fields{
			"environment": os.Getenv("STAGE"),
			"commit":      build.GitCommit,
			"version":     build.Version,
			"service":     build.AppName,
			"endpoint":    j.URL,
		})
	j.log = log
}

// AddCacheProvider - adds cache provider
func (j *DSJira) AddCacheProvider() {
	cacheProvider := cache.NewManager(fmt.Sprintf("v2/%s", JiraDataSource), os.Getenv("STAGE"))
	j.cacheProvider = *cacheProvider
}

func (j *DSJira) getProjects() ([]project, error) {
	httpClient := http.NewClientProvider(time.Second*60, false)
	url := j.URL + JiraAPIRoot + "/project"
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}

	statusCode, res, err := httpClient.Request(url, "GET", headers, nil, nil)
	if err != nil {
		return []project{}, err
	}
	if statusCode > 201 {
		return []project{}, fmt.Errorf("error getting projects, status code: %d", statusCode)
	}

	var projectsRes []project
	err = json.Unmarshal(res, &projectsRes)
	if err != nil {
		return projectsRes, err
	}
	return projectsRes, nil
}

func (j *DSJira) getCachedIssues() {
	comB, err := j.cacheProvider.GetFileByKey(j.endpoint, IssuesCacheFile)
	if err != nil {
		return
	}
	reader := csv.NewReader(bytes.NewBuffer(comB))
	records, err := reader.ReadAll()
	if err != nil {
		return
	}
	for i, record := range records {
		if i == 0 {
			continue
		}
		orphaned, err := strconv.ParseBool(record[5])
		if err != nil {
			orphaned = false
		}

		cachedIssues[record[1]] = EntityCache{
			Timestamp:      record[0],
			EntityID:       record[1],
			SourceEntityID: record[2],
			FileLocation:   record[3],
			Hash:           record[4],
			Orphaned:       orphaned,
		}
	}
}

func (j *DSJira) getCachedComments() error {
	commentsB, err := j.cacheProvider.GetFileByKey(j.endpoint, commentsCacheFile)
	records := make(map[string][]EntityCache)
	if commentsB != nil {
		if err = json.Unmarshal(commentsB, &records); err != nil {
			return err
		}
	}
	for key, val := range records {
		cachedComments[key] = val
	}
	return nil
}

func (j *DSJira) createCacheFile(cache []EntityCache, path string) error {
	for _, comm := range cache {
		comm.FileLocation = path
		cachedIssues[comm.EntityID] = comm
	}
	records := [][]string{
		{"timestamp", "entity_id", "source_entity_id", "file_location", "hash", "orphaned"},
	}
	for _, c := range cachedIssues {
		records = append(records, []string{c.Timestamp, c.EntityID, c.SourceEntityID, c.FileLocation, c.Hash, strconv.FormatBool(c.Orphaned)})
	}

	csvFile, err := os.Create(IssuesCacheFile)
	if err != nil {
		return err
	}

	w := csv.NewWriter(csvFile)
	err = w.WriteAll(records)
	if err != nil {
		return err
	}
	err = csvFile.Close()
	if err != nil {
		return err
	}
	file, err := os.ReadFile(IssuesCacheFile)
	if err != nil {
		return err
	}
	err = os.Remove(IssuesCacheFile)
	if err != nil {
		return err
	}
	err = j.cacheProvider.UpdateFileByKey(j.endpoint, IssuesCacheFile, file)
	if err != nil {
		return err
	}

	return nil
}

func isKeyCreated(id string) bool {
	c, ok := cachedIssues[id]
	if ok {
		cachedIssues[id] = c
		return true
	}
	return false
}

// EntityCache single commit cache schema
type EntityCache struct {
	Timestamp      string `json:"timestamp"`
	EntityID       string `json:"entity_id"`
	SourceEntityID string `json:"source_entity_id"`
	FileLocation   string `json:"file_location"`
	Hash           string `json:"hash"`
	Orphaned       bool   `json:"orphaned"`
}

type project struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// IssueComments ...
type IssueComments struct {
	Comments []IssueComment
}

// IssueComment ...
type IssueComment struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}
