package neontestdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

type Client struct {
	Client    http.Client
	Key       string
	ProjectID string
	Branch    string
	NoCleanup bool
}

var defaultBranch = "main"

func SetDefaultBranch(branch string) {
	defaultBranch = branch
}

func LoadClient() Client {
	require := func(key string) string {
		value := os.Getenv(key)
		if value == "" {
			log.Fatalf("missing required environment variable: %s", key)
		}
		return value
	}
	return Client{
		Client:    http.Client{},
		Key:       require("NEON_API_KEY"),
		ProjectID: require("NEON_PROJECT_ID"),
		Branch:    defaultBranch,
	}
}

func (n Client) UsingTestBranch(t *testing.T, do func(ConnectionURI)) {
	hostname, _ := os.Hostname()
	branch := strings.ReplaceAll(fmt.Sprintf("%s.%s", hostname, t.Name()), "/", ".")
	n.UsingBranch(branch, do)
}

func (n Client) UsingBranch(name string, do func(ConnectionURI)) {
	created := n.ForcedCreateBranch(name)
	do(created.ConnectionURIs[0])
	if !n.NoCleanup {
		n.DeleteBranch(created.Branch.ID)
	}
}

func (n Client) GetBranches() *Branches {
	resp := n.Do(n.NewRequest("GET", n.BranchesURL(), nil))
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	validateStatus(resp, http.StatusOK)

	return parseResponse[Branches](resp)
}

func (n Client) ForcedCreateBranch(name string) *BranchCreated {
	if branch := n.GetBranch(name); branch != nil {
		n.DeleteBranch(name)
	}
	return n.CreateBranch(name)
}

func (n Client) GetBranch(name string) *Branch {
	resp := n.Do(n.NewRequest("GET", n.BranchURL(name), nil))
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	validateStatus(resp, http.StatusOK)

	return parseResponse[Branch](resp)
}

func (n Client) GetBranchByName(name string) *Branch {
	for _, branch := range n.GetBranches().Branches {
		if branch.Name == name {
			return &branch
		}
	}
	return nil
}

func (n Client) CreateBranch(name string) *BranchCreated {
	parent := n.GetBranchByName(n.Branch)
	if parent == nil {
		log.Fatalf("error creating branch %s, parent branch '%s' not found", name, n.Branch)
	}

	start := time.Now()
	for retry := 10 * time.Millisecond; retry <= 100*time.Millisecond; retry += 10 {
		create := CreateBranch{
			Name:     name,
			ParentID: parent.ID,
		}
		resp := n.Do(n.NewCreateBranchRequest(create))
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusLocked {
			time.Sleep(retry)
			continue
		}

		validateStatus(resp, http.StatusOK, http.StatusCreated)

		return parseResponse[BranchCreated](resp)
	}
	log.Fatalf("failed to create branch after: %v", time.Since(start))
	return nil
}

func (n Client) NewCreateBranchRequest(branch CreateBranch) *http.Request {
	create := CreateBranchRequest{
		Endpoints: []CreateEndpoint{
			{Type: "read_write"},
		},
		Branch: branch,
	}
	body, err := json.Marshal(create)
	if err != nil {
		log.Fatalf("error marshaling request for %v %v", branch, err)
	}
	req := n.NewRequest("POST", n.BranchesURL(), bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	return req
}

func (n Client) BranchesURL() string {
	return fmt.Sprintf("https://console.neon.tech/api/v2/projects/%s/branches", n.ProjectID)
}

func (n Client) DeleteBranch(name string) {
	resp := n.Do(n.NewRequest("DELETE", n.BranchURL(name), nil))
	defer resp.Body.Close()

	validateStatus(resp, http.StatusOK)
}

func (n Client) BranchURL(branch string) string {
	return fmt.Sprintf("https://console.neon.tech/api/v2/projects/%s/branches/%s", n.ProjectID, branch)
}

func (n Client) NewRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Fatalf("error creating request at %s: %v", url, err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+n.Key)
	return req.WithContext(context.Background())
}

func (n Client) Do(req *http.Request) *http.Response {
	resp, err := n.Client.Do(req)
	if err != nil {
		log.Fatalf("%s: http.Client error %v", getCallerName(1), err)
	}
	return resp
}

func parseResponse[T any](resp *http.Response) (result *T) {
	result = new(T)
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		logFatalResponse(resp, "error decoding %T", *result)
	}
	return result
}

func validateStatus(resp *http.Response, acceptedStatus ...int) {
	if !slices.Contains(acceptedStatus, resp.StatusCode) {
		logFatalResponse(resp, "unexpected status code Status: %d", resp.StatusCode)
	}
}

func logFatalResponse(resp *http.Response, format string, a ...any) {
	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)
	msg := fmt.Sprintf(format, a...)
	log.Fatalf("%s: %s Url: %s Body: %s", getCallerName(2), msg, resp.Request.RequestURI, body)
}

func getCallerName(n int) string {
	pc, _, _, _ := runtime.Caller(n + 1)
	return runtime.FuncForPC(pc).Name()
}

type CreateBranchRequest struct {
	Endpoints []CreateEndpoint `json:"endpoints"`
	Branch    CreateBranch     `json:"branch"`
}

type CreateEndpoint struct {
	Type string `json:"type"`
}

type CreateBranch struct {
	Name     string `json:"name"`
	ParentID string `json:"parent_id"`
}

type Branch struct {
	ID                 string    `json:"id"`
	ProjectID          string    `json:"project_id"`
	ParentID           string    `json:"parent_id"`
	ParentLSN          string    `json:"parent_lsn"`
	ParentTimestamp    string    `json:"parent_timestamp"`
	Name               string    `json:"name"`
	CurrentState       string    `json:"current_state"`
	PendingState       string    `json:"pending_state"`
	StateChangedAt     string    `json:"state_changed_at"`
	CreationSource     string    `json:"creation_source"`
	Primary            bool      `json:"primary"`
	Default            bool      `json:"default"`
	Protected          bool      `json:"protected"`
	CPUUsedSec         int       `json:"cpu_used_sec"`
	ComputeTimeSeconds int       `json:"compute_time_seconds"`
	ActiveTimeSeconds  int       `json:"active_time_seconds"`
	WrittenDataBytes   int       `json:"written_data_bytes"`
	DataTransferBytes  int       `json:"data_transfer_bytes"`
	CreatedAt          string    `json:"created_at"`
	UpdatedAt          string    `json:"updated_at"`
	CreatedBy          CreatedBy `json:"created_by"`
}

type CreatedBy struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type Endpoint struct {
	Host                  string                 `json:"host"`
	ID                    string                 `json:"id"`
	ProjectID             string                 `json:"project_id"`
	BranchID              string                 `json:"branch_id"`
	AutoscalingLimitMinCu float64                `json:"autoscaling_limit_min_cu"`
	AutoscalingLimitMaxCu float64                `json:"autoscaling_limit_max_cu"`
	RegionID              string                 `json:"region_id"`
	Type                  string                 `json:"type"`
	CurrentState          string                 `json:"current_state"`
	PendingState          string                 `json:"pending_state"`
	Settings              map[string]interface{} `json:"settings"`
	PoolerEnabled         bool                   `json:"pooler_enabled"`
	PoolerMode            string                 `json:"pooler_mode"`
	Disabled              bool                   `json:"disabled"`
	PasswordlessAccess    bool                   `json:"passwordless_access"`
	CreationSource        string                 `json:"creation_source"`
	CreatedAt             string                 `json:"created_at"`
	UpdatedAt             string                 `json:"updated_at"`
	ProxyHost             string                 `json:"proxy_host"`
	SuspendTimeoutSeconds int                    `json:"suspend_timeout_seconds"`
	Provisioner           string                 `json:"provisioner"`
}

type Operation struct {
	ID              string `json:"id"`
	ProjectID       string `json:"project_id"`
	BranchID        string `json:"branch_id"`
	EndpointID      string `json:"endpoint_id,omitempty"`
	Action          string `json:"action"`
	Status          string `json:"status"`
	FailuresCount   int    `json:"failures_count"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
	TotalDurationMs int    `json:"total_duration_ms"`
}

type Role struct {
	BranchID  string `json:"branch_id"`
	Name      string `json:"name"`
	Protected bool   `json:"protected"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type Database struct {
	ID        int    `json:"id"`
	BranchID  string `json:"branch_id"`
	Name      string `json:"name"`
	OwnerName string `json:"owner_name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type ConnectionParameters struct {
	Database   string `json:"database"`
	Password   string `json:"password"`
	Role       string `json:"role"`
	Host       string `json:"host"`
	PoolerHost string `json:"pooler_host"`
}

type ConnectionURI struct {
	ConnectionURI        string               `json:"connection_uri"`
	ConnectionParameters ConnectionParameters `json:"connection_parameters"`
}

type BranchCreated struct {
	Branch         Branch          `json:"branch"`
	Endpoints      []Endpoint      `json:"endpoints"`
	Operations     []Operation     `json:"operations"`
	Roles          []Role          `json:"roles"`
	Databases      []Database      `json:"databases"`
	ConnectionURIs []ConnectionURI `json:"connection_uris"`
}

type Branches struct {
	Branches    []Branch               `json:"branches"`
	Annotations map[string]interface{} `json:"annotations"`
}
