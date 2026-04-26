package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

// Response types for type-safe API responses.
type UploadResp struct {
	Ok            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	FileID        string `json:"file_id"`
	FileName      string `json:"file_name"`
	Size          int64  `json:"size"`
	Chunks        int    `json:"chunks"`
	Replicas      int    `json:"replicas"`
	EncryptionKey string `json:"encryption_key"`
}

type ListResp struct {
	Ok    bool       `json:"ok"`
	Error string     `json:"error,omitempty"`
	Files []FileInfo `json:"files"`
	Count int        `json:"count"`
}

type FileInfo struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Size      int64       `json:"size"`
	ChunkSize int64       `json:"chunk_size"`
	Chunks    []ChunkInfo `json:"chunks"`
	CreatedAt string      `json:"created_at"`
}

type ChunkInfo struct {
	ID    string   `json:"id"`
	Index int      `json:"index"`
	Size  int64    `json:"size"`
	Nodes []string `json:"nodes"`
}

type FileInfoResp struct {
	Ok    bool     `json:"ok"`
	Error string   `json:"error,omitempty"`
	File  FileInfo `json:"file"`
}

type NodeInfo struct {
	ID          int    `json:"id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	LogLen      int    `json:"log_length"`
	CommitIndex int    `json:"commit_index"`
	Alive       bool   `json:"alive"`
	IsLeader    bool   `json:"is_leader"`
	ChunkCount  int    `json:"chunk_count"`
	DiskUsage   int64  `json:"disk_usage"`
}

type HealthResp struct {
	Status    string `json:"status"`
	Leader    int    `json:"leader"`
	FileCount int    `json:"file_count"`
}

type DeleteResp struct {
	Ok      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Deleted string `json:"deleted"`
}

// Client communicates with the Bunkr API server.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a Bunkr API client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

// Upload sends a file to the server.
func (c *Client) Upload(filePath string) (*UploadResp, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("file", filePath)
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}
	writer.Close()

	req, err := http.NewRequest("POST", c.baseURL+"/api/files/upload", body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}
	defer resp.Body.Close()

	var result UploadResp
	json.NewDecoder(resp.Body).Decode(&result)
	return &result, nil
}

// Download fetches a file from the server.
func (c *Client) Download(nameOrID, encKey string) ([]byte, string, error) {
	url := fmt.Sprintf("%s/api/files/download?name=%s", c.baseURL, nameOrID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("X-Encryption-Key", encKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("download failed: %s", string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	// Extract filename from Content-Disposition if available
	filename := nameOrID
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		fmt.Sscanf(cd, "attachment; filename=%q", &filename)
	}

	return data, filename, nil
}

// ListFiles returns all files.
func (c *Client) ListFiles() (*ListResp, error) {
	var result ListResp
	if err := c.getJSON("/api/files/list", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteFile removes a file.
func (c *Client) DeleteFile(nameOrID string) (*DeleteResp, error) {
	var result DeleteResp
	if err := c.postJSON("/api/files/delete", map[string]string{"name": nameOrID}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFileInfo gets file details.
func (c *Client) GetFileInfo(nameOrID string) (*FileInfoResp, error) {
	var result FileInfoResp
	if err := c.getJSON(fmt.Sprintf("/api/files/info?name=%s", nameOrID), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Nodes returns cluster status.
func (c *Client) Nodes() ([]NodeInfo, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/api/cluster/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result []NodeInfo
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

// Health returns cluster health.
func (c *Client) Health() (*HealthResp, error) {
	var result HealthResp
	if err := c.getJSON("/api/health", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) getJSON(path string, out interface{}) error {
	resp, err := c.httpClient.Get(c.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) postJSON(path string, data, out interface{}) error {
	body, _ := json.Marshal(data)
	resp, err := c.httpClient.Post(c.baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}
