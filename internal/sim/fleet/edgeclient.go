package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// EdgeClient is the sim's only view of the edge service: the same HTTP surface a
// real RFID reader, LPR camera and dispatcher UI would use.
type EdgeClient struct {
	Base string
	HTTP *http.Client
}

func NewEdgeClient(base string) *EdgeClient {
	return &EdgeClient{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func (c *EdgeClient) do(ctx context.Context, method, path string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: bad JSON: %w", method, path, err)
		}
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return resp.StatusCode, nil
}

func (c *EdgeClient) PostRFID(ctx context.Context, tag string, quality float64) error {
	_, err := c.do(ctx, http.MethodPost, "/io/rfid", map[string]any{"tag": tag, "quality": quality}, nil)
	return err
}

func (c *EdgeClient) PostLPR(ctx context.Context, plate string, confidence float64, imageRef string) error {
	_, err := c.do(ctx, http.MethodPost, "/io/lpr", map[string]any{"plate": plate, "confidence": confidence, "image_ref": imageRef}, nil)
	return err
}

// WorkflowView is the slice of /api/workflow the sim needs.
type WorkflowView struct {
	State       string `json:"state"`
	Mode        string `json:"mode"`
	Transaction *struct {
		ID              string `json:"id"`
		State           string `json:"state"`
		Direction       string `json:"direction"`
		Identity        string `json:"identity"`
		LastBlockReason string `json:"last_block_reason"`
	} `json:"transaction"`
}

func (c *EdgeClient) Workflow(ctx context.Context) (WorkflowView, error) {
	var v WorkflowView
	_, err := c.do(ctx, http.MethodGet, "/api/workflow", nil, &v)
	return v, err
}

type QueueItem struct {
	ID     string `json:"id"`
	Plate  string `json:"plate"`
	RFID   string `json:"rfid"`
	Status string `json:"status"`
}

func (c *EdgeClient) Queue(ctx context.Context) ([]QueueItem, error) {
	var out struct {
		Items []QueueItem `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/queue", nil, &out)
	return out.Items, err
}

func (c *EdgeClient) Call(ctx context.Context, cycleID string) error {
	_, err := c.do(ctx, http.MethodPost, "/api/queue/"+cycleID+"/call", nil, nil)
	return err
}

func (c *EdgeClient) Healthy(ctx context.Context) bool {
	_, err := c.do(ctx, http.MethodGet, "/healthz", nil, nil)
	return err == nil
}

// Verify asks the edge to verify both hash-chained audit journals.
func (c *EdgeClient) Verify(ctx context.Context) (weights, events bool, err error) {
	var w, e struct {
		Verified bool `json:"verified"`
	}
	if _, err = c.do(ctx, http.MethodGet, "/api/audit/weights/verify", nil, &w); err != nil {
		return
	}
	if _, err = c.do(ctx, http.MethodGet, "/api/audit/events/verify", nil, &e); err != nil {
		return
	}
	return w.Verified, e.Verified, nil
}
