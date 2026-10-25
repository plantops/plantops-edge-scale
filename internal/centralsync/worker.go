package centralsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/sqlitestore"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/ports"
)

type Store interface {
	PendingSync(context.Context, int) ([]sqlitestore.SyncItem, error)
	MarkSyncAttempt(context.Context, int64, string, time.Time) error
	AckSync(context.Context, int64) error
}

type Config struct {
	TicketURL string
	HeartbeatURL string
	BearerToken string
	StationID string
	Version string
	GitSHA string
	Interval time.Duration
	HeartbeatEvery time.Duration
	Batch int
	Timeout time.Duration

	// BlockReason, when set, stops ticket delivery: tickets stay pending and are
	// not attempted. Used when part of the site is simulated, so a bench ticket
	// can never be delivered to Central as production data.
	BlockReason string
	// Hardware is added to every heartbeat so Central can see what is simulated.
	Hardware map[string]any
}

type Status struct {
	Enabled bool `json:"enabled"`
	TicketSyncEnabled bool `json:"ticket_sync_enabled"`
	HeartbeatEnabled bool `json:"heartbeat_enabled"`
	LastTicketAttemptAt time.Time `json:"last_ticket_attempt_at,omitempty"`
	LastTicketAckAt time.Time `json:"last_ticket_ack_at,omitempty"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitempty"`
	LastError string `json:"last_error,omitempty"`
	TicketSyncBlocked bool `json:"ticket_sync_blocked,omitempty"`
	BlockReason string `json:"block_reason,omitempty"`
}

type Worker struct {
	Store Store
	Audit ports.AuditStore
	Client *http.Client
	Config Config
	mu sync.RWMutex
	status Status
}

func (w *Worker) Snapshot() Status {
	w.mu.RLock();defer w.mu.RUnlock()
	s:=w.status;s.TicketSyncBlocked=w.Config.TicketURL!=""&&w.Config.BlockReason!="";s.BlockReason=w.Config.BlockReason;s.Enabled=w.Config.TicketURL!=""||w.Config.HeartbeatURL!="";s.TicketSyncEnabled=w.Config.TicketURL!="";s.HeartbeatEnabled=w.Config.HeartbeatURL!="";return s
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Store==nil{return errors.New("central sync store is nil")}
	if w.Config.TicketURL==""&&w.Config.HeartbeatURL==""{return nil}
	interval:=w.Config.Interval;if interval<=0{interval=5*time.Second};heartbeatEvery:=w.Config.HeartbeatEvery;if heartbeatEvery<=0{heartbeatEvery=30*time.Second}
	tick:=time.NewTicker(interval);defer tick.Stop();hb:=time.NewTicker(heartbeatEvery);defer hb.Stop()
	_ = w.syncOnce(ctx);_ = w.heartbeat(ctx)
	for{select{case <-ctx.Done():return nil;case <-tick.C:_=w.syncOnce(ctx);case <-hb.C:_=w.heartbeat(ctx)}}
}

func (w *Worker) syncOnce(ctx context.Context) error {
	if w.Config.TicketURL==""{return nil}
	if w.Config.BlockReason!=""{return nil}
	batch:=w.Config.Batch;if batch<=0||batch>100{batch=20}
	items,err:=w.Store.PendingSync(ctx,batch);if err!=nil{w.fail(err);return err}
	for _,item:=range items{
		if item.Kind!="TICKET"{continue}
		w.mu.Lock();w.status.LastTicketAttemptAt=time.Now().UTC();w.mu.Unlock()
		_ = w.audit(ctx,domain.AuditCentralSyncAttempt,item.EntityID,"attempt",map[string]any{"queue_id":item.ID,"attempt":item.AttemptCount+1})
		status,sendErr:=w.post(ctx,w.Config.TicketURL,[]byte(item.PayloadJSON))
		if sendErr!=nil||status<200||status>=300{
			msg:="";if sendErr!=nil{msg=sendErr.Error()}else{msg=fmt.Sprintf("HTTP %d",status)}
			next:=time.Now().UTC().Add(backoff(item.AttemptCount+1));_ = w.Store.MarkSyncAttempt(ctx,item.ID,msg,next);w.fail(errors.New(msg));continue
		}
		if err:=w.Store.AckSync(ctx,item.ID);err!=nil{w.fail(err);return err}
		now:=time.Now().UTC();w.mu.Lock();w.status.LastTicketAckAt=now;w.status.LastError="";w.mu.Unlock()
		_ = w.audit(ctx,domain.AuditCentralSyncAck,item.EntityID,"acked",map[string]any{"queue_id":item.ID,"http_status":status})
	}
	return nil
}

func (w *Worker) heartbeat(ctx context.Context) error {
	if w.Config.HeartbeatURL==""{return nil}
	payload,_:=json.Marshal(map[string]any{"station_id":w.Config.StationID,"version":w.Config.Version,"git_sha":w.Config.GitSHA,"at_utc":time.Now().UTC(),"hardware":w.Config.Hardware})
	status,err:=w.post(ctx,w.Config.HeartbeatURL,payload)
	if err!=nil{w.fail(err);return err};if status<200||status>=300{err=fmt.Errorf("heartbeat HTTP %d",status);w.fail(err);return err}
	now:=time.Now().UTC();w.mu.Lock();w.status.LastHeartbeatAt=now;w.status.LastError="";w.mu.Unlock();_ = w.audit(ctx,domain.AuditHeartbeat,"","sent",map[string]any{"http_status":status});return nil
}

func (w *Worker) post(ctx context.Context,url string,payload []byte)(int,error){
	client:=w.Client;if client==nil{timeout:=w.Config.Timeout;if timeout<=0{timeout=5*time.Second};client=&http.Client{Timeout:timeout}}
	req,err:=http.NewRequestWithContext(ctx,http.MethodPost,url,bytes.NewReader(payload));if err!=nil{return 0,err};req.Header.Set("Content-Type","application/json");if w.Config.BearerToken!=""{req.Header.Set("Authorization","Bearer "+w.Config.BearerToken)}
	resp,err:=client.Do(req);if err!=nil{return 0,err};defer resp.Body.Close();return resp.StatusCode,nil
}
func (w *Worker) audit(ctx context.Context,kind domain.AuditKind,txID,action string,data map[string]any)error{if w.Audit==nil{return nil};_,err:=w.Audit.Append(ctx,domain.AuditEvent{StationID:w.Config.StationID,TransactionID:txID,AtUTC:time.Now().UTC(),Kind:kind,Actor:"SYSTEM",Source:"CENTRAL_SYNC",Action:action,Data:data,RuntimeGitSHA:w.Config.GitSHA});return err}
func (w *Worker) fail(err error){if err==nil{return};w.mu.Lock();w.status.LastErrorAt=time.Now().UTC();w.status.LastError=err.Error();w.mu.Unlock()}
func backoff(attempt int)time.Duration{if attempt<1{attempt=1};d:=time.Duration(1<<min(attempt-1,6))*5*time.Second;if d>5*time.Minute{return 5*time.Minute};return d}
func min(a,b int)int{if a<b{return a};return b}
