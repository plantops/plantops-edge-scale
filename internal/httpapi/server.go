package httpapi

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/auditjournal"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/ingress"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/rawjournal"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/scaleascii"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/engine"
)

//go:embed web/*
var webFS embed.FS

type Workflow interface {
	Snapshot() engine.Snapshot
	ObservePosition(context.Context, domain.PositionSnapshot) error
	ObserveFault(context.Context, domain.Fault) error
	ObserveCamera(context.Context, domain.CameraEvidence) error
	AuthorizeOverride(context.Context, domain.Override) error
	ResetCompleted() error
}

type CycleQueue interface {
	Queue(context.Context) ([]domain.WeighCycle, error)
	Call(context.Context, string) error
}

type Server struct {
	RFID            *ingress.RFID
	LPR             *ingress.LPR
	WeightAudit     *rawjournal.Journal
	EventAudit      *auditjournal.Journal
	ScaleMonitor    *scaleascii.Monitor
	Workflow        Workflow
	Cycles          CycleQueue
	CameraIDs       map[string]bool
	IOStatus        func() any
	CentralStatus   func() any
	StorageStatus   func(context.Context) (any,error)
	LatestTicket    func(context.Context) (domain.Ticket,bool,error)
	RecentTickets   func(context.Context,int) ([]domain.Ticket,error)
	Vehicles        func() any
	AllowSimulation bool
	// Hardware reports which components are real and which are simulated.
	Hardware func() any
	// Bench controls exist only for components configured with adapter "fake".
	FakeIOState    func() any
	FakeIOPatch    func(json.RawMessage) error
	FakeScaleState func() any
	FakeScalePatch func(json.RawMessage) error
	Version         string
	GitSHA          string
}

func (s *Server) Handler() http.Handler {
	mux:=http.NewServeMux()
	mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){
		payload:=map[string]any{"status":"ok","service":"plantops-edge-scale-go","version":s.Version,"git_sha":s.GitSHA,"utc":time.Now().UTC(),"event_audit_configured":s.EventAudit!=nil,"two_pass_cycle":s.Cycles!=nil}
		if s.ScaleMonitor!=nil{payload["scale"]=s.ScaleMonitor.Snapshot()}
		if s.Workflow!=nil{payload["workflow"]=s.Workflow.Snapshot()}
		if s.IOStatus!=nil{payload["io"]=s.IOStatus()}
		if s.CentralStatus!=nil{payload["central"]=s.CentralStatus()}
		if s.StorageStatus!=nil{if st,err:=s.StorageStatus(r.Context());err==nil{payload["storage"]=st}else{payload["status"]="degraded";payload["storage_error"]=err.Error()}}
		writeJSON(w,http.StatusOK,payload)
	})
	mux.HandleFunc("GET /api/workflow",func(w http.ResponseWriter,_ *http.Request){if s.Workflow==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"workflow engine not configured"});return};writeJSON(w,http.StatusOK,s.Workflow.Snapshot())})
	mux.HandleFunc("GET /api/io/status",func(w http.ResponseWriter,_ *http.Request){if s.IOStatus==nil{writeJSON(w,http.StatusOK,map[string]any{"enabled":false});return};writeJSON(w,http.StatusOK,s.IOStatus())})
	mux.HandleFunc("GET /api/central/status",func(w http.ResponseWriter,_ *http.Request){if s.CentralStatus==nil{writeJSON(w,http.StatusOK,map[string]any{"enabled":false});return};writeJSON(w,http.StatusOK,s.CentralStatus())})
	mux.HandleFunc("GET /api/scale/status",func(w http.ResponseWriter,_ *http.Request){if s.ScaleMonitor==nil{writeJSON(w,http.StatusOK,map[string]any{"enabled":false});return};writeJSON(w,http.StatusOK,s.ScaleMonitor.Snapshot())})
	mux.HandleFunc("GET /api/storage/status",func(w http.ResponseWriter,r *http.Request){if s.StorageStatus==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"storage not configured"});return};st,err:=s.StorageStatus(r.Context());if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusOK,st)})
	mux.HandleFunc("GET /api/tickets/latest",func(w http.ResponseWriter,r *http.Request){if s.LatestTicket==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"ticket reader not configured"});return};t,ok,err:=s.LatestTicket(r.Context());if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};if !ok{writeJSON(w,http.StatusNotFound,map[string]string{"error":"no completed ticket"});return};writeJSON(w,http.StatusOK,t)})

	mux.HandleFunc("GET /api/tickets/recent",func(w http.ResponseWriter,r *http.Request){if s.RecentTickets==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"ticket reader not configured"});return};ts,err:=s.RecentTickets(r.Context(),parseLimit(r,20));if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};if ts==nil{ts=[]domain.Ticket{}};writeJSON(w,http.StatusOK,map[string]any{"count":len(ts),"tickets":ts})})
	mux.HandleFunc("GET /api/vehicles",func(w http.ResponseWriter,_ *http.Request){if s.Vehicles==nil{writeJSON(w,http.StatusOK,map[string]any{"vehicles":[]any{}});return};writeJSON(w,http.StatusOK,map[string]any{"vehicles":s.Vehicles()})})
	mux.HandleFunc("GET /api/queue",func(w http.ResponseWriter,r *http.Request){if s.Cycles==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"two-pass cycle coordinator not configured"});return};items,err:=s.Cycles.Queue(r.Context());if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusOK,map[string]any{"count":len(items),"items":items})})
	mux.HandleFunc("POST /api/queue/{cycleID}/call",func(w http.ResponseWriter,r *http.Request){if s.Cycles==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"two-pass cycle coordinator not configured"});return};id:=r.PathValue("cycleID");if id==""{writeJSON(w,http.StatusBadRequest,map[string]string{"error":"cycle id required"});return};if err:=s.Cycles.Call(r.Context(),id);err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};items,err:=s.Cycles.Queue(r.Context());if err!=nil{writeJSON(w,http.StatusAccepted,map[string]any{"status":"called","cycle_id":id});return};writeJSON(w,http.StatusAccepted,map[string]any{"status":"called","cycle_id":id,"count":len(items),"items":items})})

	mux.HandleFunc("GET /api/audit/weights",func(w http.ResponseWriter,r *http.Request){if s.WeightAudit==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"raw weight audit not configured"});return};records,err:=s.WeightAudit.Tail(parseLimit(r,200));if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusOK,map[string]any{"count":len(records),"records":records})})
	mux.HandleFunc("GET /api/audit/weights/verify",func(w http.ResponseWriter,_ *http.Request){if s.WeightAudit==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"raw weight audit not configured"});return};writeVerify(w,s.WeightAudit.Verify())})
	mux.HandleFunc("GET /api/audit/events",func(w http.ResponseWriter,r *http.Request){if s.EventAudit==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"operational event audit not configured"});return};records,err:=s.EventAudit.Tail(parseLimit(r,200));if err!=nil{writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusOK,map[string]any{"count":len(records),"records":records})})
	mux.HandleFunc("GET /api/audit/events/verify",func(w http.ResponseWriter,_ *http.Request){if s.EventAudit==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"operational event audit not configured"});return};writeVerify(w,s.EventAudit.Verify())})

	mux.HandleFunc("GET /api/identity",func(w http.ResponseWriter,_ *http.Request){writeJSON(w,http.StatusOK,map[string]any{"rfid":s.RFID.Latest(),"lpr":s.LPR.Latest()})})
	mux.HandleFunc("POST /io/rfid",func(w http.ResponseWriter,r *http.Request){var in struct{Tag string `json:"tag"`;Quality float64 `json:"quality"`};if json.NewDecoder(r.Body).Decode(&in)!=nil||in.Tag==""{writeJSON(w,http.StatusBadRequest,map[string]string{"error":"tag required"});return};s.RFID.Ingest(in.Tag,in.Quality);writeJSON(w,http.StatusAccepted,map[string]string{"status":"accepted"})})
	mux.HandleFunc("POST /io/lpr",func(w http.ResponseWriter,r *http.Request){
		var in struct{Plate string `json:"plate"`;Confidence float64 `json:"confidence"`;ImageRef string `json:"image_ref"`;CameraID string `json:"camera_id"`}
		if json.NewDecoder(r.Body).Decode(&in)!=nil||in.Plate==""{writeJSON(w,http.StatusBadRequest,map[string]string{"error":"plate required"});return}
		s.LPR.Ingest(in.Plate,in.Confidence,in.ImageRef)
		linked:=false
		if in.ImageRef!=""&&s.Workflow!=nil{
			cameraID:=strings.ToUpper(strings.TrimSpace(in.CameraID));if cameraID==""{cameraID=directionalLPRCamera(s.Workflow.Snapshot())}
			if cameraID!=""&&s.cameraAllowed(cameraID){e:=domain.CameraEvidence{CameraID:cameraID,Role:"LPR",ImageRef:in.ImageRef,CapturedAt:time.Now().UTC()};if err:=s.Workflow.ObserveCamera(r.Context(),e);err==nil{linked=true}}
		}
		writeJSON(w,http.StatusAccepted,map[string]any{"status":"accepted","evidence_linked":linked})
	})
	mux.HandleFunc("POST /io/camera/{cameraID}",func(w http.ResponseWriter,r *http.Request){
		if s.Workflow==nil{writeJSON(w,http.StatusServiceUnavailable,map[string]string{"error":"workflow engine not configured"});return}
		id:=strings.ToUpper(strings.TrimSpace(r.PathValue("cameraID")));if id==""||!s.cameraAllowed(id){writeJSON(w,http.StatusBadRequest,map[string]string{"error":"camera id not configured"});return}
		var in struct{Role string `json:"role"`;ImageRef string `json:"image_ref"`;CapturedAt time.Time `json:"captured_at"`};if err:=json.NewDecoder(r.Body).Decode(&in);err!=nil||in.ImageRef==""{writeJSON(w,http.StatusBadRequest,map[string]string{"error":"image_ref required"});return};if in.CapturedAt.IsZero(){in.CapturedAt=time.Now().UTC()};if in.Role==""{in.Role="OVERVIEW"}
		e:=domain.CameraEvidence{CameraID:id,Role:in.Role,ImageRef:in.ImageRef,CapturedAt:in.CapturedAt};if err:=s.Workflow.ObserveCamera(r.Context(),e);err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusAccepted,map[string]any{"status":"accepted","evidence":e})
	})

	mux.HandleFunc("GET /api/hardware",func(w http.ResponseWriter,_ *http.Request){if s.Hardware==nil{writeJSON(w,http.StatusOK,map[string]any{"mode":"UNKNOWN"});return};writeJSON(w,http.StatusOK,s.Hardware())})
	bench:=func(path string,state func()any,patch func(json.RawMessage)error){
		if state==nil{return}
		mux.HandleFunc("GET "+path,func(w http.ResponseWriter,_ *http.Request){writeJSON(w,http.StatusOK,state())})
		if patch!=nil{mux.HandleFunc("POST "+path,func(w http.ResponseWriter,r *http.Request){var raw json.RawMessage;if err:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1<<16)).Decode(&raw);err!=nil{writeJSON(w,http.StatusBadRequest,map[string]string{"error":err.Error()});return};if err:=patch(raw);err!=nil{writeJSON(w,http.StatusBadRequest,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusAccepted,state())})}
	}
	bench("/api/fake/io",s.FakeIOState,s.FakeIOPatch)
	bench("/api/fake/scale",s.FakeScaleState,s.FakeScalePatch)

	if s.AllowSimulation&&s.Workflow!=nil{
		mux.HandleFunc("POST /sim/position",func(w http.ResponseWriter,r *http.Request){var p domain.PositionSnapshot;if err:=json.NewDecoder(r.Body).Decode(&p);err!=nil{writeJSON(w,http.StatusBadRequest,map[string]string{"error":err.Error()});return};if p.Observed.IsZero(){p.Observed=time.Now().UTC()};if err:=s.Workflow.ObservePosition(r.Context(),p);err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusAccepted,s.Workflow.Snapshot())})
		mux.HandleFunc("POST /sim/fault",func(w http.ResponseWriter,r *http.Request){var f domain.Fault;if err:=json.NewDecoder(r.Body).Decode(&f);err!=nil||f.Device==""{writeJSON(w,http.StatusBadRequest,map[string]string{"error":"valid device fault required"});return};if err:=s.Workflow.ObserveFault(r.Context(),f);err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusAccepted,s.Workflow.Snapshot())})
		mux.HandleFunc("POST /sim/override",func(w http.ResponseWriter,r *http.Request){var o domain.Override;if err:=json.NewDecoder(r.Body).Decode(&o);err!=nil{writeJSON(w,http.StatusBadRequest,map[string]string{"error":err.Error()});return};if err:=s.Workflow.AuthorizeOverride(r.Context(),o);err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusAccepted,s.Workflow.Snapshot())})
		mux.HandleFunc("POST /sim/reset-complete",func(w http.ResponseWriter,_ *http.Request){if err:=s.Workflow.ResetCompleted();err!=nil{writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()});return};writeJSON(w,http.StatusOK,s.Workflow.Snapshot())})
	}
	static,_:=fs.Sub(webFS,"web");mux.Handle("/",http.FileServer(http.FS(static)));return mux
}

func (s *Server) cameraAllowed(id string)bool{if len(s.CameraIDs)==0{return id=="C1A"||id=="C1B"||id=="C3"};return s.CameraIDs[id]}
func directionalLPRCamera(snap engine.Snapshot)string{if snap.Transaction==nil{return ""};if snap.Transaction.Direction==domain.DirectionBToA{return "C1B"};if snap.Transaction.Direction==domain.DirectionAToB{return "C1A"};return ""}
func parseLimit(r *http.Request,fallback int)int{limit:=fallback;if raw:=r.URL.Query().Get("limit");raw!=""{if n,err:=strconv.Atoi(raw);err==nil{limit=n}};return limit}
func writeVerify(w http.ResponseWriter,err error){if err==nil{writeJSON(w,http.StatusOK,map[string]any{"status":"ok","verified":true});return};if os.IsNotExist(err){writeJSON(w,http.StatusOK,map[string]any{"status":"empty","verified":true});return};writeJSON(w,http.StatusConflict,map[string]any{"status":"invalid","verified":false,"error":err.Error()})}
func writeJSON(w http.ResponseWriter,status int,v any){w.Header().Set("Content-Type","application/json");w.WriteHeader(status);_ = json.NewEncoder(w).Encode(v)}
