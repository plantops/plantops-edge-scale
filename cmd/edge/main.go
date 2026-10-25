package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/auditjournal"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/ingress"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/laumasrtu"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/modbustcp"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/rawjournal"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/registry"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/scaleascii"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/sqlitestore"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/centralsync"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/cycle"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/engine"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/httpapi"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/runtimeio"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/twopass"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/winservice"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/workflowaudit"
)

var (
	version = "dev"
	gitSHA  = "dev"
)

type config struct {
	listen, stationID, dbPath, scaleAddr, rawWeightJournal, eventJournal string
	reconnectDelay time.Duration
	ioAddr string
	ioUnitID uint
	ioMapSpec string
	ioPoll, ioTimeout, buzzerPulse, barrierFeedbackTimeout time.Duration
	vehicleMap, cameraIDs string
	emptyScaleMaxKG, minStableWeightKG int64
	stableConfirmations int
	stableToleranceKG int64
	pairMinElapsed, pairMaxElapsed time.Duration
	pairMinGrossKG, pairMaxGrossKG, pairMinTareKG, pairMaxTareKG int64
	pairMinNetKG, pairMaxNetKG int64
	pairRequireGrossFirst bool
	scaleProtocol, scaleSerial, scaleParity string
	scaleBaud, scaleStopBits int
	scaleSlave uint
	scalePoll, scaleResponseTimeout time.Duration
	scaleMissedPolls int
	laumasStableBit int
	laumasOverloadMask, laumasFaultMask uint
	centralTicketURL, centralHeartbeatURL, centralToken string
	centralSyncInterval, centralHeartbeatInterval, centralTimeout time.Duration
	simulation bool
}

func main() {
	cfg, serviceAction := parseFlags()
	if serviceAction != "" { if err := manageService(serviceAction); err != nil { log.Fatal(err) }; return }
	runner := func(ctx context.Context) error { return run(ctx, cfg) }
	isService, err := winservice.IsService(); if err != nil { log.Fatalf("detect Windows Service context: %v", err) }
	if isService { if err := winservice.Run(runner); err != nil { log.Fatal(err) }; return }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM); defer stop()
	if err := runner(ctx); err != nil { log.Fatal(err) }
}

func parseFlags() (config, string) {
	var c config; var serviceAction string
	flag.StringVar(&c.listen,"listen","127.0.0.1:8080","HTTP listen address")
	flag.StringVar(&c.stationID,"station-id","EDGE-01","stable station identifier used in audit records")
	flag.StringVar(&c.dbPath,"db","data/edge.db","SQLite edge database path")
	flag.StringVar(&c.scaleAddr,"scale-addr","","scale controller TCP address host:port; empty disables live collector")
	flag.StringVar(&c.rawWeightJournal,"raw-weight-journal","data/raw-weight.jsonl","append-only raw weight audit journal path")
	flag.StringVar(&c.eventJournal,"event-journal","data/events.jsonl","append-only operational event audit journal path")
	flag.StringVar(&c.scaleProtocol,"scale-protocol","ascii","scale protocol: ascii (TCP line stream, placeholder) or laumas-rtu (Laumas W100/W200 Modbus RTU poll)")
	flag.StringVar(&c.scaleSerial,"scale-serial","","laumas-rtu: native COM port, e.g. COM3 (Windows). Empty = use -scale-addr as a serial-gateway TCP address")
	flag.IntVar(&c.scaleBaud,"scale-baud",9600,"laumas-rtu serial baud rate")
	flag.StringVar(&c.scaleParity,"scale-parity","N","laumas-rtu serial parity: N, E or O")
	flag.IntVar(&c.scaleStopBits,"scale-stop-bits",1,"laumas-rtu serial stop bits: 1 or 2")
	flag.UintVar(&c.scaleSlave,"scale-slave",1,"laumas-rtu Modbus slave address 1..247")
	flag.DurationVar(&c.scalePoll,"scale-poll",100*time.Millisecond,"laumas-rtu poll interval (every poll is one raw-journal record)")
	flag.DurationVar(&c.scaleResponseTimeout,"scale-response-timeout",300*time.Millisecond,"laumas-rtu per-poll response timeout")
	flag.IntVar(&c.scaleMissedPolls,"scale-missed-polls",3,"laumas-rtu consecutive missed/damaged polls before the scale fault is raised")
	flag.IntVar(&c.laumasStableBit,"laumas-stable-bit",-1,"laumas-rtu status bit meaning STABLE; -1 = verified default (bit 11)")
	flag.UintVar(&c.laumasOverloadMask,"laumas-overload-mask",0,"laumas-rtu status mask meaning OVERLOAD; 0 = not decoded (verify against the manual first)")
	flag.UintVar(&c.laumasFaultMask,"laumas-fault-mask",0,"laumas-rtu status mask meaning indicator FAULT; 0 = not decoded (verify against the manual first)")
	flag.DurationVar(&c.reconnectDelay,"scale-reconnect-delay",2*time.Second,"delay before reconnecting to scale controller")
	flag.StringVar(&c.ioAddr,"io-addr","","Modbus TCP remote I/O host:port; empty disables hardware I/O")
	flag.UintVar(&c.ioUnitID,"io-unit-id",1,"Modbus TCP unit identifier 0..255")
	flag.StringVar(&c.ioMapSpec,"io-map","","comma-separated logical I/O address overrides")
	flag.DurationVar(&c.ioPoll,"io-poll",100*time.Millisecond,"remote I/O input/reconcile interval")
	flag.DurationVar(&c.ioTimeout,"io-timeout",time.Second,"Modbus TCP request timeout")
	flag.DurationVar(&c.buzzerPulse,"io-buzzer-pulse",700*time.Millisecond,"bounded buzzer pulse on release authorization")
	flag.DurationVar(&c.barrierFeedbackTimeout,"barrier-feedback-timeout",5*time.Second,"maximum wait for barrier OPEN feedback")
	flag.StringVar(&c.vehicleMap,"vehicle-map","","bootstrap RFID=PLATE pairs separated by commas")
	flag.StringVar(&c.cameraIDs,"camera-ids","C1A,C1B,C3","configured camera IDs; default is installed two-way LPR pair plus overview")
	flag.Int64Var(&c.emptyScaleMaxKG,"empty-scale-max-kg",500,"maximum absolute stable weight treated as empty deck")
	flag.Int64Var(&c.minStableWeightKG,"min-stable-weight-kg",1000,"minimum stable weight eligible for pass acceptance")
	flag.IntVar(&c.stableConfirmations,"stable-confirmations",2,"consecutive authoritative stable frames required")
	flag.Int64Var(&c.stableToleranceKG,"stable-tolerance-kg",20,"maximum delta between stable confirmation frames")
	flag.DurationVar(&c.pairMinElapsed,"pair-min-elapsed",0,"minimum valid elapsed time between A->B and B->A; 0 disables")
	flag.DurationVar(&c.pairMaxElapsed,"pair-max-elapsed",0,"maximum valid elapsed time between A->B and B->A; 0 disables expiry")
	flag.BoolVar(&c.pairRequireGrossFirst,"pair-require-gross-first",false,"legacy inbound-only rule: reject a pair whose second pass is heavier than the first; default accepts tare-first (material OUT) trips")
	flag.Int64Var(&c.pairMinGrossKG,"pair-min-gross-kg",0,"minimum valid gross (heavier pass) weight; 0 disables")
	flag.Int64Var(&c.pairMaxGrossKG,"pair-max-gross-kg",0,"maximum valid gross (heavier pass) weight; 0 disables")
	flag.Int64Var(&c.pairMinTareKG,"pair-min-tare-kg",0,"minimum valid tare (lighter pass) weight; 0 disables")
	flag.Int64Var(&c.pairMaxTareKG,"pair-max-tare-kg",0,"maximum valid tare (lighter pass) weight; 0 disables")
	flag.Int64Var(&c.pairMinNetKG,"pair-min-net-kg",0,"minimum valid gross-tare net weight; 0 disables")
	flag.Int64Var(&c.pairMaxNetKG,"pair-max-net-kg",0,"maximum valid gross-tare net weight; 0 disables")
	flag.StringVar(&c.centralTicketURL,"central-ticket-url","","Central completed-ticket POST endpoint; empty disables ticket sync")
	flag.StringVar(&c.centralHeartbeatURL,"central-heartbeat-url","","Central heartbeat POST endpoint; empty disables heartbeat")
	flag.StringVar(&c.centralToken,"central-token","","optional bearer token for Central endpoints")
	flag.DurationVar(&c.centralSyncInterval,"central-sync-interval",5*time.Second,"Central pending-ticket sync interval")
	flag.DurationVar(&c.centralHeartbeatInterval,"central-heartbeat-interval",30*time.Second,"Central heartbeat interval")
	flag.DurationVar(&c.centralTimeout,"central-timeout",5*time.Second,"Central HTTP timeout")
	flag.BoolVar(&c.simulation,"simulation",false,"enable explicit /sim/* test ingress endpoints")
	flag.StringVar(&serviceAction,"service","","Windows Service action: install|start|stop|status|uninstall")
	flag.Parse()
	if c.ioUnitID > 255 { log.Fatal("io-unit-id must be 0..255") }
	switch c.scaleProtocol {
	case "ascii":
	case "laumas-rtu":
		if c.scaleSerial == "" && c.scaleAddr == "" { log.Fatal("scale-protocol laumas-rtu needs -scale-serial COMx or -scale-addr host:port (serial gateway)") }
		if c.scaleSerial != "" && c.scaleAddr != "" { log.Fatal("use either -scale-serial or -scale-addr with laumas-rtu, not both") }
		if c.scaleSlave < 1 || c.scaleSlave > 247 { log.Fatal("scale-slave must be 1..247") }
		if c.scaleStopBits != 1 && c.scaleStopBits != 2 { log.Fatal("scale-stop-bits must be 1 or 2") }
		c.scaleParity=strings.ToUpper(strings.TrimSpace(c.scaleParity));if c.scaleParity!="N"&&c.scaleParity!="E"&&c.scaleParity!="O"{log.Fatal("scale-parity must be N, E or O")}
		if c.laumasStableBit > 15 { log.Fatal("laumas-stable-bit must be -1 or 0..15") }
		if c.laumasOverloadMask > 0xFFFF || c.laumasFaultMask > 0xFFFF { log.Fatal("laumas masks are 16-bit") }
	default:
		log.Fatalf("unknown -scale-protocol %q; use ascii or laumas-rtu", c.scaleProtocol)
	}
	if c.pairMinElapsed < 0 || c.pairMaxElapsed < 0 { log.Fatal("pair elapsed limits cannot be negative") }
	if c.pairMaxElapsed > 0 && c.pairMinElapsed > c.pairMaxElapsed { log.Fatal("pair-min-elapsed cannot exceed pair-max-elapsed") }
	return c, serviceAction
}

func manageService(action string) error {
	switch action {
	case "install": if err:=winservice.Install(winservice.StripManagementArg(os.Args[1:]));err!=nil{return err};fmt.Printf("installed %s\n",winservice.Name);return nil
	case "start": if err:=winservice.Start();err!=nil{return err};fmt.Printf("started %s\n",winservice.Name);return nil
	case "stop": if err:=winservice.Stop(30*time.Second);err!=nil{return err};fmt.Printf("stopped %s\n",winservice.Name);return nil
	case "status": st,err:=winservice.Status();if err!=nil{return err};fmt.Println(st);return nil
	case "uninstall": if err:=winservice.Uninstall();err!=nil{return err};fmt.Printf("uninstalled %s\n",winservice.Name);return nil
	default: return fmt.Errorf("unknown -service action %q; use install|start|stop|status|uninstall",action)
	}
}

func run(ctx context.Context, c config) error {
	weightAudit:=&rawjournal.Journal{Path:c.rawWeightJournal};if err:=weightAudit.Verify();err!=nil&&!os.IsNotExist(err){return fmt.Errorf("raw weight audit integrity: %w",err)}
	eventAudit:=&auditjournal.Journal{Path:c.eventJournal};if err:=eventAudit.Verify();err!=nil&&!os.IsNotExist(err){return fmt.Errorf("operational event audit integrity: %w",err)}
	store,err:=sqlitestore.Open(c.dbPath);if err!=nil{return fmt.Errorf("SQLite edge store: %w",err)};defer store.Close()
	if st,err:=store.Status(ctx);err!=nil{return fmt.Errorf("SQLite edge store integrity: %w",err)}else{log.Printf("SQLite ready path=%s schema=%d tickets=%d queued=%d called=%d completed_cycles=%d pending_sync=%d",st.Path,st.Schema,st.Tickets,st.QueuedCycles,st.CalledCycles,st.CompletedCycles,st.PendingSync)}

	pairPolicy:=domain.PairPolicy{RequireGrossFirst:c.pairRequireGrossFirst,MinElapsed:c.pairMinElapsed,MaxElapsed:c.pairMaxElapsed,MinGrossKG:c.pairMinGrossKG,MaxGrossKG:c.pairMaxGrossKG,MinTareKG:c.pairMinTareKG,MaxTareKG:c.pairMaxTareKG,MinNetKG:c.pairMinNetKG,MaxNetKG:c.pairMaxNetKG}
	cycleCoordinator:=cycle.New(store,pairPolicy);commitBridge:=twopass.NewCommitBridge(cycleCoordinator)
	vehicleRegistry,err:=registry.Parse(c.vehicleMap);if err!=nil{return fmt.Errorf("vehicle map: %w",err)}
	innerWorkflow:=engine.New(engine.Config{StationID:c.stationID,EmptyScaleMaxKG:c.emptyScaleMaxKG,MinStableWeightKG:c.minStableWeightKG,StableConfirmations:c.stableConfirmations,StableToleranceKG:c.stableToleranceKG},commitBridge,vehicleRegistry)
	twoWayWorkflow:=twopass.NewWorkflow(innerWorkflow,commitBridge,cycleCoordinator)
	workflow:=&workflowaudit.Recorder{Engine:twoWayWorkflow,Audit:eventAudit,StationID:c.stationID,RuntimeGitSHA:gitSHA}

	if pairPolicy.MaxElapsed>0{go func(){ticker:=time.NewTicker(time.Minute);defer ticker.Stop();for{select{case <-ctx.Done():return;case at:=<-ticker.C:n,err:=cycleCoordinator.ExpireOrphans(context.Background(),at.UTC());if err!=nil{log.Printf("cycle orphan expiry: %v",err)}else if n>0{log.Printf("cycle orphan expiry marked %d incomplete cycle(s)",n)}}}}()}

	rfid:=&ingress.RFID{};lpr:=&ingress.LPR{}
	rfid.OnObservation=func(o domain.RFIDObservation){if err:=workflow.ObserveRFID(context.Background(),o);err!=nil{log.Printf("workflow RFID observation: %v",err)}}
	lpr.OnObservation=func(o domain.LPRObservation){if err:=workflow.ObserveLPR(context.Background(),o);err!=nil{log.Printf("workflow LPR observation: %v",err)}}

	laumas:=c.scaleProtocol=="laumas-rtu"
	scaleSource:=c.scaleAddr;if laumas&&c.scaleSerial!=""{scaleSource=fmt.Sprintf("%s@%d",c.scaleSerial,c.scaleBaud)}
	scaleMonitor:=scaleascii.NewMonitor(scaleSource!="",scaleSource)
	if laumas{
		decode:=laumasrtu.DecodeConfig{OverloadMask:uint16(c.laumasOverloadMask),FaultMask:uint16(c.laumasFaultMask)};if c.laumasStableBit>=0{decode.StableBit=uint(c.laumasStableBit);decode.StableBitSet=true}
		dial:=laumasrtu.TCPDialer(c.scaleAddr,2*time.Second);if c.scaleSerial!=""{dial=laumasrtu.SerialDialer(laumasrtu.SerialConfig{Port:c.scaleSerial,Baud:c.scaleBaud,Parity:c.scaleParity[0],StopBits:c.scaleStopBits})}
		collector:=&laumasrtu.Collector{Source:scaleSource,Dial:dial,Slave:byte(c.scaleSlave),PollInterval:c.scalePoll,ResponseTimeout:c.scaleResponseTimeout,ReconnectDelay:c.reconnectDelay,MaxMissedPolls:c.scaleMissedPolls,StationID:c.stationID,TransactionID:workflow.ActiveTransactionID,Journal:weightAudit,Decode:decode,
			OnReading:func(a domain.AuditedScaleReading){scaleMonitor.Reading(a.Reading);_ = workflow.ClearFault(context.Background(),domain.DeviceScale);if err:=workflow.ObserveScale(context.Background(),a);err!=nil{log.Printf("workflow audited scale observation: %v",err)}},
			OnFault:func(err error){scaleMonitor.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:err.Error(),Overridable:false,Critical:true})}}
		go func(){if err:=collector.Run(ctx);err!=nil&&ctx.Err()==nil{scaleMonitor.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:"laumas collector stopped: "+err.Error(),Overridable:false,Critical:true});log.Printf("CRITICAL laumas raw weight collector stopped: %v",err)}}()
	}
	if !laumas&&c.scaleAddr!=""{collector:=&scaleascii.StreamCollector{Addr:c.scaleAddr,StationID:c.stationID,TransactionID:workflow.ActiveTransactionID,Journal:weightAudit,ReconnectDelay:c.reconnectDelay,OnReading:func(a domain.AuditedScaleReading){scaleMonitor.Reading(a.Reading);_ = workflow.ClearFault(context.Background(),domain.DeviceScale);if err:=workflow.ObserveScale(context.Background(),a);err!=nil{log.Printf("workflow audited scale observation: %v",err)}},OnFault:func(err error){scaleMonitor.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:err.Error(),Overridable:false,Critical:true})}};go func(){if err:=collector.Run(ctx);err!=nil&&ctx.Err()==nil{scaleMonitor.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:"raw weight collector stopped: "+err.Error(),Overridable:false,Critical:true});log.Printf("CRITICAL raw weight collector stopped: %v",err)}}()}

	ioMonitor:=runtimeio.NewMonitor(false);var safeOutputs runtimeio.Outputs
	if c.ioAddr!=""{mapping,err:=modbustcp.ParseMapping(c.ioMapSpec);if err!=nil{return fmt.Errorf("io-map: %w",err)};if mapping.SafetyClear==nil{log.Printf("WARNING remote I/O enabled without safety_clear mapping; fail-safe SafetyClear=false will block automatic entry")};client:=&modbustcp.Client{Addr:c.ioAddr,UnitID:byte(c.ioUnitID),Timeout:c.ioTimeout};hardwareIO:=&modbustcp.IO{Client:client,Mapping:mapping};auditedOutputs:=&runtimeio.AuditedOutputs{Inner:hardwareIO,Audit:eventAudit,StationID:c.stationID,RuntimeGitSHA:gitSHA,TransactionID:workflow.ActiveTransactionID,OnAuditFailure:func(err error){_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceAuditStore,Health:domain.HealthFault,Reason:"operational audit output gate failed: "+err.Error(),Overridable:false,Critical:true})}};safeOutputs=auditedOutputs;ioMonitor=runtimeio.NewMonitor(true);controller:=&runtimeio.Controller{Workflow:workflow,Inputs:hardwareIO,Outputs:auditedOutputs,Monitor:ioMonitor,PollInterval:c.ioPoll,BuzzerPulse:c.buzzerPulse,BarrierFeedbackTimeout:c.barrierFeedbackTimeout};go func(){if err:=controller.Run(ctx);err!=nil&&ctx.Err()==nil{log.Printf("CRITICAL runtime I/O controller stopped: %v",err)}}()}

	central:=&centralsync.Worker{Store:store,Audit:eventAudit,Config:centralsync.Config{TicketURL:c.centralTicketURL,HeartbeatURL:c.centralHeartbeatURL,BearerToken:c.centralToken,StationID:c.stationID,Version:version,GitSHA:gitSHA,Interval:c.centralSyncInterval,HeartbeatEvery:c.centralHeartbeatInterval,Timeout:c.centralTimeout}}
	if c.centralTicketURL!=""||c.centralHeartbeatURL!=""{go func(){if err:=central.Run(ctx);err!=nil&&ctx.Err()==nil{log.Printf("Central sync worker stopped: %v",err)}}()}

	cameraIDs:=parseIDs(c.cameraIDs)
	s:=&httpapi.Server{RFID:rfid,LPR:lpr,WeightAudit:weightAudit,EventAudit:eventAudit,ScaleMonitor:scaleMonitor,Workflow:workflow,Cycles:cycleCoordinator,CameraIDs:cameraIDs,IOStatus:func()any{return ioMonitor.Snapshot()},CentralStatus:func()any{return central.Snapshot()},StorageStatus:func(x context.Context)(any,error){return store.Status(x)},LatestTicket:store.LastTicket,AllowSimulation:c.simulation,Version:version,GitSHA:gitSHA}
	httpServer:=&http.Server{Addr:c.listen,Handler:s.Handler(),ReadHeaderTimeout:5*time.Second};httpErr:=make(chan error,1)
	go func(){err:=httpServer.ListenAndServe();if err!=nil&&!errors.Is(err,http.ErrServerClosed){httpErr<-err;return};httpErr<-nil}()
	log.Printf("plantops-edge-scale %s sha=%s http=%s simulation=%v db=%s two_pass=true cameras=%v central=%v",version,gitSHA,c.listen,c.simulation,c.dbPath,keys(cameraIDs),central.Snapshot().Enabled)
	select{
	case err:=<-httpErr:if err!=nil{return fmt.Errorf("HTTP server: %w",err)};return nil
	case <-ctx.Done():log.Printf("shutdown requested");shutdownCtx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();if safeOutputs!=nil{if err:=safeOutputs.SafeState(shutdownCtx);err!=nil{log.Printf("SafeState on shutdown: %v",err)}};if err:=httpServer.Shutdown(shutdownCtx);err!=nil{log.Printf("HTTP shutdown: %v",err)};if err:=store.Checkpoint(shutdownCtx);err!=nil{log.Printf("SQLite checkpoint on shutdown: %v",err)};log.Printf("shutdown complete");return nil
	}
}

func parseIDs(spec string)map[string]bool{out:=map[string]bool{};for _,raw:=range strings.Split(spec,","){id:=strings.ToUpper(strings.TrimSpace(raw));if id!=""{out[id]=true}};return out}
func keys(m map[string]bool)[]string{out:=make([]string,0,len(m));for k:=range m{out=append(out,k)};return out}
