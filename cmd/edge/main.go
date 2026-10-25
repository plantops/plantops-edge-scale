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
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/modbustcp"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/rawjournal"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/registry"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/scaleascii"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/sqlitestore"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/w100"
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
	centralTicketURL, centralHeartbeatURL, centralToken string
	centralSyncInterval, centralHeartbeatInterval, centralTimeout time.Duration
	simulation bool
	scaleDriver, scalePort, scaleParity string
	scaleBaud, scaleStopBits int
	scaleUnit uint
	scalePoll, scaleRepeat time.Duration
	probe bool
}

func main() {
	cfg, serviceAction := parseFlags()
	if cfg.probe { if err := probe(cfg); err != nil { log.Fatal(err) }; return }
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
	flag.Int64Var(&c.pairMinGrossKG,"pair-min-gross-kg",0,"minimum valid first/gross weight; 0 disables")
	flag.Int64Var(&c.pairMaxGrossKG,"pair-max-gross-kg",0,"maximum valid first/gross weight; 0 disables")
	flag.Int64Var(&c.pairMinTareKG,"pair-min-tare-kg",0,"minimum valid second/tare weight; 0 disables")
	flag.Int64Var(&c.pairMaxTareKG,"pair-max-tare-kg",0,"maximum valid second/tare weight; 0 disables")
	flag.Int64Var(&c.pairMinNetKG,"pair-min-net-kg",0,"minimum valid gross-tare net weight; 0 disables")
	flag.Int64Var(&c.pairMaxNetKG,"pair-max-net-kg",0,"maximum valid gross-tare net weight; 0 disables")
	flag.StringVar(&c.centralTicketURL,"central-ticket-url","","Central completed-ticket POST endpoint; empty disables ticket sync")
	flag.StringVar(&c.centralHeartbeatURL,"central-heartbeat-url","","Central heartbeat POST endpoint; empty disables heartbeat")
	flag.StringVar(&c.centralToken,"central-token","","optional bearer token for Central endpoints")
	flag.DurationVar(&c.centralSyncInterval,"central-sync-interval",5*time.Second,"Central pending-ticket sync interval")
	flag.DurationVar(&c.centralHeartbeatInterval,"central-heartbeat-interval",30*time.Second,"Central heartbeat interval")
	flag.DurationVar(&c.centralTimeout,"central-timeout",5*time.Second,"Central HTTP timeout")
	flag.BoolVar(&c.simulation,"simulation",false,"enable explicit /sim/* test ingress endpoints")
	flag.StringVar(&c.scaleDriver,"scale-driver","ascii","scale input: ascii (TCP text stream, -scale-addr) or w100 (LAUMAS W100 Modbus RTU, -scale-port)")
	flag.StringVar(&c.scalePort,"scale-port","","W100 line: COM5, /dev/ttyUSB0, or tcp:host:port for an RS485-Ethernet gateway")
	flag.IntVar(&c.scaleBaud,"scale-baud",9600,"W100 baud rate, as set in the instrument's serial menu")
	flag.StringVar(&c.scaleParity,"scale-parity","N","W100 parity: N, E or O")
	flag.IntVar(&c.scaleStopBits,"scale-stop-bits",1,"W100 stop bits: 1 or 2")
	flag.UintVar(&c.scaleUnit,"scale-unit-id",1,"W100 Modbus address 1..99")
	flag.DurationVar(&c.scalePoll,"scale-poll",200*time.Millisecond,"W100 poll interval")
	flag.DurationVar(&c.scaleRepeat,"scale-repeat-every",time.Second,"W100: an unchanged answer is journaled and delivered at most this often; 0 = every poll")
	flag.BoolVar(&c.probe,"probe",false,"read the scale for 10 s, print each reading, and exit (bench wiring check)")
	flag.StringVar(&serviceAction,"service","","Windows Service action: install|start|stop|status|uninstall")
	flag.Parse()
	if c.ioUnitID > 255 { log.Fatal("io-unit-id must be 0..255") }
	if c.scaleDriver != "ascii" && c.scaleDriver != "w100" { log.Fatal("scale-driver must be ascii or w100") }
	if c.scaleDriver == "w100" && (c.scaleUnit < 1 || c.scaleUnit > 99) { log.Fatal("scale-unit-id must be 1..99") }
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

	pairPolicy:=domain.PairPolicy{MinElapsed:c.pairMinElapsed,MaxElapsed:c.pairMaxElapsed,MinGrossKG:c.pairMinGrossKG,MaxGrossKG:c.pairMaxGrossKG,MinTareKG:c.pairMinTareKG,MaxTareKG:c.pairMaxTareKG,MinNetKG:c.pairMinNetKG,MaxNetKG:c.pairMaxNetKG}
	cycleCoordinator:=cycle.New(store,pairPolicy);commitBridge:=twopass.NewCommitBridge(cycleCoordinator)
	vehicleRegistry,err:=registry.Parse(c.vehicleMap);if err!=nil{return fmt.Errorf("vehicle map: %w",err)}
	innerWorkflow:=engine.New(engine.Config{StationID:c.stationID,EmptyScaleMaxKG:c.emptyScaleMaxKG,MinStableWeightKG:c.minStableWeightKG,StableConfirmations:c.stableConfirmations,StableToleranceKG:c.stableToleranceKG},commitBridge,vehicleRegistry)
	twoWayWorkflow:=twopass.NewWorkflow(innerWorkflow,commitBridge,cycleCoordinator)
	workflow:=&workflowaudit.Recorder{Engine:twoWayWorkflow,Audit:eventAudit,StationID:c.stationID,RuntimeGitSHA:gitSHA}

	if pairPolicy.MaxElapsed>0{go func(){ticker:=time.NewTicker(time.Minute);defer ticker.Stop();for{select{case <-ctx.Done():return;case at:=<-ticker.C:n,err:=cycleCoordinator.ExpireOrphans(context.Background(),at.UTC());if err!=nil{log.Printf("cycle orphan expiry: %v",err)}else if n>0{log.Printf("cycle orphan expiry marked %d incomplete cycle(s)",n)}}}}()}

	rfid:=&ingress.RFID{};lpr:=&ingress.LPR{}
	rfid.OnObservation=func(o domain.RFIDObservation){if err:=workflow.ObserveRFID(context.Background(),o);err!=nil{log.Printf("workflow RFID observation: %v",err)}}
	lpr.OnObservation=func(o domain.LPRObservation){if err:=workflow.ObserveLPR(context.Background(),o);err!=nil{log.Printf("workflow LPR observation: %v",err)}}

	onScaleReading:=func(m *scaleascii.Monitor)func(domain.AuditedScaleReading){return func(a domain.AuditedScaleReading){m.Reading(a.Reading);_ = workflow.ClearFault(context.Background(),domain.DeviceScale);if err:=workflow.ObserveScale(context.Background(),a);err!=nil{log.Printf("workflow audited scale observation: %v",err)}}}
	onScaleFault:=func(m *scaleascii.Monitor)func(error){return func(err error){m.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:err.Error(),Overridable:false,Critical:true})}}
	onCollectorStopped:=func(m *scaleascii.Monitor,err error){if err!=nil&&ctx.Err()==nil{m.Fault(err);_ = workflow.ObserveFault(context.Background(),domain.Fault{Device:domain.DeviceScale,Health:domain.HealthFault,Reason:"raw weight collector stopped: "+err.Error(),Overridable:false,Critical:true});log.Printf("CRITICAL raw weight collector stopped: %v",err)}}
	var scaleMonitor *scaleascii.Monitor
	switch c.scaleDriver {
	case "w100":
		wc:=w100Collector(c);scaleMonitor=scaleascii.NewMonitor(c.scalePort!="",wc.Source())
		if c.scalePort!=""{wc.TransactionID=workflow.ActiveTransactionID;wc.Journal=weightAudit;wc.OnReading=onScaleReading(scaleMonitor);wc.OnFault=onScaleFault(scaleMonitor);go func(){onCollectorStopped(scaleMonitor,wc.Run(ctx))}()}
	default:
		scaleMonitor=scaleascii.NewMonitor(c.scaleAddr!="",c.scaleAddr)
		if c.scaleAddr!=""{collector:=&scaleascii.StreamCollector{Addr:c.scaleAddr,StationID:c.stationID,TransactionID:workflow.ActiveTransactionID,Journal:weightAudit,ReconnectDelay:c.reconnectDelay,OnReading:onScaleReading(scaleMonitor),OnFault:onScaleFault(scaleMonitor)};go func(){onCollectorStopped(scaleMonitor,collector.Run(ctx))}()}
	}

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

func w100Collector(c config) *w100.Collector {
	parity := byte('N')
	if c.scaleParity != "" { parity = strings.ToUpper(c.scaleParity)[0] }
	return &w100.Collector{Port: c.scalePort, Line: w100.LineSettings{Baud: c.scaleBaud, Parity: parity, StopBits: c.scaleStopBits},
		Unit: byte(c.scaleUnit), Poll: c.scalePoll, RepeatEvery: c.scaleRepeat, ReconnectDelay: c.reconnectDelay, StationID: c.stationID}
}

// probe prints what the W100 says for 10 s, journaling nothing. Bench check only:
// it never feeds the workflow.
func probe(c config) error {
	if c.scaleDriver != "w100" || c.scalePort == "" { return errors.New("-probe needs -scale-driver w100 -scale-port COMx") }
	wc := w100Collector(c)
	p, err := w100.Open(c.scalePort, wc.Line)
	if err != nil { return err }
	defer p.Close()
	fmt.Printf("probing %s\n", wc.Source())
	req := w100.ReadRequest(byte(c.scaleUnit), w100.RegStatus, w100.StatusBlockLen)
	ok := 0
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		raw, err := w100.Transact(p, req, 300*time.Millisecond)
		if err != nil { fmt.Printf("%s  %v  % X\n", time.Now().Format("15:04:05.0"), err, raw); continue }
		regs, err := w100.ParseReadResponse(byte(c.scaleUnit), w100.StatusBlockLen, raw)
		if err != nil { fmt.Printf("%s  %v  % X\n", time.Now().Format("15:04:05.0"), err, raw); continue }
		b, err := w100.Decode(regs)
		if err != nil { fmt.Printf("%s  %v\n", time.Now().Format("15:04:05.0"), err); continue }
		ok++
		fmt.Printf("%s  %s\n", time.Now().Format("15:04:05.0"), b.Text())
	}
	if ok == 0 { return errors.New("no valid answer: check COM port, A/B wires, baud/parity and the W100 Modbus address") }
	return nil
}
