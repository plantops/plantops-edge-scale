package twopass

import (
	"context"
	"errors"
	"sync"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/engine"
)

type InnerWorkflow interface {
	Snapshot() engine.Snapshot
	ActiveTransactionID() string
	ObservePosition(context.Context, domain.PositionSnapshot) error
	ObserveRFID(context.Context, domain.RFIDObservation) error
	ObserveLPR(context.Context, domain.LPRObservation) error
	ObserveScale(context.Context, domain.AuditedScaleReading) error
	ObserveFault(context.Context, domain.Fault) error
	ClearFault(context.Context, domain.DeviceID) error
	AuthorizeOverride(context.Context, domain.Override) error
	ResetCompleted() error
}

type Workflow struct {
	Inner InnerWorkflow
	Bridge *CommitBridge
	Cycles Cycles
	mu sync.RWMutex
	direction domain.Direction
	bound *domain.WeighCycle
}

func NewWorkflow(inner InnerWorkflow, bridge *CommitBridge, cycles Cycles) *Workflow { return &Workflow{Inner:inner,Bridge:bridge,Cycles:cycles} }
func (w *Workflow) ActiveTransactionID() string { if w.Inner==nil{return ""};return w.Inner.ActiveTransactionID() }

func (w *Workflow) ObservePosition(ctx context.Context, physical domain.PositionSnapshot) error {
	if w.Inner==nil{return errors.New("two-pass workflow inner engine is nil")}
	before:=w.Inner.Snapshot()
	if before.Transaction!=nil&&before.Transaction.State==domain.StateComplete&&allPresenceClear(physical){txID:=before.Transaction.ID;if err:=w.Inner.ResetCompleted();err==nil{w.resetSession(txID)}}
	w.mu.Lock();if w.direction==""&&w.Inner.ActiveTransactionID()==""{w.direction=detectDirection(physical);if w.Bridge!=nil&&w.direction!=""{w.Bridge.SetDirection(w.direction)}};direction:=w.direction;w.mu.Unlock()
	logical:=physical;if direction==domain.DirectionBToA{logical=swapPhysicalPosition(physical)}
	if err:=w.Inner.ObservePosition(ctx,logical);err!=nil{return err};w.tryBindCalled(ctx);return nil
}
func (w *Workflow) ObserveRFID(ctx context.Context,o domain.RFIDObservation)error{if err:=w.Inner.ObserveRFID(ctx,o);err!=nil{return err};w.tryBindCalled(ctx);return nil}
func (w *Workflow) ObserveLPR(ctx context.Context,o domain.LPRObservation)error{if err:=w.Inner.ObserveLPR(ctx,o);err!=nil{return err};w.tryBindCalled(ctx);return nil}
func (w *Workflow) ObserveCamera(_ context.Context,e domain.CameraEvidence)error{if w.Bridge==nil{return errors.New("camera evidence bridge unavailable")};txID:=w.ActiveTransactionID();return w.Bridge.AddEvidence(txID,e)}
func (w *Workflow) ObserveScale(ctx context.Context,a domain.AuditedScaleReading)error{return w.Inner.ObserveScale(ctx,a)}
func (w *Workflow) ObserveFault(ctx context.Context,f domain.Fault)error{return w.Inner.ObserveFault(ctx,f)}
func (w *Workflow) ClearFault(ctx context.Context,d domain.DeviceID)error{return w.Inner.ClearFault(ctx,d)}
func (w *Workflow) AuthorizeOverride(ctx context.Context,o domain.Override)error{return w.Inner.AuthorizeOverride(ctx,o)}
func (w *Workflow) ResetCompleted()error{if w.Inner==nil{return nil};s:=w.Inner.Snapshot();txID:="";if s.Transaction!=nil{txID=s.Transaction.ID};if err:=w.Inner.ResetCompleted();err!=nil{return err};w.resetSession(txID);return nil}

func (w *Workflow) Snapshot() engine.Snapshot {
	if w.Inner==nil{return engine.Snapshot{}}
	s:=w.Inner.Snapshot();if s.Transaction==nil{return s}
	tx:=cloneTx(s.Transaction)
	w.mu.RLock();direction:=w.direction;var bound *domain.WeighCycle;if w.bound!=nil{v:=*w.bound;bound=&v};w.mu.RUnlock()
	if direction==""{direction=domain.DirectionAToB};tx.Direction=direction;if direction==domain.DirectionAToB{tx.PassNumber=domain.PassFirst}else{tx.PassNumber=domain.PassSecond}
	if direction==domain.DirectionBToA{tx.PositionSnapshot=swapPhysicalPosition(tx.PositionSnapshot);tx.Outputs=swapPhysicalOutputs(tx.Outputs)}
	if w.Bridge!=nil{tx.CameraEvidence=w.Bridge.Evidence(tx.ID)}
	if bound!=nil{tx.CycleID=bound.ID;tx.CycleStatus=bound.Status;tx.GrossKG=bound.GrossKG;tx.TareKG=bound.TareKG;tx.NetKG=bound.NetKG;tx.PairElapsedSeconds=int64(bound.PairElapsed/1e9)}
	if w.Bridge!=nil{
		if out,ok:=w.Bridge.Outcome(tx.ID);ok{
			tx.CycleID=out.Cycle.ID;tx.CycleStatus=out.Cycle.Status
			if out.Direction==domain.DirectionAToB{tx.TicketID="";tx.BusinessComplete=false}
			if out.Direction==domain.DirectionBToA&&out.FinalTicket.ID!=""{tx.TicketID=out.FinalTicket.ID;tx.BusinessComplete=true;tx.CycleStatus=domain.CycleComplete;tx.GrossKG=out.Cycle.GrossKG;tx.TareKG=out.Cycle.TareKG;tx.NetKG=out.Cycle.NetKG;tx.PairElapsedSeconds=int64(out.Cycle.PairElapsed/1e9)}
			if out.Err!=""{tx.BusinessComplete=false;tx.LastBlockReason=out.Err}
		}
	}
	if direction==domain.DirectionBToA&&tx.Identity==domain.IdentityAccepted&&bound==nil&&tx.State!=domain.StateFaultLockout&&tx.State!=domain.StateComplete{tx.State=domain.StateQueueMismatch;tx.CycleStatus=domain.CycleUnpairedReturn;tx.BusinessComplete=false;tx.Outputs=domain.DesiredOutputs{};tx.LastBlockReason="B_TO_A return has no matching CALLED cycle; automatic entry blocked"}
	s.Transaction=tx;s.State=tx.State;s.Mode=tx.Mode;return s
}

func (w *Workflow) tryBindCalled(ctx context.Context){if w.Cycles==nil||w.Inner==nil{return};w.mu.RLock();direction:=w.direction;w.mu.RUnlock();if direction!=domain.DirectionBToA{return};s:=w.Inner.Snapshot();if s.Transaction==nil||s.Transaction.Identity!=domain.IdentityAccepted{return};// Once this physical pass has committed, its cycle is COMPLETE and therefore no longer CALLED. Re-resolving then would find nothing and unbind it, flipping the snapshot to QUEUE_MISMATCH and withdrawing the release outputs of a truck whose ticket already exists.
	if w.Bridge!=nil{if _,committed:=w.Bridge.Outcome(s.Transaction.ID);committed{return}};cycle,found,err:=w.Cycles.ResolveCalled(ctx,s.Transaction.LPR.Plate,s.Transaction.RFID.Tag);w.mu.Lock();defer w.mu.Unlock();if err!=nil||!found{w.bound=nil;return};v:=cycle;w.bound=&v}
func (w *Workflow) resetSession(txID string){w.mu.Lock();w.direction="";w.bound=nil;w.mu.Unlock();if w.Bridge!=nil{w.Bridge.Reset(txID)}}
func detectDirection(p domain.PositionSnapshot)domain.Direction{switch{case p.EntryPresent&&!p.ExitPresent:return domain.DirectionAToB;case p.ExitPresent&&!p.EntryPresent:return domain.DirectionBToA;default:return ""}}
func swapPhysicalPosition(p domain.PositionSnapshot)domain.PositionSnapshot{return domain.PositionSnapshot{EntryPresent:p.ExitPresent,FrontPresent:p.RearPresent,RearPresent:p.FrontPresent,ExitPresent:p.EntryPresent,EntryBarrierOpen:p.ExitBarrierOpen,EntryBarrierClosed:p.ExitBarrierClosed,ExitBarrierOpen:p.EntryBarrierOpen,ExitBarrierClosed:p.EntryBarrierClosed,SafetyClear:p.SafetyClear,Observed:p.Observed}}
func swapPhysicalOutputs(v domain.DesiredOutputs)domain.DesiredOutputs{return domain.DesiredOutputs{EntryGreen:v.ExitGreen,ExitGreen:v.EntryGreen,Buzzer:v.Buzzer,EntryBarrierOpen:v.ExitBarrierOpen,ExitBarrierOpen:v.EntryBarrierOpen}}
func allPresenceClear(p domain.PositionSnapshot)bool{return !p.EntryPresent&&!p.FrontPresent&&!p.RearPresent&&!p.ExitPresent}
func cloneTx(in *domain.Transaction)*domain.Transaction{if in==nil{return nil};out:=*in;out.Faults=append([]domain.Fault(nil),in.Faults...);out.Overrides=append([]domain.Override(nil),in.Overrides...);out.CameraEvidence=append([]domain.CameraEvidence(nil),in.CameraEvidence...);if in.LatestScale!=nil{v:=*in.LatestScale;out.LatestScale=&v};if in.AcceptedWeight!=nil{v:=*in.AcceptedWeight;out.AcceptedWeight=&v};if in.LocalCommittedAt!=nil{v:=*in.LocalCommittedAt;out.LocalCommittedAt=&v};if in.CompletedAt!=nil{v:=*in.CompletedAt;out.CompletedAt=&v};return &out}
