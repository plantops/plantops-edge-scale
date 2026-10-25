package cycle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/ports"
)

var (
	ErrNoCalledCycle  = errors.New("no matching CALLED cycle for returning vehicle")
	ErrInvalidPair    = errors.New("second pass does not form a valid pair")
	ErrPairWindowOver = errors.New("pair time window expired")
)

type Coordinator struct {
	store ports.CycleStore
	policy domain.PairPolicy
	now func() time.Time
	newID func(string) string
}

func New(store ports.CycleStore,policy domain.PairPolicy)*Coordinator{return &Coordinator{store:store,policy:policy,now:func()time.Time{return time.Now().UTC()},newID:randomID}}

func (c *Coordinator) RecordFirstPass(ctx context.Context,pass domain.WeighPass)(domain.WeighCycle,error){
	if c.store==nil{return domain.WeighCycle{},errors.New("cycle store unavailable")}
	if pass.ID==""{pass.ID=c.newID("pass1")};pass.Number=domain.PassFirst;if pass.Direction==""{pass.Direction=domain.DirectionAToB};if pass.CommittedAt.IsZero(){pass.CommittedAt=c.now()}
	if err:=domain.ValidateFirstPass(pass);err!=nil{return domain.WeighCycle{},err}
	cycleID:=c.newID("cycle");pass.CycleID=cycleID;cy:=domain.WeighCycle{ID:cycleID,StationID:pass.StationID,Plate:pass.Plate,RFID:pass.RFID,Status:domain.CycleQueued,FirstPass:pass,QueuedAt:pass.CommittedAt.UTC()}
	if err:=c.store.OpenCycle(ctx,cy);err!=nil{return domain.WeighCycle{},err};return cy,nil
}
func (c *Coordinator) Call(ctx context.Context,cycleID string)error{if c.store==nil{return errors.New("cycle store unavailable")};return c.store.CallCycle(ctx,cycleID,c.now())}
func (c *Coordinator) Queue(ctx context.Context)([]domain.WeighCycle,error){if c.store==nil{return nil,errors.New("cycle store unavailable")};return c.store.ListQueue(ctx)}

func (c *Coordinator) RecordSecondPass(ctx context.Context,pass domain.WeighPass)(domain.WeighCycle,domain.Ticket,error){
	if c.store==nil{return domain.WeighCycle{},domain.Ticket{},errors.New("cycle store unavailable")}
	if pass.Direction!=domain.DirectionBToA{return domain.WeighCycle{},domain.Ticket{},fmt.Errorf("%w: second pass direction must be B_TO_A",ErrInvalidPair)}
	if pass.ID==""{pass.ID=c.newID("pass2")};pass.Number=domain.PassSecond;if pass.CommittedAt.IsZero(){pass.CommittedAt=c.now()}
	open,found,err:=c.store.FindCalledCycle(ctx,pass.Plate,pass.RFID);if err!=nil{return domain.WeighCycle{},domain.Ticket{},err};if !found{return domain.WeighCycle{},domain.Ticket{},ErrNoCalledCycle}
	pass.CycleID=open.ID;validation:=domain.ValidatePair(open.FirstPass,pass,c.policy)
	if !validation.Valid{
		if validation.Status==domain.CyclePairTimeInvalid&&c.policy.MaxElapsed>0&&validation.Elapsed>c.policy.MaxElapsed{_ = c.store.MarkCycleStatus(ctx,open.ID,domain.CyclePairTimeInvalid,validation.Reason,c.now());return open,domain.Ticket{},fmt.Errorf("%w: %s",ErrPairWindowOver,validation.Reason)}
		if validation.Status!=domain.CycleWrongTruck{_ = c.store.MarkCycleStatus(ctx,open.ID,domain.CycleCalled,validation.Reason,c.now())}
		return open,domain.Ticket{},fmt.Errorf("%w: %s",ErrInvalidPair,validation.Reason)
	}
	now:=c.now();open.SecondPass=&pass;open.Status=domain.CycleComplete;open.PairElapsed=validation.Elapsed;open.GrossKG=validation.GrossKG;open.TareKG=validation.TareKG;open.NetKG=validation.NetKG;open.Flow=validation.Flow;open.CompletedAt=&now;open.LastBlockReason=""
	camera:=append([]domain.CameraEvidence{},open.FirstPass.CameraEvidence...);camera=append(camera,pass.CameraEvidence...)
	ticket:=domain.Ticket{ID:c.newID("ticket"),StationID:open.StationID,TransactionID:open.ID,CycleID:open.ID,Plate:open.Plate,RFID:open.RFID,Mode:combineMode(open.FirstPass.Mode,pass.Mode),Overrides:append(append([]domain.Override{},open.FirstPass.Overrides...),pass.Overrides...),CameraEvidence:camera,CommittedAt:now}
	if err:=c.store.CompleteCycle(ctx,open,ticket);err!=nil{return open,domain.Ticket{},err}
	ticket.FirstPassID=open.FirstPass.ID;ticket.SecondPassID=pass.ID;ticket.GrossKG=open.GrossKG;ticket.TareKG=open.TareKG;ticket.NetKG=open.NetKG;ticket.Flow=open.Flow;ticket.FirstWeightObservedAt=open.FirstPass.Weight.ObservedAt;ticket.SecondWeightObservedAt=pass.Weight.ObservedAt;ticket.FirstWeightRawRef=open.FirstPass.Weight.RawRef;ticket.SecondWeightRawRef=pass.Weight.RawRef;ticket.PairElapsedSeconds=int64(open.PairElapsed/time.Second)
	return open,ticket,nil
}

func (c *Coordinator) ExpireOrphans(ctx context.Context,at time.Time)(int,error){if c.store==nil{return 0,errors.New("cycle store unavailable")};if c.policy.MaxElapsed<=0{return 0,nil};if at.IsZero(){at=c.now()};queue,err:=c.store.ListQueue(ctx);if err!=nil{return 0,err};count:=0;for _,open:=range queue{firstAt:=open.FirstPass.Weight.ObservedAt;if firstAt.IsZero()||at.Sub(firstAt)<=c.policy.MaxElapsed{continue};reason:=fmt.Sprintf("no valid B_TO_A pair within configured window %s",c.policy.MaxElapsed);if err:=c.store.MarkCycleStatus(ctx,open.ID,domain.CycleOrphanedFirstPass,reason,at);err!=nil{return count,err};count++};return count,nil}

func combineMode(a,b domain.Mode)domain.Mode{rank:=func(m domain.Mode)int{switch m{case domain.ModeLockout:return 4;case domain.ModeManual:return 3;case domain.ModeDegraded:return 2;default:return 1}};if rank(b)>rank(a){return b};if a==""{return domain.ModeNormal};return a}
func randomID(prefix string)string{var b [12]byte;if _,err:=rand.Read(b[:]);err!=nil{return fmt.Sprintf("%s-%d",prefix,time.Now().UTC().UnixNano())};return prefix+"-"+hex.EncodeToString(b[:])}
