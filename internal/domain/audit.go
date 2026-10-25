package domain

import "time"

type AuditKind string

const (
	AuditTransactionStarted AuditKind = "TRANSACTION_STARTED"
	AuditStateTransition    AuditKind = "STATE_TRANSITION"
	AuditRFIDObserved       AuditKind = "RFID_OBSERVED"
	AuditLPRObserved        AuditKind = "LPR_OBSERVED"
	AuditCameraEvidence     AuditKind = "CAMERA_EVIDENCE"
	AuditIdentityDecision   AuditKind = "IDENTITY_DECISION"
	AuditPositionDecision   AuditKind = "POSITION_DECISION"
	AuditFaultSet           AuditKind = "FAULT_SET"
	AuditFaultCleared       AuditKind = "FAULT_CLEARED"
	AuditOverrideAuthorized AuditKind = "OVERRIDE_AUTHORIZED"
	AuditStableAccepted     AuditKind = "STABLE_WEIGHT_ACCEPTED"
	AuditTicketCommitted    AuditKind = "TICKET_COMMITTED"
	AuditDesiredOutputs     AuditKind = "DESIRED_OUTPUTS_CHANGED"
	AuditOutputCommand      AuditKind = "OUTPUT_COMMAND"
	AuditOutputResult       AuditKind = "OUTPUT_RESULT"
	AuditBarrierFeedback    AuditKind = "BARRIER_FEEDBACK"
	AuditCentralSyncAttempt AuditKind = "CENTRAL_SYNC_ATTEMPT"
	AuditCentralSyncAck     AuditKind = "CENTRAL_SYNC_ACK"
	AuditHeartbeat          AuditKind = "HEARTBEAT"
	AuditTransactionDone    AuditKind = "TRANSACTION_COMPLETED"
	AuditTransactionReset   AuditKind = "TRANSACTION_RESET"
	AuditHardwareProfile    AuditKind = "HARDWARE_PROFILE"
)

type AuditRef struct {
	Seq  uint64 `json:"seq"`
	Hash string `json:"hash"`
}

type AuditEvent struct {
	StationID     string         `json:"station_id"`
	TransactionID string         `json:"transaction_id,omitempty"`
	AtUTC         time.Time      `json:"at_utc"`
	Kind          AuditKind      `json:"kind"`
	Actor         string         `json:"actor,omitempty"`
	Source        string         `json:"source"`
	Device        DeviceID       `json:"device,omitempty"`
	Action        string         `json:"action,omitempty"`
	OldState      WorkflowState  `json:"old_state,omitempty"`
	NewState      WorkflowState  `json:"new_state,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	Evidence      []string       `json:"evidence,omitempty"`
	Data          map[string]any `json:"data,omitempty"`
	RuntimeGitSHA string         `json:"runtime_git_sha,omitempty"`
}
