package application

import (
	"github.com/jedi-knights/jk-metering/internal/domain"
)

// LagoEventCode is the single code every metering shim event lands under
// in Lago. Per identity-platform-go ADR-0019, the shim does not split
// audit events into multiple Lago event codes — all discrimination
// happens via Lago billable-metric filters on properties (event_type,
// resource_kind, resource_parent, resource_path, actor_type, etc.).
const LagoEventCode = "usage"

// BillingIdentityField selects which audit field becomes the Lago
// external_subscription_id. ADR-0019 makes this configurable per plan;
// the shim treats it as a service-wide setting today and defers
// per-plan overrides to a follow-up.
type BillingIdentityField string

const (
	// BillingIdentitySubject bills the resource-owner subject. Default per
	// ADR-0019 ("user owns the cost").
	BillingIdentitySubject BillingIdentityField = "subject"
	// BillingIdentityActor bills the immediate actor — useful when the
	// agent's operator is the customer.
	BillingIdentityActor BillingIdentityField = "actor"
	// BillingIdentityClient bills the OAuth client. Falls back to actor
	// when client_id is empty on the event.
	BillingIdentityClient BillingIdentityField = "client"
)

// Transform converts an [domain.AuditEvent] to a [domain.LagoEvent] per
// ADR-0019. The transformation is a flat property pump — every audit
// envelope field that is non-empty becomes a Lago event property, and
// the attrs map is unpacked verbatim into the properties bag. Lago's
// billable-metric filters discriminate at the operator side; the shim
// does not interpret event semantics.
//
// billingIdentity selects which field becomes external_subscription_id:
// subject (default), actor, or client. An empty / unrecognized value
// defaults to subject — the ADR-0019 "user owns the cost" rule.
func Transform(e domain.AuditEvent, billingIdentity BillingIdentityField) domain.LagoEvent {
	props := map[string]any{
		"event_type":     e.EventType,
		"service":        e.Service,
		"actor_type":     e.ActorType,
		"actor_id":       e.ActorID,
		"resource":       e.Resource,
		"action":         e.Action,
		"decision":       e.Decision,
		"schema_version": e.SchemaVersion,
	}
	addOptionalProps(props, e)
	for k, v := range e.Attrs {
		// Audit attrs win on key collisions — they are the most specific
		// signal and the metering shim respects what the emitter put there.
		props[k] = v
	}
	return domain.LagoEvent{
		TransactionID:          e.EventID,
		ExternalSubscriptionID: pickBillingIdentity(e, billingIdentity),
		Code:                   LagoEventCode,
		Timestamp:              e.Timestamp.Unix(),
		Properties:             props,
	}
}

// addOptionalProps sets each optional audit envelope field on the props
// map when non-empty. Extracted so Transform stays under the gocyclo
// cap; every entry here is a "present in envelope → forward to Lago"
// mapping with no other logic.
func addOptionalProps(props map[string]any, e domain.AuditEvent) {
	optional := []struct {
		key string
		val string
	}{
		{"subject_id", e.SubjectID},
		{"client_id", e.ClientID},
		{"resource_kind", e.ResourceKind},
		{"resource_id", e.ResourceID},
		{"resource_parent", e.ResourceParent},
		{"resource_path", e.ResourcePath},
		{"reason", e.Reason},
		{"trace_id", e.TraceID},
		{"correlation_id", e.CorrelationID},
	}
	for _, opt := range optional {
		if opt.val != "" {
			props[opt.key] = opt.val
		}
	}
}

// pickBillingIdentity resolves the field that becomes the Lago
// external_subscription_id. Falls back to subject_id and then actor_id
// when the requested field is empty — so an event without a subject (a
// client_credentials token issuance, for example) bills the actor
// rather than silently producing an unattributable event.
func pickBillingIdentity(e domain.AuditEvent, field BillingIdentityField) string {
	switch field {
	case BillingIdentityActor:
		if e.ActorID != "" {
			return e.ActorID
		}
	case BillingIdentityClient:
		if e.ClientID != "" {
			return e.ClientID
		}
	}
	// Subject is the default; fall through here for unknown values too.
	if e.SubjectID != "" {
		return e.SubjectID
	}
	if e.ClientID != "" {
		return e.ClientID
	}
	return e.ActorID
}
