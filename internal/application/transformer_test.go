package application_test

import (
	"testing"
	"time"

	"github.com/jedi-knights/jk-metering/internal/application"
	"github.com/jedi-knights/jk-metering/internal/domain"
)

func sampleEvent() domain.AuditEvent {
	return domain.AuditEvent{
		SchemaVersion:  "1.0",
		EventID:        "01J7M3X9TEST0000000000000",
		EventType:      "tool_invoked",
		Timestamp:      time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC),
		Service:        "jk-mcp-nwsl",
		ActorType:      "agent",
		ActorID:        "agent-claude",
		SubjectID:      "user-omar",
		ClientID:       "agent-claude",
		Resource:       "tool:get_standings",
		ResourceKind:   "tool",
		ResourceID:     "get_standings",
		ResourceParent: "jk-mcp-nwsl",
		ResourcePath:   "jk-mcp-nwsl/tool/get_standings",
		Action:         "invoke",
		Decision:       "allow",
		Attrs: map[string]any{
			"duration_ms": 142,
		},
	}
}

func TestTransform_SingleCode(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentitySubject)
	if got.Code != application.LagoEventCode {
		t.Errorf("code = %q, want %q", got.Code, application.LagoEventCode)
	}
}

func TestTransform_TransactionIDIsEventID(t *testing.T) {
	e := sampleEvent()
	got := application.Transform(e, application.BillingIdentitySubject)
	if got.TransactionID != e.EventID {
		t.Errorf("transaction_id = %q, want %q", got.TransactionID, e.EventID)
	}
}

func TestTransform_BillingIdentitySubjectDefault(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentitySubject)
	if got.ExternalSubscriptionID != "user-omar" {
		t.Errorf("external_subscription_id = %q, want user-omar", got.ExternalSubscriptionID)
	}
}

func TestTransform_BillingIdentityActor(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentityActor)
	if got.ExternalSubscriptionID != "agent-claude" {
		t.Errorf("external_subscription_id = %q, want agent-claude", got.ExternalSubscriptionID)
	}
}

func TestTransform_BillingIdentityClient(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentityClient)
	if got.ExternalSubscriptionID != "agent-claude" {
		t.Errorf("external_subscription_id = %q, want agent-claude (client_id)", got.ExternalSubscriptionID)
	}
}

func TestTransform_SubjectFallback(t *testing.T) {
	// When the requested field is empty, fall through to subject, then
	// client, then actor — so an unattributable event still produces a
	// non-empty identifier whenever any candidate is present.
	e := sampleEvent()
	e.SubjectID = ""
	got := application.Transform(e, application.BillingIdentitySubject)
	if got.ExternalSubscriptionID != "agent-claude" {
		t.Errorf("expected fallback to client_id, got %q", got.ExternalSubscriptionID)
	}
}

func TestTransform_AllIdentitiesEmpty(t *testing.T) {
	e := sampleEvent()
	e.SubjectID = ""
	e.ClientID = ""
	e.ActorID = ""
	got := application.Transform(e, application.BillingIdentitySubject)
	if got.ExternalSubscriptionID != "" {
		t.Errorf("expected empty external_subscription_id, got %q", got.ExternalSubscriptionID)
	}
}

func TestTransform_AttrsForwardedVerbatim(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentitySubject)
	if got.Properties["duration_ms"] != 142 {
		t.Errorf("expected attrs.duration_ms forwarded, got %v", got.Properties["duration_ms"])
	}
}

func TestTransform_TaxonomyForwarded(t *testing.T) {
	got := application.Transform(sampleEvent(), application.BillingIdentitySubject)
	want := map[string]string{
		"event_type":      "tool_invoked",
		"resource_kind":   "tool",
		"resource_parent": "jk-mcp-nwsl",
		"resource_path":   "jk-mcp-nwsl/tool/get_standings",
		"actor_type":      "agent",
	}
	for k, v := range want {
		got, _ := got.Properties[k].(string)
		if got != v {
			t.Errorf("properties[%q] = %q, want %q", k, got, v)
		}
	}
}

func TestTransform_AttrsWinOnCollision(t *testing.T) {
	// Attrs are the most specific signal; on a key collision the attr
	// value wins. This protects emitter-set values from being clobbered
	// by the generic envelope spread.
	e := sampleEvent()
	e.Attrs["resource_kind"] = "agent_override"
	got := application.Transform(e, application.BillingIdentitySubject)
	if got.Properties["resource_kind"] != "agent_override" {
		t.Errorf("resource_kind = %v, want agent_override", got.Properties["resource_kind"])
	}
}
