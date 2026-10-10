package server

import (
	"math"
	"testing"
	"time"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/address"
)

// The structured address on the wire is convenient, not trusted: it is rebuilt
// and revalidated, so a workspace that could not have been written as a URI is
// refused here rather than failing later at delivery.
func TestAddressesAreRevalidatedOnTheWayIn(t *testing.T) {
	tests := []struct {
		name  string
		in    *mandatumv1.Address
		want  string
		fails bool
	}{
		{
			name: "slack thread",
			in: &mandatumv1.Address{
				Connector: "slack",
				Workspace: "T0123",
				Path:      []string{"C0456"},
				Params:    map[string]string{"thread": "1699123456.001"},
			},
			want: "slack://T0123/C0456?thread=1699123456.001",
		},
		{
			name: "path segments needing escaping",
			in: &mandatumv1.Address{
				Connector: "github",
				Workspace: "urmzd",
				Path:      []string{"mandatum", "issues", "42"},
			},
			want: "github://urmzd/mandatum/issues/42",
		},
		{
			name:  "workspace is not token safe",
			in:    &mandatumv1.Address{Connector: "slack", Workspace: "team one"},
			fails: true,
		},
		{
			name:  "workspace carries a separator",
			in:    &mandatumv1.Address{Connector: "jira", Workspace: "acme/other"},
			fails: true,
		},
		{
			name:  "no connector",
			in:    &mandatumv1.Address{Workspace: "acme"},
			fails: true,
		},
		{
			name:  "absent",
			fails: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := addressFromProto(tt.in)
			if tt.fails {
				if err == nil {
					t.Fatalf("want %v to be refused, got %s", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.String() != tt.want {
				t.Fatalf("address: got %q, want %q", got, tt.want)
			}
			// And back out again, unchanged.
			if round := addressToProto(got); round.GetConnector() != tt.in.GetConnector() ||
				round.GetWorkspace() != tt.in.GetWorkspace() ||
				len(round.GetPath()) != len(tt.in.GetPath()) {
				t.Fatalf("round trip changed the address: %v", round)
			}
		})
	}
}

// A workspace that is safe as a token is safe in a topic and in a delivery
// subject, which is the reason the check exists at all.
func TestWorkspaceValidationIsTheAddressPackages(t *testing.T) {
	if err := address.ValidWorkspace("T0123"); err != nil {
		t.Fatalf("want a token-safe workspace to be accepted: %v", err)
	}
	if err := address.ValidWorkspace("T 0123"); err == nil {
		t.Fatal("want a workspace with a space to be refused")
	}
}

// A status the edge does not recognise reaches the wire as UNSPECIFIED, because
// the enum's zero value exists so that "unknown to me" is expressible.
func TestRunStatusMapsUnknownStatesToUnspecified(t *testing.T) {
	tests := map[Status]mandatumv1.RunStatus{
		StatusAccepted:  mandatumv1.RunStatus_RUN_STATUS_ACCEPTED,
		StatusRunning:   mandatumv1.RunStatus_RUN_STATUS_RUNNING,
		StatusParked:    mandatumv1.RunStatus_RUN_STATUS_PARKED,
		StatusCompleted: mandatumv1.RunStatus_RUN_STATUS_COMPLETED,
		StatusFailed:    mandatumv1.RunStatus_RUN_STATUS_FAILED,
		"":              mandatumv1.RunStatus_RUN_STATUS_UNSPECIFIED,
		"reticulating":  mandatumv1.RunStatus_RUN_STATUS_UNSPECIFIED,
	}
	for status, want := range tests {
		if got := statusToProto(status); got != want {
			t.Errorf("status %q: got %v, want %v", status, got, want)
		}
	}
}

// The zero time means "not yet", and protobuf's way of saying that is an absent
// message: a run that has not started must not report having started in 1970.
func TestTheZeroTimeIsAbsentOnTheWire(t *testing.T) {
	if got := timeToProto(time.Time{}); got != nil {
		t.Fatalf("want the zero time to be absent, got %v", got.AsTime())
	}
	if got := timeFromProto(nil); !got.IsZero() {
		t.Fatalf("want an absent timestamp to be the zero time, got %s", got)
	}
	local := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("elsewhere", 3600))
	if got := timeToProto(local).AsTime(); !got.Equal(local) {
		t.Fatalf("want the same instant, got %s", got)
	}
}

// A revision narrows for the wire by saturating: a number that reads as maximal
// is obviously wrong, while a wrapped negative revision looks plausible and would
// filter as one.
func TestRevisionNarrowingSaturatesRatherThanWraps(t *testing.T) {
	tests := map[int]int32{
		0:                 0,
		7:                 7,
		math.MaxInt32:     math.MaxInt32,
		math.MaxInt32 + 1: math.MaxInt32,
		math.MinInt32 - 1: math.MinInt32,
	}
	for in, want := range tests {
		if got := int32Of(in); got != want {
			t.Errorf("int32Of(%d): got %d, want %d", in, got, want)
		}
	}
}

// A spec with no name is not a spec: it names no agent, so nothing downstream
// could store or run it.
func TestASpecWithoutANameIsRefused(t *testing.T) {
	if _, err := specFromProto(nil); err == nil {
		t.Fatal("want an absent spec to be refused")
	}
	if _, err := specFromProto(&mandatumv1.AgentSpec{Description: "no name"}); err == nil {
		t.Fatal("want a nameless spec to be refused")
	}
}
