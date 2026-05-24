package ops

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservationDashboardHandlerServesReadOnlyReport(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}

	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()

	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, text)
	}
	for _, want := range []string{
		"sw-block read-only status",
		"Managed Volume Conditions",
		"pvc-healthy",
		"This report is observation-only",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("dashboard missing %q:\n%s", want, text)
		}
	}
}

func TestObservationDashboardHandlerServesMachineArtifacts(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	cluster.Volumes = []VolumeEvidence{healthyObservationVolume()}
	cluster.Events = []ClusterEvent{{
		EventID:   "master-1",
		EventTime: time.Date(2026, 5, 21, 12, 0, 1, 0, time.UTC),
		Type:      "placement_verified",
		Severity:  "info",
		Message:   "placement verified",
	}}

	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()

	assertEndpointContains(t, server.URL+"/cluster-evidence.json", `"managed_volumes"`)
	assertEndpointContains(t, server.URL+"/timeline.jsonl", `"event_id":"master-1"`)
	assertEndpointContains(t, server.URL+"/summary.txt", "managed_volume_condition=Ready")
	assertEndpointContains(t, server.URL+"/operator-snapshot.json", `"read_only": true`)
	assertEndpointContains(t, server.URL+"/operator-snapshot.json", `"mutation_allowed": false`)
}

func TestObservationDashboardHandlerRejectsMutationMethods(t *testing.T) {
	cluster := NewClusterEvidence(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	server := httptest.NewServer(NewObservationDashboardHandler(cluster))
	defer server.Close()

	resp, err := http.Post(server.URL+"/", "application/json", strings.NewReader(`{"action":"promote"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "read-only dashboard") {
		t.Fatalf("missing read-only rejection: %s", body)
	}
}

func assertEndpointContains(t *testing.T, url, want string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", url, resp.StatusCode, body)
	}
	if !strings.Contains(string(body), want) {
		t.Fatalf("%s missing %q:\n%s", url, want, body)
	}
}
