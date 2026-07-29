package csi

import "testing"

func TestParseISCSIMultipathDeviceForIQN_UsesRawDeviceMembership(t *testing.T) {
	out := `mpatha (36001405f7d3f3e5a) dm-0 LIO-ORG,sw-block
size=1G features='1 queue_if_no_path' hwhandler='1 alua' wp=rw
|-+- policy='service-time 0' prio=50 status=active
| ` + "`" + `- 5:0:0:0 sdb 8:16 active ready running
` + "`" + `-+- policy='service-time 0' prio=10 status=enabled
  ` + "`" + `- 6:0:0:0 sdc 8:32 active ready running
`

	dev, paths, err := parseISCSIMultipathDeviceForIQN(out, "iqn.2026-05.io.seaweedfs:v1", map[string]struct{}{
		"sdb": {},
		"sdc": {},
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if dev != "/dev/mapper/mpatha" || paths != 2 {
		t.Fatalf("dev=%q paths=%d", dev, paths)
	}
}

func TestParseISCSIMultipathDeviceForIQN_FallsBackToIQNLine(t *testing.T) {
	out := `mpathz (iqn.2026-05.io.seaweedfs:v1) dm-1 LIO-ORG,sw-block
` + "`" + `- 7:0:0:0 sdd 8:48 active ready running
`

	dev, paths, err := parseISCSIMultipathDeviceForIQN(out, "iqn.2026-05.io.seaweedfs:v1", nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if dev != "/dev/mapper/mpathz" || paths != 1 {
		t.Fatalf("dev=%q paths=%d", dev, paths)
	}
}

func TestParseISCSIMultipathDeviceForIQN_IgnoresUnrelatedMaps(t *testing.T) {
	out := `mpatha (36001405f7d3f3e5a) dm-0 LIO-ORG,sw-block
` + "`" + `- 5:0:0:0 sdb 8:16 active ready running
`

	dev, paths, err := parseISCSIMultipathDeviceForIQN(out, "iqn.2026-05.io.seaweedfs:v1", map[string]struct{}{
		"sdc": {},
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if dev != "" || paths != 0 {
		t.Fatalf("dev=%q paths=%d want empty", dev, paths)
	}
}

func TestNVMeSubsystemHasPathRequiresTargetAddressNotSourceAddress(t *testing.T) {
	doc := map[string]any{
		"Subsystems": []any{
			map[string]any{
				"NQN": "nqn.2026-05.io.seaweedfs:v1",
				"Paths": []any{
					map[string]any{
						"Address":   "traddr=192.168.1.181,trsvcid=4420,src_addr=192.168.1.184",
						"State":     "live",
						"Transport": "tcp",
					},
				},
			},
		},
	}
	if !nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", FrontendTransportTCP, "192.168.1.181:4420") {
		t.Fatal("expected exact traddr/trsvcid path to match")
	}
	if nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", FrontendTransportTCP, "192.168.1.184:4420") {
		t.Fatal("src_addr must not satisfy requested target traddr")
	}
	if nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", FrontendTransportRDMA, "192.168.1.181:4420") {
		t.Fatal("TCP path must not satisfy requested RDMA transport")
	}
	doc["Subsystems"].([]any)[0].(map[string]any)["Paths"].([]any)[0].(map[string]any)["State"] = "reconnecting"
	if nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", FrontendTransportTCP, "192.168.1.181:4420") {
		t.Fatal("reconnecting path must not satisfy a live-path check")
	}
}

func TestNVMeSubsystemPathsReturnsAddressAndController(t *testing.T) {
	doc := map[string]any{
		"Subsystems": []any{
			map[string]any{
				"NQN": "nqn.2026-05.io.seaweedfs:v1",
				"Paths": []any{
					map[string]any{
						"Name":      "nvme2",
						"Address":   "traddr=192.168.1.181,trsvcid=4420,src_addr=192.168.1.184",
						"State":     "live",
						"Transport": "tcp",
					},
					map[string]any{
						"Controller": "/dev/nvme3",
						"Address":    "traddr=192.168.1.184,trsvcid=4520",
						"State":      "live",
						"Transport":  "rdma",
					},
				},
			},
			map[string]any{
				"NQN": "nqn.2026-05.io.seaweedfs:other",
				"Paths": []any{
					map[string]any{
						"Name":      "nvme9",
						"Address":   "traddr=192.168.1.199,trsvcid=4420",
						"State":     "live",
						"Transport": "tcp",
					},
				},
			},
		},
	}

	paths := nvmeSubsystemPaths(doc, "nqn.2026-05.io.seaweedfs:v1")
	if len(paths) != 2 {
		t.Fatalf("paths=%+v", paths)
	}
	if paths[0].Addr != "192.168.1.181:4420" || paths[0].Controller != "/dev/nvme2" || paths[0].Transport != FrontendTransportTCP {
		t.Fatalf("path[0]=%+v", paths[0])
	}
	if paths[1].Addr != "192.168.1.184:4520" || paths[1].Controller != "/dev/nvme3" || paths[1].Transport != FrontendTransportRDMA {
		t.Fatalf("path[1]=%+v", paths[1])
	}
}

func TestNVMeConnectAlreadyConnectedIsIdempotentSuccess(t *testing.T) {
	for _, out := range []string{
		"Failed to write to /dev/nvme-fabrics: Already connected\n",
		"nvme connect: already connected",
	} {
		if !nvmeConnectAlreadyConnected(out) {
			t.Fatalf("output not recognized: %q", out)
		}
	}
	if nvmeConnectAlreadyConnected("connection refused") {
		t.Fatal("unrelated connect failure must not be ignored")
	}
}
