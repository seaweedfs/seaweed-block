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
						"Address": "traddr=192.168.1.181,trsvcid=4420,src_addr=192.168.1.184",
						"State":   "live",
					},
				},
			},
		},
	}
	if !nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", "192.168.1.181:4420") {
		t.Fatal("expected exact traddr/trsvcid path to match")
	}
	if nvmeSubsystemHasPath(doc, "nqn.2026-05.io.seaweedfs:v1", "192.168.1.184:4420") {
		t.Fatal("src_addr must not satisfy requested target traddr")
	}
}
