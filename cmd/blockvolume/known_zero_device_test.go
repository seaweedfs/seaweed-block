package main_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// kzDevice is the only way the KNOWN-ZERO acceptance test gets a block
// device path. It reads `nvme list-subsys -o json` and returns /dev/<ns> only
// when exactly one subsystem has subsystem NQN == nqn (string equality, never a
// prefix), it has exactly one controller with transport tcp (and serial ==
// serial when serial is not empty), and exactly one namespace. mkfs and mount
// take its result; a path given by name alone is never formatted.
type kzDevice struct {
	Path, Ctrl, NQN, Serial string
}

// Identity is the evidence line recorded before any mkfs.
func (d kzDevice) Identity() string {
	s := d.Serial
	if s == "" {
		s = "-"
	}
	return fmt.Sprintf("DEVICE-IDENTITY nqn=%s serial=%s ctrl=%s transport=tcp namespace=%s match=exact", d.NQN, s, d.Ctrl, d.Path)
}

type kzNS struct {
	NameSpace string `json:"NameSpace"`
}

type kzPath struct {
	Name       string `json:"Name"`
	Transport  string `json:"Transport"`
	Serial     string `json:"Serial"`
	Namespaces []kzNS `json:"Namespaces"`
}

type kzSubsys struct {
	NQN         string   `json:"NQN"`
	Serial      string   `json:"Serial"`
	Paths       []kzPath `json:"Paths"`
	Controllers []kzPath `json:"Controllers"`
	Namespaces  []kzNS   `json:"Namespaces"`
}

type kzRoot struct {
	Subsystems []kzSubsys `json:"Subsystems"`
}

// kzResolve parses one `nvme list-subsys -o json` document (nvme-cli 2.x
// prints a list of host roots, older versions one object).
func kzResolve(doc []byte, nqn, serial string) (kzDevice, error) {
	if !strings.HasPrefix(nqn, "nqn.") {
		return kzDevice{}, fmt.Errorf("refused: %q is not an NQN (a device path is never accepted)", nqn)
	}
	var roots []kzRoot
	if err := json.Unmarshal(doc, &roots); err != nil {
		var one kzRoot
		if err2 := json.Unmarshal(doc, &one); err2 != nil {
			return kzDevice{}, fmt.Errorf("refused: unreadable list-subsys: %v", err)
		}
		roots = []kzRoot{one}
	}
	var hits []kzSubsys
	for _, r := range roots {
		for _, s := range r.Subsystems {
			if s.NQN == nqn {
				hits = append(hits, s)
			}
		}
	}
	if len(hits) != 1 {
		return kzDevice{}, fmt.Errorf("refused: %d subsystems with NQN == %s (want exactly 1)", len(hits), nqn)
	}
	s := hits[0]
	paths := s.Paths
	if len(paths) == 0 {
		paths = s.Controllers
	}
	if len(paths) != 1 {
		return kzDevice{}, fmt.Errorf("refused: subsystem %s has %d controllers (want exactly 1)", nqn, len(paths))
	}
	p := paths[0]
	if p.Transport != "tcp" {
		return kzDevice{}, fmt.Errorf("refused: controller %s transport %q (want tcp)", p.Name, p.Transport)
	}
	got := p.Serial
	if got == "" {
		got = s.Serial
	}
	if serial != "" && got != serial {
		return kzDevice{}, fmt.Errorf("refused: controller %s serial %q != %q", p.Name, got, serial)
	}
	set := map[string]bool{}
	for _, n := range append(append([]kzNS{}, s.Namespaces...), p.Namespaces...) {
		if n.NameSpace != "" {
			set[n.NameSpace] = true
		}
	}
	if len(set) != 1 {
		ns := make([]string, 0, len(set))
		for n := range set {
			ns = append(ns, n)
		}
		sort.Strings(ns)
		return kzDevice{}, fmt.Errorf("refused: subsystem %s has %d namespaces %v (want exactly 1)", nqn, len(set), ns)
	}
	var dev string
	for n := range set {
		dev = "/dev/" + n
	}
	return kzDevice{Path: dev, Ctrl: p.Name, NQN: nqn, Serial: serial}, nil
}

func kzSubJSON(nqn, ctrl, transport, serial string, ns ...string) string {
	var b strings.Builder
	for i, n := range ns {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"NameSpace":%q}`, n)
	}
	return fmt.Sprintf(`{"NQN":%q,"Serial":%q,"Paths":[{"Name":%q,"Transport":%q,"Serial":%q,"Namespaces":[%s]}]}`, nqn, serial, ctrl, transport, serial, b.String())
}

func kzDocJSON(subs ...string) []byte {
	return []byte(`[{"HostNQN":"nqn.host","Subsystems":[` + strings.Join(subs, ",") + `]}]`)
}

// TestKnownZeroDeviceResolver: each refusal is the only check that can refuse
// its case (a real local disk, a longer or shorter NQN, duplicates, two
// namespaces, a wrong serial, a path instead of an NQN).
func TestKnownZeroDeviceResolver(t *testing.T) {
	const ours = "nqn.2026-11.io.seaweedfs:kz-r1-v1"
	real := kzSubJSON("nqn.2014.08.org.nvmexpress:uuid:real-disk", "nvme0", "pcie", "S1", "nvme0n1")
	cases := []struct {
		name, nqn, serial string
		doc               []byte
		want, refusal     string
	}{
		{"exact-single-match", ours, "", kzDocJSON(real, kzSubJSON(ours, "nvme1", "tcp", "SWB1", "nvme1n1")), "/dev/nvme1n1", ""},
		{"serial-match", ours, "SWB1", kzDocJSON(real, kzSubJSON(ours, "nvme1", "tcp", "SWB1", "nvme1n1")), "/dev/nvme1n1", ""},
		{"single-object-shape", ours, "", []byte(`{"Subsystems":[` + kzSubJSON(ours, "nvme1", "tcp", "SWB1", "nvme1n1") + `]}`), "/dev/nvme1n1", ""},
		{"absent", ours, "", kzDocJSON(real), "", "0 subsystems"},
		{"duplicate", ours, "", kzDocJSON(kzSubJSON(ours, "nvme1", "tcp", "A", "nvme1n1"), kzSubJSON(ours, "nvme2", "tcp", "B", "nvme2n1")), "", "2 subsystems"},
		{"longer-nqn-not-ours", ours, "", kzDocJSON(kzSubJSON(ours+"-other", "nvme1", "tcp", "A", "nvme1n1")), "", "0 subsystems"},
		{"shorter-nqn-not-ours", ours, "", kzDocJSON(kzSubJSON("nqn.2026-11.io.seaweedfs:kz-r1", "nvme1", "tcp", "A", "nvme1n1")), "", "0 subsystems"},
		{"pcie-real-disk", ours, "", kzDocJSON(kzSubJSON(ours, "nvme0", "pcie", "A", "nvme0n1")), "", `transport "pcie"`},
		{"two-namespaces", ours, "", kzDocJSON(kzSubJSON(ours, "nvme1", "tcp", "A", "nvme1n1", "nvme1n2")), "", "2 namespaces"},
		{"wrong-serial", ours, "SWB9", kzDocJSON(kzSubJSON(ours, "nvme1", "tcp", "SWB1", "nvme1n1")), "", `serial "SWB1" != "SWB9"`},
		{"literal-path", "/dev/nvme1n1", "", kzDocJSON(kzSubJSON(ours, "nvme1", "tcp", "A", "nvme1n1")), "", "is not an NQN"},
		{"unreadable", ours, "", []byte("not json"), "", "unreadable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := kzResolve(c.doc, c.nqn, c.serial)
			if c.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), c.refusal) {
					t.Fatalf("want refusal containing %q, got path %q err %v", c.refusal, d.Path, err)
				}
				return
			}
			if err != nil || d.Path != c.want {
				t.Fatalf("want %s, got %q err %v", c.want, d.Path, err)
			}
		})
	}
	d, err := kzResolve(kzDocJSON(kzSubJSON(ours, "nvme1", "tcp", "SWB1", "nvme1n1")), ours, "SWB1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := d.Identity(), "DEVICE-IDENTITY nqn="+ours+" serial=SWB1 ctrl=nvme1 transport=tcp namespace=/dev/nvme1n1 match=exact"; got != want {
		t.Fatalf("identity line\n got %s\nwant %s", got, want)
	}
}
