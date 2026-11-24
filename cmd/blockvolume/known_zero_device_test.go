package main_test

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
)

// kzDevice is the only way the KNOWN-ZERO acceptance test gets a block
// device path. It reads sysfs (the same source the CSI node uses) and returns
// /dev/<ns> only when exactly one subsystem has subsysnqn == nqn (string
// equality, never a prefix), it has exactly one controller with transport tcp
// (and serial == serial when serial is not empty), and exactly one namespace.
// mkfs and mount take its result; a path given by name alone is never
// formatted.
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

var (
	kzCtrlName = regexp.MustCompile(`^nvme[0-9]+$`)
	kzNSName   = regexp.MustCompile(`^nvme[0-9]+n[0-9]+$`)
)

// kzRead and kzNames return every read error: an unreadable entry is not
// an empty one, so a partial inventory can never look like exactly one device.
func kzRead(sys fs.FS, p string) (string, error) {
	b, err := fs.ReadFile(sys, p)
	if err != nil {
		return "", fmt.Errorf("refused: incomplete sysfs inventory: %v", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func kzNames(sys fs.FS, dir string, re *regexp.Regexp) ([]string, error) {
	ents, err := fs.ReadDir(sys, dir)
	if err != nil {
		return nil, fmt.Errorf("refused: incomplete sysfs inventory: %v", err)
	}
	var out []string
	for _, e := range ents {
		if re.MatchString(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// kzResolve reads a sysfs class tree (/sys/class): nvme-subsystem/*/subsysnqn
// names the subsystem, its nvmeN entries are its controllers, and namespaces
// are nvmeXnY entries under the subsystem (native multipath) or under the
// controller (nvme/<ctrl>/), whichever the kernel uses.
func kzResolve(sys fs.FS, nqn, serial string) (kzDevice, error) {
	if !strings.HasPrefix(nqn, "nqn.") {
		return kzDevice{}, fmt.Errorf("refused: %q is not an NQN (a device path is never accepted)", nqn)
	}
	// discovery lists the class directory itself (fs.Glob drops directory
	// errors) and reads every subsystem's NQN; any failure refuses
	ents, err := fs.ReadDir(sys, "nvme-subsystem")
	if err != nil {
		return kzDevice{}, fmt.Errorf("refused: incomplete sysfs inventory: %v", err)
	}
	var subs, hits []string
	for _, e := range ents {
		d := path.Join("nvme-subsystem", e.Name())
		subs = append(subs, d)
		got, err := kzRead(sys, d+"/subsysnqn")
		if err != nil {
			return kzDevice{}, err
		}
		if got == nqn {
			hits = append(hits, d)
		}
	}
	if len(hits) != 1 {
		return kzDevice{}, fmt.Errorf("refused: %d subsystems with NQN == %s (want exactly 1; %d subsystems seen)", len(hits), nqn, len(subs))
	}
	dir := hits[0]
	ctrls, err := kzNames(sys, dir, kzCtrlName)
	if err != nil {
		return kzDevice{}, err
	}
	if len(ctrls) != 1 {
		return kzDevice{}, fmt.Errorf("refused: subsystem %s has %d controllers %v (want exactly 1)", nqn, len(ctrls), ctrls)
	}
	c := ctrls[0]
	tr, err := kzRead(sys, "nvme/"+c+"/transport")
	if err != nil {
		return kzDevice{}, err
	}
	if tr != "tcp" {
		return kzDevice{}, fmt.Errorf("refused: controller %s transport %q (want tcp)", c, tr)
	}
	got, err := kzRead(sys, "nvme/"+c+"/serial")
	if err != nil {
		return kzDevice{}, err
	}
	if serial != "" && got != serial {
		return kzDevice{}, fmt.Errorf("refused: controller %s serial %q != %q", c, got, serial)
	}
	set := map[string]bool{}
	for _, d := range []string{dir, "nvme/" + c} {
		names, err := kzNames(sys, d, kzNSName)
		if err != nil {
			return kzDevice{}, err
		}
		for _, n := range names {
			set[n] = true
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
	return kzDevice{Path: dev, Ctrl: c, NQN: nqn, Serial: serial}, nil
}

// kzSub adds one subsystem to a fake /sys/class tree. nsUnderSub puts the
// namespaces under the subsystem (native multipath), otherwise under the
// controller.
func kzSub(m fstest.MapFS, idx int, nqn, ctrl, transport, serial string, nsUnderSub bool, ns ...string) {
	d := fmt.Sprintf("nvme-subsystem/nvme-subsys%d", idx)
	m[d+"/subsysnqn"] = &fstest.MapFile{Data: []byte(nqn + "\n")}
	m[d+"/"+ctrl] = &fstest.MapFile{Mode: fs.ModeDir}
	m["nvme/"+ctrl+"/transport"] = &fstest.MapFile{Data: []byte(transport + "\n")}
	m["nvme/"+ctrl+"/serial"] = &fstest.MapFile{Data: []byte(serial + "    \n")}
	for _, n := range ns {
		if nsUnderSub {
			m[d+"/"+n] = &fstest.MapFile{Mode: fs.ModeDir}
		} else {
			m["nvme/"+ctrl+"/"+n] = &fstest.MapFile{Mode: fs.ModeDir}
		}
	}
}

func kzTree(f ...func(fstest.MapFS)) fstest.MapFS {
	m := fstest.MapFS{}
	for _, x := range f {
		x(m)
	}
	return m
}

// TestKnownZeroDeviceResolver: each refusal is the only check that can refuse
// its case (a real local disk, a longer or shorter NQN, duplicates, two
// controllers, two namespaces, a wrong serial, a path instead of an NQN).
func TestKnownZeroDeviceResolver(t *testing.T) {
	const ours = "nqn.2026-11.io.seaweedfs:kz-r1-v1"
	real := func(m fstest.MapFS) {
		kzSub(m, 0, "nqn.2014.08.org.nvmexpress:uuid:real-disk", "nvme0", "pcie", "S1", false, "nvme0n1")
	}
	one := func(ns bool) func(fstest.MapFS) {
		return func(m fstest.MapFS) { kzSub(m, 1, ours, "nvme1", "tcp", "SWB1", ns, "nvme1n1") }
	}
	cases := []struct {
		name, nqn, serial string
		sys               fstest.MapFS
		want, refusal     string
	}{
		{"exact-single-match-ns-under-subsystem", ours, "", kzTree(real, one(true)), "/dev/nvme1n1", ""},
		{"exact-single-match-ns-under-controller", ours, "", kzTree(real, one(false)), "/dev/nvme1n1", ""},
		{"serial-match", ours, "SWB1", kzTree(real, one(true)), "/dev/nvme1n1", ""},
		{"absent", ours, "", kzTree(real), "", "0 subsystems"},
		{"duplicate", ours, "", kzTree(func(m fstest.MapFS) {
			kzSub(m, 1, ours, "nvme1", "tcp", "A", true, "nvme1n1")
			kzSub(m, 2, ours, "nvme2", "tcp", "B", true, "nvme2n1")
		}), "", "2 subsystems"},
		{"longer-nqn-not-ours", ours, "", kzTree(func(m fstest.MapFS) { kzSub(m, 1, ours+"-other", "nvme1", "tcp", "A", true, "nvme1n1") }), "", "0 subsystems"},
		{"shorter-nqn-not-ours", ours, "", kzTree(func(m fstest.MapFS) {
			kzSub(m, 1, "nqn.2026-11.io.seaweedfs:kz-r1", "nvme1", "tcp", "A", true, "nvme1n1")
		}), "", "0 subsystems"},
		{"pcie-real-disk", ours, "", kzTree(func(m fstest.MapFS) { kzSub(m, 0, ours, "nvme0", "pcie", "A", true, "nvme0n1") }), "", `transport "pcie"`},
		{"two-controllers", ours, "", kzTree(one(true), func(m fstest.MapFS) {
			m["nvme-subsystem/nvme-subsys1/nvme3"] = &fstest.MapFile{Mode: fs.ModeDir}
		}), "", "2 controllers"},
		{"two-namespaces", ours, "", kzTree(func(m fstest.MapFS) { kzSub(m, 1, ours, "nvme1", "tcp", "A", true, "nvme1n1", "nvme1n2") }), "", "2 namespaces"},
		{"no-namespace", ours, "", kzTree(func(m fstest.MapFS) { kzSub(m, 1, ours, "nvme1", "tcp", "A", true) }), "", "0 namespaces"},
		{"wrong-serial", ours, "SWB9", kzTree(one(true)), "", `serial "SWB1" != "SWB9"`},
		{"literal-path", "/dev/nvme1n1", "", kzTree(func(m fstest.MapFS) { kzSub(m, 1, "/dev/nvme1n1", "nvme1", "tcp", "A", true, "nvme1n1") }), "", "is not an NQN"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := kzResolve(c.sys, c.nqn, c.serial)
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
	d, err := kzResolve(kzTree(one(true)), ours, "SWB1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := d.Identity(), "DEVICE-IDENTITY nqn="+ours+" serial=SWB1 ctrl=nvme1 transport=tcp namespace=/dev/nvme1n1 match=exact"; got != want {
		t.Fatalf("identity line\n got %s\nwant %s", got, want)
	}
}
