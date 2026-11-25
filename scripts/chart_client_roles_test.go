package scripts_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"text/template"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"gopkg.in/yaml.v3"
)

// clientLeastRole is the role each in-cluster client's master-tls Secret may
// carry at most: the CSI node and the status operator only read; the CSI
// controller deletes volumes and the failback executor runs failbacks.
var clientLeastRole = map[string]auth.Role{
	"sw-block-csi-node":          auth.RoleViewer,
	"sw-block-operator-status":   auth.RoleViewer,
	"sw-block-csi-controller":    auth.RoleAdmin,
	"sw-block-failback-executor": auth.RoleAdmin,
}

// Each in-cluster client mounts its own master-tls Secret, and the role file
// the certificate helper writes gives that Secret's certificate the least role
// the client needs: a CSI node (on every host) never holds an operator or
// admin certificate. The chart is rendered with its own values; the alpha
// manifests are read as they are applied.
func TestClientSecretsCarryTheLeastRole(t *testing.T) {
	root := repoRoot(t)
	vals := chartValues(t, root)
	vals["failbackExecutor"].(map[string]any)["create"] = true
	vals["operatorStatus"].(map[string]any)["create"] = true
	mounts := map[string]string{} // "where" -> Secret
	for _, c := range []struct{ tpl, app string }{
		{"csi-node.yaml", "sw-block-csi-node"},
		{"operator-status.yaml", "sw-block-operator-status"},
		{"csi-controller.yaml", "sw-block-csi-controller"},
		{"failback-executor.yaml", "sw-block-failback-executor"},
	} {
		secret := masterTLSSecret(t, renderChart(t, root, c.tpl, vals))
		if secret != c.app+"-tls" {
			t.Errorf("chart %s mounts master-tls Secret %q, want its own %s-tls", c.tpl, secret, c.app)
		}
		mounts["chart "+c.tpl] = secret
	}
	for _, d := range []string{"alpha", "g15b", "g15d"} {
		for _, c := range []struct{ file, app string }{{"csi-node.yaml", "sw-block-csi-node"}, {"csi-controller.yaml", "sw-block-csi-controller"}} {
			p := filepath.Join(root, "deploy", "k8s", d, c.file)
			b, err := os.ReadFile(p)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			secret := masterTLSSecret(t, string(b))
			if secret != c.app+"-tls" {
				t.Errorf("deploy/k8s/%s/%s mounts master-tls Secret %q, want %s-tls", d, c.file, secret, c.app)
			}
			mounts["deploy/k8s/"+d+"/"+c.file] = secret
		}
	}

	// an admin Secret (by name or values key) appears only in the CSI
	// controller's and the failback executor's manifests
	adminRefs := map[string]string{
		"sw-block-csi-controller-tls": "csi-controller.yaml", "clientSecrets.csiController": "csi-controller.yaml",
		"sw-block-failback-executor-tls": "failback-executor.yaml", "clientSecrets.failbackExecutor": "failback-executor.yaml",
	}
	for _, dir := range []string{filepath.Join(root, "charts", "seaweed-block", "templates"), filepath.Join(root, "deploy", "k8s")} {
		err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			for ref, owner := range adminRefs {
				if strings.Contains(string(b), ref) && filepath.Base(p) != owner {
					t.Errorf("%s names the admin Secret %s; only %s may", p, ref, owner)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if runtime.GOOS == "windows" {
		t.Skip("bash and openssl script")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not installed")
	}
	out := t.TempDir()
	cmd := exec.Command("bash", filepath.Join(root, "scripts", "k8s-alpha-certs.sh"), "kube-system", out, "node-a")
	cmd.Env = append(os.Environ(), "SW_BLOCK_TLS_APPLY=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("k8s-alpha-certs.sh: %v\n%s", err, b)
	}
	roles := roleFileRoles(t, filepath.Join(out, "roles"))
	a := mustAuthenticator(t, filepath.Join(out, "roles"))
	master := tlsFiles(out, "master")
	for where, secret := range mounts {
		id := strings.TrimSuffix(secret, "-tls")
		got, ok := roles[id]
		if !ok {
			t.Errorf("%s mounts %s, whose certificate (id %s) the role file does not list", where, secret, id)
			continue
		}
		if got != clientLeastRole[id] {
			t.Errorf("%s mounts %s with role %s, want %s", where, secret, got, clientLeastRole[id])
		}
		// the certificate in the Secret authenticates as that line
		p, err := handshake(t, master, tlsFiles(out, strings.TrimPrefix(id, "sw-block-")), a)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if p.Role != got || p.ID != id {
			t.Errorf("%s authenticated as %s %s, want %s %s", id, p.Role, p.ID, got, id)
		}
	}
	if len(roles) != len(clientLeastRole)+1 { // the clients and node-a
		t.Errorf("role file lists %v, want the %d clients and one node", roles, len(clientLeastRole))
	}
}

func chartValues(t *testing.T, root string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "charts", "seaweed-block", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := yaml.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// renderChart renders one chart template with its helpers, implementing the
// Helm functions the chart's client templates use.
func renderChart(t *testing.T, root, tpl string, vals map[string]any) string {
	t.Helper()
	dir := filepath.Join(root, "charts", "seaweed-block", "templates")
	tm := template.New(tpl).Option("missingkey=zero")
	tm.Funcs(template.FuncMap{
		"include": func(name string, data any) (string, error) {
			var buf bytes.Buffer
			err := tm.ExecuteTemplate(&buf, name, data)
			return buf.String(), err
		},
		"nindent": func(n int, s string) string {
			pad := strings.Repeat(" ", n)
			return "\n" + pad + strings.ReplaceAll(s, "\n", "\n"+pad)
		},
		"quote": func(v any) string { return strconv.Quote(helmString(v)) },
		"default": func(d, v any) any {
			if helmString(v) == "" {
				return d
			}
			return v
		},
		"trunc": func(n int, s string) string {
			if len(s) > n {
				return s[:n]
			}
			return s
		},
		"trimSuffix": func(suf, s string) string { return strings.TrimSuffix(s, suf) },
		"replace":    func(old, new, s string) string { return strings.ReplaceAll(s, old, new) },
		"int":        func(v any) int { i, _ := strconv.Atoi(helmString(v)); return i },
		"fail":       func(msg string) (string, error) { return "", errors.New(msg) },
		"toYaml": func(v any) string {
			b, _ := yaml.Marshal(v)
			return strings.TrimSuffix(string(b), "\n")
		},
	})
	for _, f := range []string{"_helpers.tpl", tpl} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tm.New(f).Parse(string(b)); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
	}
	data := map[string]any{
		"Values":  vals,
		"Release": map[string]any{"Name": "sw-block", "Namespace": "kube-system", "Service": "Helm"},
		"Chart":   map[string]any{"Name": "seaweed-block", "Version": "0.0.0"},
	}
	var buf bytes.Buffer
	if err := tm.ExecuteTemplate(&buf, tpl, data); err != nil {
		t.Fatalf("render %s: %v", tpl, err)
	}
	return strings.ReplaceAll(buf.String(), "<no value>", "")
}

func helmString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// masterTLSSecret is the Secret behind the pod volume named master-tls in a
// rendered manifest (one workload; other documents are skipped).
func masterTLSSecret(t *testing.T, manifest string) string {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(manifest))
	found := ""
	for {
		var doc struct {
			Spec struct {
				Template struct {
					Spec struct {
						Volumes []struct {
							Name   string `yaml:"name"`
							Secret struct {
								SecretName string `yaml:"secretName"`
							} `yaml:"secret"`
						} `yaml:"volumes"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("manifest: %v\n%s", err, manifest)
		}
		for _, v := range doc.Spec.Template.Spec.Volumes {
			if v.Name == "master-tls" {
				if found != "" {
					t.Fatalf("two master-tls volumes in one manifest")
				}
				found = v.Secret.SecretName
			}
		}
	}
	if found == "" {
		t.Fatalf("no master-tls volume in\n%s", manifest)
	}
	return found
}

// roleFileRoles maps each role-file id to its role.
func roleFileRoles(t *testing.T, path string) map[string]auth.Role {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]auth.Role{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 3 && !strings.HasPrefix(f[0], "#") {
			m[f[2]] = auth.Role(f[1])
		}
	}
	return m
}

func mustAuthenticator(t *testing.T, roleFile string) *auth.Authenticator {
	t.Helper()
	table, err := auth.LoadRoleFile(roleFile)
	if err != nil {
		t.Fatal(err)
	}
	return auth.NewAuthenticator(table, nil)
}

func tlsFiles(dir, name string) link.TLSFiles {
	return link.TLSFiles{Cert: filepath.Join(dir, name+".crt"), Key: filepath.Join(dir, name+".key"), CA: filepath.Join(dir, "ca.crt")}
}
