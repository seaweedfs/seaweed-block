package volume

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
)

// statusRoutes are the status routes and the type each one encodes. The
// anonymous bodies of /status/peers and /status/durable are restated here
// field for field; TestStatusBodiesMatchTheRoutes checks they still match.
var statusRoutes = []struct {
	route, note string
	typ         reflect.Type
}{
	{"/status", "the volume's authority and health projection", reflect.TypeOf(StatusProjection{})},
	{"/status/peers", "replication peers of the served volume", reflect.TypeOf(peersStatusBody{})},
	{"/status/durable", "durable store state of the served volume", reflect.TypeOf(durableStatusBody{})},
	{"/status/recovery", "the engine's replica projection (only with recovery enabled)", reflect.TypeOf(engine.ReplicaProjection{})},
	{"/status/chunk", "chunk store usage of a walstore-chunk volume (only with --chunk-control-endpoint)", reflect.TypeOf(chunkstore.VolumeUsage{})},
	{"/status/nvme", "NVMe/TCP frontend counters", reflect.TypeOf(NVMeStatus{})},
	{"/status/ublk", "ublk frontend counters", reflect.TypeOf(UblkStatus{})},
	{"/status/frontend-capabilities", "the frontends this host can serve", reflect.TypeOf(FrontendCapabilitiesStatus{})},
}

type peersStatusBody struct {
	VolumeID  string                          // the served volume
	ReplicaID string                          // this host's replica
	PeerCount int                             // number of entries in Peers
	Peers     []replication.ReplicaPeerStatus // this primary's replication peers
}

type durableStatusBody struct {
	VolumeID    string                 // the served volume
	ReplicaID   string                 // this host's replica
	VolumeCount int                    // number of entries in Volumes
	Volumes     []durable.VolumeStatus // the durable store state of the served volume
}

const (
	statusSchemaDoc   = "../../../docs/status-schema.md"
	statusSchemaBegin = "<!-- BEGIN GENERATED: go test ./core/host/volume -run TestStatusSchemaDoc (STATUS_SCHEMA_UPDATE=1 rewrites) -->"
	statusSchemaEnd   = "<!-- END GENERATED -->"
	modulePath        = "github.com/seaweedfs/seaweed-block/"
)

// fieldDocs maps "pkgpath.Type.Field" to the field's doc or line comment.
type fieldDocs map[string]string

func (d fieldDocs) load(t *testing.T, pkgPath string) {
	t.Helper()
	if !strings.HasPrefix(pkgPath, modulePath) {
		return
	}
	if _, done := d["pkg:"+pkgPath]; done {
		return
	}
	d["pkg:"+pkgPath] = ""
	dir := filepath.Join("..", "..", "..", strings.TrimPrefix(pkgPath, modulePath))
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go") || fi.Name() == "status_schema_test.go" // the restated bodies
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	for _, p := range pkgs {
		for _, f := range p.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					return true
				}
				// A doc comment above a run of fields with no blank line
				// between them describes the whole run (a group comment).
				group, groupFirst, prevEnd := "", "", -1
				for _, fl := range st.Fields.List {
					text := ""
					line := fset.Position(fl.Pos()).Line
					switch {
					case fl.Doc != nil:
						text = fl.Doc.Text()
						group, groupFirst = strings.Join(strings.Fields(text), " "), ""
						if len(fl.Names) > 0 {
							groupFirst = fl.Names[0].Name
						}
					case fl.Comment != nil:
						text = fl.Comment.Text()
					case group != "" && line == prevEnd+1:
						text = "(group of `" + groupFirst + "`) " + group
					default:
						group = ""
					}
					prevEnd = fset.Position(fl.End()).Line
					text = strings.Join(strings.Fields(text), " ")
					for _, name := range fl.Names {
						d[pkgPath+"."+ts.Name.Name+"."+name.Name] = text
					}
				}
				return true
			})
		}
	}
}

// schemaRows walks t's JSON fields; prefix is the dotted JSON path so far.
func schemaRows(t *testing.T, docs fieldDocs, typ reflect.Type, prefix string, seen map[reflect.Type]bool, rows *[]string) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return
	}
	seen[typ] = true
	defer delete(seen, typ)
	docs.load(t, typ.PkgPath())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if f.Anonymous && name == "" {
			schemaRows(t, docs, f.Type, prefix, seen, rows)
			continue
		}
		if name == "" {
			name = f.Name
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		ft, suffix := f.Type, ""
		for {
			switch ft.Kind() {
			case reflect.Pointer:
				ft = ft.Elem()
				continue
			case reflect.Slice, reflect.Array:
				if ft.Elem().Kind() != reflect.Uint8 {
					ft, suffix = ft.Elem(), suffix+"[]"
					continue
				}
			case reflect.Map:
				ft, suffix = ft.Elem(), suffix+"{}"
				continue
			}
			break
		}
		typeName := ft.Kind().String()
		if ft.Kind() == reflect.Struct {
			typeName = "object"
			if ft.PkgPath() == "time" && ft.Name() == "Time" {
				typeName = "time (RFC 3339)"
			}
		} else if ft.Name() != "" && ft.PkgPath() != "" {
			typeName = ft.Name() + " (" + ft.Kind().String() + ")"
		}
		if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
			typeName = "bytes (base64)"
		}
		desc := docs[typ.PkgPath()+"."+typ.Name()+"."+f.Name]
		if desc == "" {
			desc = "—"
		}
		if strings.Contains(opts, "omitempty") {
			desc += " (omitted when empty)"
		}
		desc = strings.ReplaceAll(desc, "|", "\\|")
		*rows = append(*rows, fmt.Sprintf("| `%s%s` | %s | %s |", path, suffix, typeName, desc))
		if ft.Kind() == reflect.Struct && !(ft.PkgPath() == "time" && ft.Name() == "Time") {
			schemaRows(t, docs, ft, path+suffix, seen, rows)
		}
	}
}

func generateStatusSchema(t *testing.T) string {
	t.Helper()
	docs := fieldDocs{}
	var b strings.Builder
	b.WriteString(statusSchemaBegin + "\n")
	for _, r := range statusRoutes {
		var rows []string
		schemaRows(t, docs, r.typ, "", map[reflect.Type]bool{}, &rows)
		fmt.Fprintf(&b, "\n### `GET %s`\n\n%s. Go type `%s`.\n\n| Field | Type | Meaning |\n|---|---|---|\n", r.route, upperFirst(r.note), r.typ.String())
		b.WriteString(strings.Join(rows, "\n") + "\n")
	}
	b.WriteString("\n" + statusSchemaEnd)
	return b.String()
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// The status schema document lists every field the status routes return,
// generated from the Go types and their field comments. A field added,
// removed or renamed without regenerating the document fails here.
func TestStatusSchemaDoc(t *testing.T) {
	want := generateStatusSchema(t)
	raw, err := os.ReadFile(statusSchemaDoc)
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.ReplaceAll(string(raw), "\r\n", "\n")
	i, j := strings.Index(doc, statusSchemaBegin), strings.Index(doc, statusSchemaEnd)
	if i < 0 || j < i {
		t.Fatalf("%s has no generated block", statusSchemaDoc)
	}
	got := doc[i : j+len(statusSchemaEnd)]
	if got == want {
		return
	}
	if os.Getenv("STATUS_SCHEMA_UPDATE") == "1" {
		if err := os.WriteFile(statusSchemaDoc, []byte(doc[:i]+want+doc[j+len(statusSchemaEnd):]), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	gl, wl := strings.Split(got, "\n"), strings.Split(want, "\n")
	for k := 0; k < len(gl) || k < len(wl); k++ {
		var g, w string
		if k < len(gl) {
			g = gl[k]
		}
		if k < len(wl) {
			w = wl[k]
		}
		if g != w {
			t.Fatalf("%s is out of date at line %d of the generated block:\n doc:  %s\n code: %s\nregenerate with STATUS_SCHEMA_UPDATE=1", statusSchemaDoc, k+1, g, w)
		}
	}
}

// The restated bodies of /status/peers and /status/durable keep the
// handlers' field names and types.
func TestStatusBodiesMatchTheRoutes(t *testing.T) {
	fields := func(v any) []string {
		var out []string
		ty := reflect.TypeOf(v)
		for i := 0; i < ty.NumField(); i++ {
			out = append(out, ty.Field(i).Name+":"+ty.Field(i).Type.String())
		}
		sort.Strings(out)
		return out
	}
	src, err := os.ReadFile("status_server.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, f := range append(fields(peersStatusBody{}), fields(durableStatusBody{})...) {
		name, typ, _ := strings.Cut(f, ":")
		typ = strings.TrimPrefix(typ, "volume.")
		if !strings.Contains(s, "\t\t"+name+" ") || !strings.Contains(s, typ) {
			t.Errorf("status_server.go no longer declares body field %s %s", name, typ)
		}
	}
}

// Every /status route the server registers is in the schema.
func TestStatusSchemaCoversEveryRoute(t *testing.T) {
	listed := map[string]bool{}
	for _, r := range statusRoutes {
		listed[r.route] = true
	}
	for _, file := range []string{"status_server.go", "chunk_control.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(src), "\n") {
			i := strings.Index(line, `HandleFunc("/status`)
			if i < 0 {
				continue
			}
			route := line[i+len(`HandleFunc("`):]
			route = route[:strings.Index(route, `"`)]
			if !listed[route] {
				t.Errorf("%s registers %s, which docs/status-schema.md does not cover", file, route)
			}
		}
	}
}
