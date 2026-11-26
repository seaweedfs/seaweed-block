package main_test

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// keepLogCopy copies a daemon's whole log at logPath into
// $SWBLOCK_TEST_LOG_DIR (unset: nothing is copied) as
// <test name>-<daemon>.log.
func keepLogCopy(testName, daemon, logPath string) error {
	dir := os.Getenv("SWBLOCK_TEST_LOG_DIR")
	if dir == "" {
		return nil
	}
	data, err := os.ReadFile(logPath)
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	if err == nil {
		name := strings.NewReplacer("/", "_", " ", "_").Replace(testName) + "-" + daemon + ".log"
		err = os.WriteFile(filepath.Join(dir, name), data, 0o644)
	}
	return err
}

// keptLogFixture is a log longer than the test output keeps (16 KB), with a
// decision line at its head and one at its tail.
func keptLogFixture(t *testing.T) (path string, data []byte) {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("head: the decision at bring-up\n")
	body := make([]byte, 40<<10)
	rand.New(rand.NewSource(1)).Read(body)
	b.Write(body)
	b.WriteString("\ntail: the last line\n")
	path = filepath.Join(t.TempDir(), "master.log")
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path, b.Bytes()
}

// sameLog reports whether kept is the whole log: every byte, head to tail.
func sameLog(kept, log []byte) bool { return bytes.Equal(kept, log) }

func TestKeptLogIsTheWholeLog(t *testing.T) {
	path, data := keptLogFixture(t)
	dir := filepath.Join(t.TempDir(), "evidence")
	t.Setenv("SWBLOCK_TEST_LOG_DIR", dir)
	if err := keepLogCopy("TestX/sub case", "master", path); err != nil {
		t.Fatal(err)
	}
	kept, err := os.ReadFile(filepath.Join(dir, "TestX_sub_case-master.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !sameLog(kept, data) {
		t.Fatalf("kept %d bytes; want the whole %d-byte log", len(kept), len(data))
	}
	// Control: the check refuses a copy that kept only the tail, or lost the
	// last byte.
	if sameLog(data[len(data)-16<<10:], data) || sameLog(data[:len(data)-1], data) {
		t.Fatal("the check accepted a truncated copy")
	}
}

func TestKeptLogUnsetDirCopiesNothing(t *testing.T) {
	path, _ := keptLogFixture(t)
	t.Setenv("SWBLOCK_TEST_LOG_DIR", "")
	cwd := t.TempDir()
	t.Chdir(cwd)
	if err := keepLogCopy("TestX", "master", path); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(cwd); len(entries) != 0 {
		t.Fatalf("with the directory unset, %d files were written", len(entries))
	}
}
