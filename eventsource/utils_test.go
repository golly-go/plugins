package eventsource

import (
	"testing"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

type testType struct{}

func Test_capitalizeFirstCharASCII(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"", ""},
		{"a", "A"},
		{"A", "A"},
		{"abc", "Abc"},
		{"z9", "Z9"},
		{"Z9", "Z9"},
	}
	for _, tt := range tests {
		got := capitalizeFirstCharASCII(tt.in)
		if got != tt.out {
			t.Fatalf("capitalizeFirstCharASCII(%q) = %q, want %q", tt.in, got, tt.out)
		}
	}
}

func Test_ObjectPath(t *testing.T) {
	// Use a known type from this package to make expectations deterministic
	typ := golly.TypeNoPtr(Engine{})

	want := typ.PkgPath() + "/" + typ.Name()
	if got := ObjectPath(Engine{}); got != want {
		t.Fatalf("ObjectPath(Engine) = %q, want %q", got, want)
	}
}

func Test_ObjectName_LegacyAndNew(t *testing.T) {
	// Preserve original flag
	prev := legacy
	t.Cleanup(func() { legacy = prev })

	// Legacy path uses reflect.Type.String()
	legacy = true
	var x testType
	typ := golly.TypeNoPtr(x)
	wantLegacy := typ.String()
	if got := ObjectName(x); got != wantLegacy {
		t.Fatalf("legacy ObjectName = %q, want %q", got, wantLegacy)
	}

	// New path uses ObjectPath
	legacy = false
	wantNew := ObjectPath(x)
	if got := ObjectName(x); got != wantNew {
		t.Fatalf("new ObjectName = %q, want %q", got, wantNew)
	}
}

func Test_resolveName(t *testing.T) {
	// string passthrough
	if got := resolveName("orders"); got != "orders" {
		t.Fatalf("resolveName(string) = %q, want %q", got, "orders")
	}

	// non-string -> ObjectName
	prev := legacy
	legacy = false
	teardown := func() { legacy = prev }
	defer teardown()
	var v testType
	want := ObjectName(v)
	if got := resolveName(v); got != want {
		t.Fatalf("resolveName(testType) = %q, want %q", got, want)
	}
}

func Test_fileInfo(t *testing.T) {
	pc, file, line := fileInfo(0)
	if pc == 0 {
		t.Fatalf("fileInfo pc = 0")
	}
	if file == "" {
		t.Fatalf("fileInfo file is empty")
	}
	if line <= 0 {
		t.Fatalf("fileInfo line <= 0: %d", line)
	}
}

func Test_trace_NoPanic(t *testing.T) {
	// Bump to trace and ensure calling trace does not panic
	logger := golly.Logger()
	prev := logger.Level
	logger.SetLevel(logrus.TraceLevel)
	t.Cleanup(func() { logger.SetLevel(prev) })

	// Ensure no panic
	trace("testing trace %s", "message")
}
