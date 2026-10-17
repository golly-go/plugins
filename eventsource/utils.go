package eventsource

import (
	"fmt"
	"runtime"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/golly-go/golly"
)

func capitalizeFirstCharASCII(str string) string {
	if str == "" {
		return ""
	}

	b := []byte(str)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 32
	}
	return string(b)
}

func ObjectName(object any) string {
	var s string

	switch o := object.(type) {
	case string:
		s = o
	default:
		s = golly.TypeNoPtr(object).String()
	}

	if legacy {
		return s
	}

	return ObjectPath(object)

}

func ObjectPath(object any) string {
	val := golly.TypeNoPtr(object)

	name := val.String()
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}

	return val.PkgPath() + "/" + name
}

// traceEnabled lets hot callers skip building trace arguments (boxing allocs).
func traceEnabled() bool {
	return golly.DefaultLogger().Level() == golly.LogLevelTrace
}

func trace(message string, args ...any) {
	if traceEnabled() {

		msg := fmt.Sprintf(message, args...)
		pc, file, line := fileInfo(3)
		golly.DefaultLogger().Tracef("[ES] %s caller=%s:%d %s", msg, file, line, runtime.FuncForPC(pc).Name())
	}
}

func fileInfo(skip int) (uintptr, string, int) {
	var pc uintptr
	var file string
	var line int
	var ok bool

	for x := skip; x < 10; x++ {
		pc, file, line, ok = runtime.Caller(x)
		if !ok {
			return pc, file, line
		}

		if strings.Contains(file, "golly") || strings.Contains(file, "initializer.go") || strings.Contains(file, "utils.go") {
			continue
		}
		break
	}

	return pc, file, line
}

func NameToTopicUnicode(obj any) string {

	var s string

	switch o := obj.(type) {
	case string:
		s = o
	default:
		if i, ok := obj.(interface{ Topic() string }); ok {
			s = i.Topic()
		} else {
			s = golly.InfNameNoPackage(obj)
		}
	}

	const prefix = "events."

	var arr [64]byte
	b := append(arr[:0], prefix...)
	for _, r := range s {
		switch {
		case r == '/' || r == '.':
			b = append(b, '-')
		case r < utf8.RuneSelf:
			b = append(b, byte(unicode.ToLower(r)))
		default:
			b = utf8.AppendRune(b, unicode.ToLower(r))
		}
	}
	return string(b)
}

func resolveInterfaceName(obj any) string {
	if i, ok := obj.(Projection); ok {
		return projectionKey(i)
	}

	switch o := obj.(type) {
	case string:
		return o
	default:
		return ObjectName(o)
	}
}
