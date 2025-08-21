package eventsource

import (
	"fmt"
	"runtime"
	"strings"
	"unicode"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
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

	pieces := strings.Split(val.String(), ".")

	return fmt.Sprintf("%s/%s", val.PkgPath(), pieces[len(pieces)-1])
}

func trace(message string, args ...any) {
	if golly.Logger().Level == logrus.TraceLevel {

		msg := fmt.Sprintf(message, args...)
		pc, file, line := fileInfo(3)
		golly.Logger().Tracef("[ES] %s caller=%s:%d %s", msg, file, line, runtime.FuncForPC(pc).Name())
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
		s = golly.InfNameNoPackage(obj)
	}

	var b strings.Builder
	b.Grow(len(s)) // lower bound; may grow if case fold expands
	for _, r := range s {
		switch r {
		case '/':
			b.WriteByte('-')
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return "events." + b.String()
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
