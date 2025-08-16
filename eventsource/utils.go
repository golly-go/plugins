package eventsource

import (
	"fmt"
	"runtime"
	"strings"

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
	if legacy {
		return golly.TypeNoPtr(object).String()
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

func resolveName(obj any) (name string) {
	switch o := obj.(type) {
	case string:
		name = o
	default:
		name = ObjectName(o)
	}
	return
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
