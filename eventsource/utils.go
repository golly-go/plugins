package eventsource

import (
	"fmt"
	"strings"

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
