package zsql

import (
	"fmt"
	"strings"
)

// SpecialChars special characters map.
var SpecialChars map[string]string

// Escape sql string escape for safe.
func Escape(s string) string {
	// should pre-processing of \
	s = strings.Replace(s, `\`, `\\`, -1)

	for k, v := range SpecialChars {
		s = strings.Replace(s, k, v, -1)
	}

	return s
}

// Sprintf escaping special characters in format.
func Sprintf(format string, args ...interface{}) string {
	for index, arg := range args {
		switch arg.(type) {
		case string:
			{
				s, ok := arg.(string)
				if ok {
					args[index] = Escape(s)
				}
			}
		default:
			{
				// nothing to do
			}
		}
	}

	return fmt.Sprintf(format, args...)
}

func init() {
	SpecialChars = make(map[string]string)
	SpecialChars[`'`] = `\'`
}
