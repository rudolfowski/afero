package s3fs

import "strings"

const (
	DefaultFileMode = 0o755
)

func NoLeadingSeparator(s string, separator string) string {
	if len(s) > 0 && strings.HasPrefix(s, separator) {
		s = s[len(separator):]
	}
	return s
}

func NormalizeSeparators(s string, separator string) string {
	// windows replace
	s = strings.ReplaceAll(s, "\\", separator)

	// unix replace
	return strings.ReplaceAll(s, "/", separator)
}

func SplitName(name string, separator string) (string, string) {
	splitName := strings.Split(name, separator)

	return splitName[0], strings.Join(splitName[1:], separator)
}

func EnsureTrailingSeparator(s string, separator string) string {
	if len(s) > 0 && !strings.HasSuffix(s, separator) {
		return s + separator
	}
	return s
}
