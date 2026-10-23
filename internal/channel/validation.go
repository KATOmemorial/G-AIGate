package channel

import (
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validName(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 128
}

func validBaseURL(value string) bool {
	if len(value) == 0 || len(value) > 255 {
		return false
	}
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(value, "#") || u.Opaque != "" {
		return false
	}
	// url.Parse rejects nonnumeric ports, but accepts empty and out-of-range ones.
	if strings.HasSuffix(u.Host, ":") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}

func validRawKey(raw string) bool {
	if len(raw) < 1 || len(raw) > 484 {
		return false
	}
	for _, r := range raw {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateConfig(in UpdateChannelInput) error {
	if !validName(in.Name) || in.Provider != "openai" || !validBaseURL(in.BaseURL) ||
		in.Weight < 1 || in.Weight > 10000 || len(in.Models) < 1 || len(in.Models) > 64 {
		return errInvalidInput
	}
	seen := make(map[string]bool, len(in.Models))
	for _, mapping := range in.Models {
		if !validName(mapping.Model) || !validName(mapping.UpstreamModel) || seen[mapping.Model] {
			return errInvalidInput
		}
		seen[mapping.Model] = true
	}
	return nil
}
