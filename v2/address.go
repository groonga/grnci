package grnci

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Address represents a parsed address.
// The expected address format is
// [scheme://][username[:password]@]host[:port][path][?query][#fragment].
type Address struct {
	Raw      string
	Scheme   string
	Username string
	Password string
	Host     string
	Port     int
	Path     string
	Query    string
	Fragment string
}

// String reassembles the address fields except Raw into an address string.
func (a *Address) String() string {
	var url string
	if a.Scheme != "" {
		url += a.Scheme + "://"
	}
	if a.Password != "" {
		url += a.Username + ":" + a.Password + "@"
	} else if a.Username != "" {
		url += a.Username + "@"
	}
	url += a.Host
	if a.Port != 0 {
		url += ":" + strconv.Itoa(a.Port)
	}
	url += a.Path
	if a.Query != "" {
		url += "?" + a.Query
	}
	if a.Fragment != "" {
		url += "#" + a.Fragment
	}
	return url
}

const (
	gqtpScheme      = "gqtp"
	gqtpDefaultHost = "localhost"
	gqtpDefaultPort = 10043
)

// fillGQTP checks fields and fills missing fields in a GQTP address.
func (a *Address) fillGQTP() error {
	if a.Username != "" {
		return fmt.Errorf("invalid username: raw = %s", a.Raw)
	}
	if a.Password != "" {
		return fmt.Errorf("invalid password: raw = %s", a.Raw)
	}
	if a.Host == "" {
		a.Host = gqtpDefaultHost
	}
	if a.Port == 0 {
		a.Port = gqtpDefaultPort
	}
	if a.Path != "" {
		return fmt.Errorf("invalid path: raw = %s", a.Raw)
	}
	if a.Query != "" {
		return fmt.Errorf("invalid query: raw = %s", a.Raw)
	}
	if a.Fragment != "" {
		return fmt.Errorf("invalid fragment: raw = %s", a.Raw)
	}
	return nil
}

const (
	httpScheme      = "http"
	httpsScheme     = "https"
	httpDefaultHost = "localhost"
	httpDefaultPort = 10041
	httpDefaultPath = "/d/"
)

// fillHTTP checks fields and fills missing fields in an HTTP address.
func (a *Address) fillHTTP() error {
	if a.Host == "" {
		a.Host = httpDefaultHost
	}
	if a.Port == 0 {
		a.Port = httpDefaultPort
	}
	if a.Path == "" {
		a.Path = httpDefaultPath
	}
	if a.Query != "" {
		return fmt.Errorf("invalid query: raw = %s", a.Raw)
	}
	if a.Fragment != "" {
		return fmt.Errorf("invalid fragment: raw = %s", a.Raw)
	}
	return nil
}

const (
	defaultScheme = gqtpScheme
)

// fill checks fields and fills missing fields.
func (a *Address) fill() error {
	if a.Scheme == "" {
		a.Scheme = defaultScheme
	} else {
		a.Scheme = strings.ToLower(a.Scheme)
	}
	switch a.Scheme {
	case gqtpScheme:
		if err := a.fillGQTP(); err != nil {
			return err
		}
	case httpScheme, httpsScheme:
		if err := a.fillHTTP(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid scheme: raw = %s", a.Raw)
	}
	return nil
}

// ParseAddress parses an address.
// The expected address format is
// [scheme://][username[:password]@]host[:port][path][?query][#fragment].
func ParseAddress(s string) (*Address, error) {
	a := &Address{Raw: s}
	if i := strings.IndexByte(s, '#'); i != -1 {
		a.Fragment = s[i+1:]
		s = s[:i]
	}
	if i := strings.IndexByte(s, '?'); i != -1 {
		a.Query = s[i+1:]
		s = s[:i]
	}
	if i := strings.Index(s, "://"); i != -1 {
		a.Scheme = s[:i]
		s = s[i+len("://"):]
	}
	if i := strings.IndexByte(s, '/'); i != -1 {
		a.Path = s[i:]
		s = s[:i]
	}
	if i := strings.IndexByte(s, '@'); i != -1 {
		auth := s[:i]
		if j := strings.IndexByte(auth, ':'); j != -1 {
			a.Username = auth[:j]
			a.Password = auth[j+1:]
		} else {
			a.Username = auth
			a.Password = ""
		}
		s = s[i+1:]
	}
	if s == "" {
		return a, nil
	}

	portStr := ""
	if s[0] == '[' {
		i := strings.IndexByte(s, ']')
		if i == -1 {
			return nil, fmt.Errorf("missing ']': s = %s", s)
		}
		a.Host = s[:i+1]
		rest := s[i+1:]
		if rest == "" {
			return a, nil
		}
		if rest[0] != ':' {
			return nil, fmt.Errorf("missing ':' after ']': s = %s", s)
		}
		portStr = rest[1:]
	} else {
		i := strings.LastIndexByte(s, ':')
		if i == -1 {
			a.Host = s
			return a, nil
		}
		a.Host = s[:i]
		portStr = s[i+1:]
	}
	if portStr != "" {
		port, err := net.LookupPort("tcp", portStr)
		if err != nil {
			return nil, fmt.Errorf("net.LookupPort failed: %v", err)
		}
		a.Port = port
	}

	if err := a.fill(); err != nil {
		return nil, err
	}
	return a, nil
}
