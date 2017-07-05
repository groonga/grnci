package grnci

import (
	"net"
	"strconv"
	"strings"
)

// Address is a parsed address.
// The expected address format is
// [scheme://][username[:password]@][host][:port][path][?query][#fragment].
type Address struct {
	Scheme   string
	Username string
	Password string
	Host     string
	Port     int
	Path     string
	Query    string
	Fragment string
}

// Address default settings.
const (
	// DefaultScheme is used to fill Address.Scheme if the scheme part is empty.
	DefaultScheme = "gqtp"
	// DefaultHost is used to fill Address.Host if the host part is empty.
	DefaultHost = "localhost"
	// DefaultGQTPPort is used to fill Address.Port if the scheme is GQTP and the port part is empty.
	DefaultGQTPPort = 10043
	// HTTPDefaultPort is used to fill Address.Port if the scheme is HTTP and the port part is empty.
	DefaultHTTPPort = 10041
	// HTTPDefaultPath is used to fill Address.Path if the scheme is HTTP and the path part is empty.
	DefaultHTTPPath = "/d/"
)

// fillGQTP fills missing fields in a GQTP address.
func (a *Address) fillGQTP() error {
	if a.Scheme == "" {
		a.Scheme = "gqtp"
	}
	if a.Username != "" {
		return NewError(InvalidAddress, map[string]interface{}{
			"username": a.Username,
			"error":    "GQTP does not accept username.",
		})
	}
	if a.Password != "" {
		return NewError(InvalidAddress, map[string]interface{}{
			"password": a.Password,
			"error":    "GQTP does not accept password.",
		})
	}
	if a.Host == "" {
		a.Host = DefaultHost
	}
	if a.Port == 0 {
		a.Port = DefaultGQTPPort
	}
	if a.Path != "" {
		return NewError(InvalidAddress, map[string]interface{}{
			"path":  a.Path,
			"error": "GQTP does not accept path.",
		})
	}
	if a.Query != "" {
		return NewError(InvalidAddress, map[string]interface{}{
			"query": a.Query,
			"error": "GQTP does not accept query.",
		})
	}
	if a.Fragment != "" {
		return NewError(InvalidAddress, map[string]interface{}{
			"fragment": a.Fragment,
			"error":    "GQTP does not accept fragment.",
		})
	}
	return nil
}

// fillHTTP fills missing fields in an HTTP address.
func (a *Address) fillHTTP() error {
	if a.Scheme == "" {
		a.Scheme = "http"
	}
	if a.Host == "" {
		a.Host = DefaultHost
	}
	if a.Port == 0 {
		a.Port = DefaultHTTPPort
	}
	if a.Path == "" {
		a.Path = DefaultHTTPPath
	}
	return nil
}

// fill fills missing fields.
func (a *Address) fill() error {
	if a.Scheme == "" {
		a.Scheme = DefaultScheme
	}
	switch strings.ToLower(a.Scheme) {
	case "gqtp":
		if err := a.fillGQTP(); err != nil {
			return err
		}
	case "http", "https":
		if err := a.fillHTTP(); err != nil {
			return err
		}
	default:
		return NewError(InvalidAddress, map[string]interface{}{
			"scheme": a.Scheme,
			"error":  "The scheme is not supported.",
		})
	}
	return nil
}

// parseHostPort parses the host and port.
func (a *Address) parseHostPort(s string) error {
	if s == "" {
		return nil
	}
	portStr := ""
	if s[0] == '[' {
		i := strings.IndexByte(s, ']')
		if i == -1 {
			return NewError(InvalidAddress, map[string]interface{}{
				"address": s,
				"error":   "IPv6 address must be enclosed in [].",
			})
		}
		a.Host = s[:i+1]
		rest := s[i+1:]
		if rest == "" {
			return nil
		}
		if rest[0] != ':' {
			return NewError(InvalidAddress, map[string]interface{}{
				"address": s,
				"error":   "IPv6 address and port must be separated by ':'.",
			})
		}
		portStr = rest[1:]
	} else {
		i := strings.LastIndexByte(s, ':')
		if i == -1 {
			a.Host = s
			return nil
		}
		a.Host = s[:i]
		portStr = s[i+1:]
	}
	if portStr != "" {
		port, err := net.LookupPort("tcp", portStr)
		if err != nil {
			return NewError(InvalidAddress, map[string]interface{}{
				"port":  portStr,
				"error": err.Error(),
			})
		}
		a.Port = port
	}
	return nil
}

// parseAddress parses an address.
// The expected address format is
// [scheme://][username[:password]@][host][:port][path][?query][#fragment].
func parseAddress(s string) (*Address, error) {
	a := new(Address)
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
	if err := a.parseHostPort(s); err != nil {
		return nil, err
	}
	return a, nil
}

// ParseAddress parses an address.
// The expected address format is
// [scheme://][username[:password]@][host][:port][path][?query][#fragment].
//
// If the scheme part is empty, it is filled with DefaultScheme.
// If the host part is empty, it is filled with DefaultHost.
// If the port part is empty, it is filled with DefaultGQTPPort or
// DefaultHTTPPort according to the scheme.
// If the scheme is HTTP or HTTPS and the path part is empty,
// it is filled with DefaultHTTPPath.
func ParseAddress(s string) (*Address, error) {
	a, err := parseAddress(s)
	if err != nil {
		return nil, err
	}
	if err := a.fill(); err != nil {
		return nil, err
	}
	return a, nil
}

// ParseGQTPAddress parses a GQTP address.
// The expected address format is [scheme://][host][:port].
//
// If the scheme part is empty, it is filled with "gqtp".
// If the host part is empty, it is filled with DefaultHost.
// If the port part is empty, it is filled with DefaultGQTPPort.
func ParseGQTPAddress(s string) (*Address, error) {
	a, err := parseAddress(s)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(a.Scheme) {
	case "", "gqtp":
	default:
		return nil, NewError(InvalidAddress, map[string]interface{}{
			"scheme": a.Scheme,
			"error":  "The scheme is not supported.",
		})
	}
	if err := a.fillGQTP(); err != nil {
		return nil, err
	}
	return a, nil
}

// ParseHTTPAddress parses an HTTP or HTTPS address.
// The expected address format is
// [scheme://][username[:password]@][host][:port][path][?query][#fragment].
//
// If the scheme part is empty, it is filled with "http".
// If the host part is empty, it is filled with DefaultHost.
// If the port part is empty, it is filled with DefaultHTTPPort.
// If the path part is empty, it is filled with DefaultHTTPPath.
func ParseHTTPAddress(s string) (*Address, error) {
	a, err := parseAddress(s)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(a.Scheme) {
	case "", "http", "https":
	default:
		return nil, NewError(InvalidAddress, map[string]interface{}{
			"scheme": a.Scheme,
			"error":  "The scheme is not supported.",
		})
	}
	if err := a.fillHTTP(); err != nil {
		return nil, err
	}
	return a, nil
}

// String assembles the fields into an address string.
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
