package grnci

import (
	"fmt"
	"sort"
	"testing"
)

func TestParseAddress(t *testing.T) {
	data := map[string]string{
		"": fmt.Sprintf("%s://%s:%d%s",
			DefaultScheme, DefaultHost, GQTPDefaultPort, ""),
		"gqtp://": fmt.Sprintf("%s://%s:%d%s",
			DefaultScheme, DefaultHost, GQTPDefaultPort, ""),
		"http://": fmt.Sprintf("%s://%s:%d%s",
			"http", DefaultHost, HTTPDefaultPort, HTTPDefaultPath),
		"https://": fmt.Sprintf("%s://%s:%d%s",
			"https", DefaultHost, HTTPDefaultPort, HTTPDefaultPath),
		"example.com": fmt.Sprintf("%s://%s:%d%s",
			DefaultScheme, "example.com", GQTPDefaultPort, ""),
		":8080": fmt.Sprintf("%s://%s:%d%s",
			DefaultScheme, DefaultHost, 8080, ""),
	}
	var keys []string
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, src := range keys {
		want := data[src]
		addr, err := ParseAddress(src)
		actual := addr.String()
		if err != nil {
			t.Fatalf("ParseAddress failed: src = %s, actual = %s, err = %v",
				src, actual, err)
		}
		if addr.String() != want {
			t.Fatalf("ParseAddress failed: src = %s, actual = %s, want = %s",
				src, actual, want)
		}
	}
}

func TestParseGQTPAddress(t *testing.T) {
	data := map[string]string{
		"": fmt.Sprintf("%s://%s:%d%s",
			"gqtp", DefaultHost, GQTPDefaultPort, ""),
		"gqtp://": fmt.Sprintf("%s://%s:%d%s",
			"gqtp", DefaultHost, GQTPDefaultPort, ""),
		"example.com": fmt.Sprintf("%s://%s:%d%s",
			"gqtp", "example.com", GQTPDefaultPort, ""),
		":8080": fmt.Sprintf("%s://%s:%d%s",
			"gqtp", DefaultHost, 8080, ""),
		"example.com:8080": fmt.Sprintf("%s://%s:%d%s",
			"gqtp", "example.com", 8080, ""),
	}
	var keys []string
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, src := range keys {
		want := data[src]
		addr, err := ParseGQTPAddress(src)
		actual := addr.String()
		if err != nil {
			t.Fatalf("ParseGQTPAddress failed: src = %s, actual = %s, err = %v",
				src, actual, err)
		}
		if addr.String() != want {
			t.Fatalf("ParseGQTPAddress failed: src = %s, actual = %s, want = %s",
				src, actual, want)
		}
	}
}

func TestParseHTTPAddress(t *testing.T) {
	data := map[string]string{
		"": fmt.Sprintf("%s://%s:%d%s",
			"http", DefaultHost, HTTPDefaultPort, HTTPDefaultPath),
		"https://": fmt.Sprintf("%s://%s:%d%s",
			"https", DefaultHost, HTTPDefaultPort, HTTPDefaultPath),
		"example.com": fmt.Sprintf("%s://%s:%d%s",
			"http", "example.com", HTTPDefaultPort, HTTPDefaultPath),
		":8080": fmt.Sprintf("%s://%s:%d%s",
			"http", DefaultHost, 8080, HTTPDefaultPath),
		"http://example.com": fmt.Sprintf("%s://%s:%d%s",
			"http", "example.com", HTTPDefaultPort, HTTPDefaultPath),
		"http://example.com:8080": fmt.Sprintf("%s://%s:%d%s",
			"http", "example.com", 8080, HTTPDefaultPath),
		"http://example.com:8080/": fmt.Sprintf("%s://%s:%d%s",
			"http", "example.com", 8080, "/"),
		"http://:8080": fmt.Sprintf("%s://%s:%d%s",
			"http", DefaultHost, 8080, HTTPDefaultPath),
		"http://:8080/": fmt.Sprintf("%s://%s:%d%s",
			"http", DefaultHost, 8080, "/"),
	}
	var keys []string
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, src := range keys {
		want := data[src]
		addr, err := ParseHTTPAddress(src)
		actual := addr.String()
		if err != nil {
			t.Fatalf("ParseHTTPAddress failed: src = %s, actual = %s, err = %v",
				src, actual, err)
		}
		if addr.String() != want {
			t.Fatalf("ParseHTTPAddress failed: src = %s, actual = %s, want = %s",
				src, actual, want)
		}
	}
}
