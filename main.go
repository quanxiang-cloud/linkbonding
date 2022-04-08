package main

import (
	"flag"
	"strings"
)

var (
	elasticHost []string
)

func main() {
	flag.Parse()
}

func dflag() {
	var hosts string
	flag.StringVar(&hosts, "elastic-host", "", "elastic host.")
	for _, host := range strings.Split(hosts, ",") {
		if !strings.HasPrefix(host, "http") {
			host = "http://" + host
		}
		elasticHost = append(elasticHost, host)
	}
}
