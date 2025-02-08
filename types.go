package pgc

import "time"

type WConf struct {
}

type RConf struct {
	enabled     bool
	debugging   bool
	database    string
	host        string
	port        int
	user        string
	password    string
	sslMode     string
	maxOpenConn int
	maxIdleConn int
	timeout     time.Duration
}
