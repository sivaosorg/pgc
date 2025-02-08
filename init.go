package pgc

import "time"

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter
//_______________________________________________________________________

func (c *RConf) IsEnabled() bool {
	return c.enabled
}

func (c *RConf) IsDebugging() bool {
	return c.debugging
}

func (c *RConf) Database() string {
	return c.database
}

func (c *RConf) Host() string {
	return c.host
}

func (c *RConf) Port() int {
	return c.port
}

func (c *RConf) User() string {
	return c.user
}

func (c *RConf) SSLMode() string {
	return c.sslMode
}

func (c *RConf) MaxOpenConn() int {
	return c.maxOpenConn
}

func (c *RConf) MaxIdleConn() int {
	return c.maxIdleConn
}

func (c *RConf) Timeout() time.Duration {
	return c.timeout
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Setter
//_______________________________________________________________________

func (c *RConf) SetEnable(value bool) {
	c.enabled = value
}

func (c *RConf) SetDebug(value bool) {
	c.debugging = value
}

func (c *RConf) SetDatabase(value string) {
	c.database = value
}

func (c *RConf) SetHost(value string) {
	c.host = value
}

func (c *RConf) SetPort(value int) {
	c.port = value
}

func (c *RConf) SetUser(value string) {
	c.user = value
}

func (c *RConf) SetPassword(value string) {
	c.password = value
}

func (c *RConf) SetSSLMode(value string) {
	c.sslMode = value
}

func (c *RConf) SetMaxOpenConn(value int) {
	c.maxOpenConn = value
}

func (c *RConf) SetMaxIdleConn(value int) {
	c.maxIdleConn = value
}

func (c *RConf) SetTimeout(value time.Duration) {
	c.timeout = value
}
