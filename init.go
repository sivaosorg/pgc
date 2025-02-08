package pgc

import (
	"strings"
	"time"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter
//_______________________________________________________________________

func (c *RConf) IsEnabled() bool {
	return c.enabled
}

func (c *RConf) IsDebugging() bool {
	return c.debugging
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

func (c *RConf) Database() string {
	return c.database
}

func (c *RConf) SSLMode() string {
	return c.sslmode
}

func (c *RConf) SSLCert() string {
	return c.sslcert
}

func (c *RConf) SSLKey() string {
	return c.sslkey
}

func (c *RConf) SSLRootCert() string {
	return c.sslrootcert
}

func (c *RConf) ConnTimeout() time.Duration {
	return c.connTimeout
}

func (c *RConf) Application() string {
	return c.application
}

func (c *RConf) MaxOpenConn() int {
	return c.maxOpenConn
}

func (c *RConf) MaxIdleConn() int {
	return c.maxIdleConn
}

func (c *RConf) ConnMaxLifetime() time.Duration {
	return c.connMaxLifetime
}

func (c *RConf) IsSSL() bool {
	return !strings.EqualFold(c.sslmode, "disable")
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

func (c *RConf) SetDatabase(value string) {
	c.database = value
}

func (c *RConf) SetSSLMode(value string) {
	c.sslmode = value
}

func (c *RConf) SetSSLCert(value string) {
	c.sslcert = value
}

func (c *RConf) SetSSLKey(value string) {
	c.sslkey = value
}

func (c *RConf) SetSSLRootCert(value string) {
	c.sslrootcert = value
}

func (c *RConf) SetConnTimeout(value time.Duration) {
	c.connTimeout = value
}

func (c *RConf) SetApplication(value string) {
	c.application = value
}

func (c *RConf) SetMaxOpenConn(value int) {
	c.maxOpenConn = value
}

func (c *RConf) SetMaxIdleConn(value int) {
	c.maxIdleConn = value
}

func (c *RConf) SetConnMaxLifetime(value time.Duration) {
	c.connMaxLifetime = value
}
