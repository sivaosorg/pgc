package pgc

import (
	"fmt"
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

func (c *RConf) SslMode() string {
	return c.sslmode
}

func (c *RConf) SslCert() string {
	return c.sslcert
}

func (c *RConf) SslKey() string {
	return c.sslkey
}

func (c *RConf) SslRootCert() string {
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

func (c *RConf) IsSsl() bool {
	return !strings.EqualFold(c.sslmode, "disable")
}

func (c *RConf) IsConnTimeout() bool {
	return c.connTimeout != 0
}

func (c *RConf) String(safe bool) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("host=%s ", c.host))
	builder.WriteString(fmt.Sprintf("port=%d ", c.port))
	builder.WriteString(fmt.Sprintf("user=%s ", c.user))
	if safe {
		builder.WriteString(fmt.Sprintf("password=%s ", "******"))
	} else {
		builder.WriteString(fmt.Sprintf("password=%s ", c.password))
	}
	builder.WriteString(fmt.Sprintf("dbname=%s ", c.database))
	builder.WriteString(fmt.Sprintf("sslmode=%s ", c.sslmode))
	if isNotEmpty(c.application) {
		builder.WriteString(fmt.Sprintf("application_name=%s ", c.application))
	}
	if c.IsConnTimeout() {
		builder.WriteString(fmt.Sprintf("connect_timeout=%d ", c.connTimeout))
	}
	if c.IsSsl() {
		builder.WriteString(fmt.Sprintf("sslcert=%s ", c.sslcert))
		builder.WriteString(fmt.Sprintf("sslkey=%s ", c.sslkey))
		builder.WriteString(fmt.Sprintf("sslrootcert=%s ", c.sslrootcert))
	}
	return builder.String()
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

func (c *RConf) SetSslMode(value string) {
	c.sslmode = value
}

func (c *RConf) SetSslCert(value string) {
	c.sslcert = value
}

func (c *RConf) SetSslKey(value string) {
	c.sslkey = value
}

func (c *RConf) SetSslRootCert(value string) {
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
