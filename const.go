package pgc

import "time"

const (
	// SslmodeDisable disables SSL.
	// In this mode, SSL is not used at all, and the connection is established in plain text.
	SslmodeDisable SslmodeVarious = "disable"

	// SslmodeRequire requires SSL.
	// In this mode, the connection is encrypted using SSL, but the server's certificate is not verified.
	SslmodeRequire SslmodeVarious = "require"

	// SslmodeVerifyCa enables SSL and verifies the certificate authority (CA).
	// In this mode, the connection is encrypted, and the server's certificate is verified to be signed by a trusted CA.
	// However, the host name in the certificate is not checked.
	SslmodeVerifyCa SslmodeVarious = "verify-ca"

	// SslmodeVerifyFull enables full SSL verification.
	// In this mode, the connection is encrypted, the server's certificate is verified to be signed by a trusted CA,
	// and the host name in the certificate is also validated against the server's host name.
	SslmodeVerifyFull SslmodeVarious = "verify-full"
)

// defaultPingInterval defines the frequency at which the connection is pinged.
const defaultPingInterval = 30 * time.Second
