package ags

import (
	"context"
	"os"
)

// CredentialProvider retrieves credentials for each signed request. Implementations
// must support concurrent calls and honor cancellation. Refreshing providers own
// their caching policy; the SDK never caches the returned secret.
type CredentialProvider interface {
	// Retrieve resolves the Cloud credential pair for a signed request. Honor cancellation;
	// refreshed values must remain within one tenant.
	Retrieve(context.Context) (CloudCredential, error)
}

// TemporaryCredentialProvider optionally supplies a three-part Tencent Cloud credential for each
// signed request. Providers passed to WithCredentialProvider may implement this interface in
// addition to CredentialProvider; two-part providers continue to work.
type TemporaryCredentialProvider interface {
	CredentialProvider
	// RetrieveTemporary resolves a SecretID, SecretKey, and optional security token. Honor
	// cancellation and keep refreshed values within one tenant.
	RetrieveTemporary(context.Context) (TemporaryCloudCredential, error)
}

// TemporaryCloudCredential is a Tencent Cloud SecretID/SecretKey pair with an optional session
// token. It implements both CredentialProvider and TemporaryCredentialProvider so it can be passed
// to the existing provider options. Default formatting is redacted.
type TemporaryCloudCredential struct {
	// SecretID and SecretKey authenticate Tencent Cloud requests; both are required.
	SecretID, SecretKey string
	// Token is the optional Tencent Cloud temporary security token, not a Sandbox instance token.
	Token string
}

// Valid reports whether the required ID/key pair is nonempty; Token may be empty for long-lived
// credentials.
func (c TemporaryCloudCredential) Valid() bool { return c.SecretID != "" && c.SecretKey != "" }

// String and GoString deliberately omit all credential fields.
func (c TemporaryCloudCredential) String() string { return "TemporaryCloudCredential{REDACTED}" }

// GoString returns a redacted Go-syntax representation.
func (c TemporaryCloudCredential) GoString() string { return c.String() }

// MarshalJSON prevents accidental disclosure by encoding only a redacted marker.
func (c TemporaryCloudCredential) MarshalJSON() ([]byte, error) {
	return []byte(`"TemporaryCloudCredential[REDACTED]"`), nil
}

// MarshalText prevents text-based serializers from receiving any credential field.
func (c TemporaryCloudCredential) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// Retrieve preserves compatibility with CredentialProvider while the optional interface carries
// the token to SDK transports.
func (c TemporaryCloudCredential) Retrieve(ctx context.Context) (CloudCredential, error) {
	if err := ctx.Err(); err != nil {
		return CloudCredential{}, err
	}
	return CloudCredential{SecretID: c.SecretID, SecretKey: c.SecretKey}, nil
}

// RetrieveTemporary returns the complete temporary credential after checking cancellation.
func (c TemporaryCloudCredential) RetrieveTemporary(ctx context.Context) (TemporaryCloudCredential, error) {
	if err := ctx.Err(); err != nil {
		return TemporaryCloudCredential{}, err
	}
	return c, nil
}

// Retrieve implements CredentialProvider for an explicitly supplied static key pair.
func (c CloudCredential) Retrieve(ctx context.Context) (CloudCredential, error) {
	if err := ctx.Err(); err != nil {
		return CloudCredential{}, err
	}
	return c, nil
}

// EnvironmentCredentials reads TENCENTCLOUD_SECRET_ID, TENCENTCLOUD_SECRET_KEY, and the optional
// TENCENTCLOUD_TOKEN for each request. Environment variables must not be changed concurrently by
// the caller.
type EnvironmentCredentials struct{}

// Retrieve reads the current environment pair and checks cancellation. Presence is validated
// before signed requests; this method itself makes no network call.
func (EnvironmentCredentials) Retrieve(ctx context.Context) (CloudCredential, error) {
	value, err := (EnvironmentCredentials{}).RetrieveTemporary(ctx)
	return CloudCredential{SecretID: value.SecretID, SecretKey: value.SecretKey}, err
}

// RetrieveTemporary reads the complete current environment credential without making a network
// request.
func (EnvironmentCredentials) RetrieveTemporary(ctx context.Context) (TemporaryCloudCredential, error) {
	return (TemporaryCloudCredential{
		SecretID:  os.Getenv("TENCENTCLOUD_SECRET_ID"),
		SecretKey: os.Getenv("TENCENTCLOUD_SECRET_KEY"),
		Token:     os.Getenv("TENCENTCLOUD_TOKEN"),
	}).RetrieveTemporary(ctx)
}

func retrieveCloudCredential(ctx context.Context, provider CredentialProvider) (TemporaryCloudCredential, error) {
	if temporary, ok := provider.(TemporaryCredentialProvider); ok {
		return temporary.RetrieveTemporary(ctx)
	}
	value, err := provider.Retrieve(ctx)
	return TemporaryCloudCredential{SecretID: value.SecretID, SecretKey: value.SecretKey}, err
}
