# Security Policy

## Supported versions

Security fixes are applied to the latest released pre-1.0 version. Older pre-1.0 versions may
require upgrading because the public API is still converging.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability
reporting feature for this repository. Include the affected version, impact, reproduction steps,
and any proposed mitigation. Remove Tencent Cloud credentials, Sandbox instance access material,
resource identifiers, and private endpoints from the report unless maintainers explicitly request
them through an approved private channel.

Maintainers will acknowledge a complete report, assess affected versions, and coordinate a fix
and disclosure timeline. Public disclosure should wait until a fix or mitigation is available.

## Credential handling

Use environment variables or a `CredentialProvider`; do not hard-code credentials. The SDK keeps
Sandbox instance access material inside the current data-plane generation. Applications should
avoid logging request headers, custom transports, option maps containing sensitive values, or
wrapped causes without review.
