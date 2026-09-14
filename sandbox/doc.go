// Package sandbox provides environment-backed shortcuts for the common single-identity
// Sandbox workflow.
//
// Create, Connect, Get, and List delegate to ags.DefaultClient().Sandboxes(). Applications
// that use multiple Tencent Cloud identities, custom transports, or explicit endpoints should
// construct separate ags.Client values instead. Both entrances return the same ags.Sandbox
// resource and use the same control-plane, Metrics, lifecycle, and data-plane implementation.
//
// The default client reads TENCENTCLOUD_REGION, TENCENTCLOUD_SECRET_ID,
// TENCENTCLOUD_SECRET_KEY, and optional TENCENTCLOUD_TOKEN. Its configuration and provider
// binding are cached after the first successful initialization; environment variables must not
// be used to switch accounts in a running process.
package sandbox
