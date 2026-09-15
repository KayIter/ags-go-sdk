package controlplane

// ActionRoute is the compile-time transport choice for an approved public Cloud action.
type ActionRoute string

const (
	// OfficialTyped binds an action directly to the generated Tencent Cloud client method.
	OfficialTyped ActionRoute = "official_typed"
	// CommonRaw is reserved for an allowlisted effective-API action that the official SDK has
	// not generated yet. No action currently uses this route and it is never a failure fallback.
	CommonRaw ActionRoute = "common_raw"
)

var actionRoutes = map[string]ActionRoute{
	"StartSandboxInstance":        OfficialTyped,
	"DescribeSandboxInstanceList": OfficialTyped,
	"AcquireSandboxInstanceToken": OfficialTyped,
	"PauseSandboxInstance":        OfficialTyped,
	"ResumeSandboxInstance":       OfficialTyped,
	"StopSandboxInstance":         OfficialTyped,
	"UpdateSandboxInstance":       OfficialTyped,
}

// ActionRoutes returns a copy of the approved static action registry. The package exposes no
// arbitrary action invocation and never changes route after a request failure.
func ActionRoutes() map[string]ActionRoute {
	out := make(map[string]ActionRoute, len(actionRoutes))
	for action, route := range actionRoutes {
		out[action] = route
	}
	return out
}
