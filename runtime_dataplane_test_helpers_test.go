package ags

import (
	"net/http"
	"time"

	"github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/dataplane"
	internalruntime "github.com/TencentCloudAgentRuntime/ags-go-sdk/internal/runtime"
)

func newDataPlane(base, token string, client *http.Client) *runtimeDataPlane {
	return newDataPlaneWithTimeout(base, token, client, 30*time.Second)
}

func newDataPlaneWithTimeout(base, token string, client *http.Client, timeout time.Duration) *runtimeDataPlane {
	return newRuntimeDataPlane(internalruntime.NewGeneration(dataplane.New(base, token, client), timeout))
}
