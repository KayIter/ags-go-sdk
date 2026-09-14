package main

import (
	"context"
	"fmt"
	"time"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
	"github.com/TencentCloudAgentRuntime/ags-go-sdk/sandbox"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	lifetime := 10 * time.Minute
	sb, err := sandbox.Create(ctx, sandbox.CreateOptions{
		Tool:    sandbox.ToolRef{ID: "your-tool-id"},
		Timeout: &lifetime,
	})
	if err != nil {
		return err
	}
	defer sb.Close()

	result, err := sb.Commands().Run(ctx, "sh", ags.CommandOptions{Args: []string{"-lc", "printf hello"}})
	if err != nil {
		return err
	}
	fmt.Printf("exit=%d stdout=%s\n", result.ExitCode, result.Stdout)

	cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	return sb.Delete(cleanup)
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
