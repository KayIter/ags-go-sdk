package main

import (
	"context"
	"fmt"
	"time"

	ags "github.com/TencentCloudAgentRuntime/ags-go-sdk"
)

func run(client *ags.Client, sandboxID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sb, err := client.Sandboxes().Connect(ctx, sandboxID)
	if err != nil {
		return err
	}
	defer sb.Close()

	codeContext, err := sb.Code().CreateContext(ctx, ags.CreateCodeContextOptions{Language: "python"})
	if err != nil {
		return err
	}
	result, err := sb.Code().Run(ctx, "counter = globals().get('counter', 0) + 1; print(counter)", ags.RunCodeOptions{Context: codeContext}, ags.CodeCallbacks{
		OnStdout: func(line string) { fmt.Print(line) },
	})
	if err != nil {
		return err
	}
	if result.Error != nil {
		return fmt.Errorf("remote execution failed: %s", result.Error.Name)
	}
	return nil
}

func main() {}
