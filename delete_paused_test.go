package ags

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type pausedDeleteControl struct {
	metricControl
	gets, resumes, deletes atomic.Int32
}

func (c *pausedDeleteControl) Get(context.Context, string) (SandboxInfo, error) {
	c.gets.Add(1)
	return SandboxInfo{ID: "fixture", State: Paused}, nil
}

func (c *pausedDeleteControl) Resume(context.Context, string, ResumeOptions) (SandboxInfo, error) {
	c.resumes.Add(1)
	return SandboxInfo{ID: "fixture", State: Running}, nil
}

func (c *pausedDeleteControl) Delete(context.Context, string) error {
	c.deletes.Add(1)
	return nil
}

func TestDeleteResumesPausedSandboxBeforeStopping(t *testing.T) {
	control := &pausedDeleteControl{}
	client, err := NewClient(WithRegion("test"), withControlPlane(control))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Sandboxes().Delete(ctx, "fixture"); err != nil {
		t.Fatal(err)
	}
	if control.gets.Load() != 1 || control.resumes.Load() != 1 || control.deletes.Load() != 1 {
		t.Fatalf("delete sequence get/resume/delete = %d/%d/%d", control.gets.Load(), control.resumes.Load(), control.deletes.Load())
	}
}
