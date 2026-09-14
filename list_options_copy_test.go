package ags

import "testing"

func TestListOptionsDeepCopy(t *testing.T) {
	input := SandboxListOptions{States: []SandboxState{Stopped}, Metadata: map[string][]string{"session": {"before"}}}
	copied := copyListOptions(input)
	input.States[0] = Running
	input.Metadata["session"][0] = "after"
	delete(input.Metadata, "session")
	if copied.States[0] != Stopped || copied.Metadata["session"][0] != "before" {
		t.Fatal("List options retain caller-owned collections")
	}
}
