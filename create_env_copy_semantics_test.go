package ags

import "testing"

func TestCreateEnvCopyPreservesEmptyValuesAndOwnership(t *testing.T) {
	readOnly := false
	original := CreateOptions{
		Env:          map[string]string{"EMPTY": "", "VALUE": "before"},
		Metadata:     map[string]string{"owner": "before"},
		MountOptions: []MountOption{{Name: "data", ReadOnly: &readOnly}},
	}
	copied, err := copyCreateEnv(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Env["EMPTY"] = "changed"
	original.Env["VALUE"] = "after"
	original.Metadata["owner"] = "after"
	original.MountOptions[0].Name = "changed"
	readOnly = true
	if copied.Env["EMPTY"] != "" || copied.Env["VALUE"] != "before" {
		t.Fatalf("create Env copy changed: %#v", copied.Env)
	}
	if copied.Metadata["owner"] != "before" || copied.MountOptions[0].Name != "data" || copied.MountOptions[0].ReadOnly == nil || *copied.MountOptions[0].ReadOnly {
		t.Fatalf("Create options retained caller-owned values: %+v", copied)
	}
	withoutEnv, err := copyCreateEnv(CreateOptions{})
	if err != nil || withoutEnv.Env != nil {
		t.Fatalf("nil Env should remain absent: %#v, %v", withoutEnv.Env, err)
	}
}
