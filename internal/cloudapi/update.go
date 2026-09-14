package cloudapi

import (
	"context"
	"errors"
	"sort"

	ags "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ags/v20250920"
)

type UpdateInput struct {
	InstanceID, Timeout string
	Metadata            map[string]string
}

func (a *AGS) Update(ctx context.Context, in UpdateInput) error {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	req := ags.NewUpdateSandboxInstanceRequest()
	req.InstanceId = &in.InstanceID
	req.Timeout = stringPtr(in.Timeout)
	keys := make([]string, 0, len(in.Metadata))
	for k := range in.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		k, v := k, in.Metadata[k]
		req.Metadata = append(req.Metadata, &ags.MetadataVar{Name: &k, Value: &v})
	}
	response, err := client.UpdateSandboxInstanceWithContext(callCtx, req)
	if err != nil {
		return wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil {
		return &Error{Code: "ClientError.MalformedResponse"}
	}
	return nil
}

// MetadataForUpdate rejects lossy inputs before the full-replacement mutation.
func (a *AGS) MetadataForUpdate(ctx context.Context, id string) (map[string]string, error) {
	client, callCtx, cancel, err := a.client(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	req := ags.NewDescribeSandboxInstanceListRequest()
	req.InstanceIds = []*string{&id}
	limit := int64(1)
	req.Limit = &limit
	response, err := client.DescribeSandboxInstanceListWithContext(callCtx, req)
	if err != nil {
		return nil, wrapCall(callCtx, err)
	}
	if response == nil || response.Response == nil {
		return nil, &Error{Code: "ClientError.MalformedResponse"}
	}
	instances := response.Response.InstanceSet
	if len(instances) == 0 {
		return nil, &Error{Code: "ResourceNotFound"}
	}
	if len(instances) != 1 || instances[0] == nil || valueString(instances[0].InstanceId) != id {
		return nil, &Error{Code: "ClientError.MalformedResponse"}
	}
	out := map[string]string{}
	for _, v := range instances[0].Metadata {
		if v == nil || v.Name == nil || v.Value == nil || *v.Name == "" {
			return nil, &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("metadata cannot be round-tripped")}
		}
		if _, exists := out[*v.Name]; exists {
			return nil, &Error{Code: "ClientError.MalformedResponse", Cause: errors.New("metadata contains duplicate keys")}
		}
		out[*v.Name] = *v.Value
	}
	return out, nil
}
