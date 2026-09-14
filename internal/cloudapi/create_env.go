package cloudapi

import (
	ags "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ags/v20250920"
	"sort"
)

func createEnvironment(values map[string]string) *ags.CustomConfiguration {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	config := &ags.CustomConfiguration{}
	for _, key := range keys {
		key, value := key, values[key]
		config.Env = append(config.Env, &ags.EnvVar{Name: &key, Value: &value})
	}
	return config
}
