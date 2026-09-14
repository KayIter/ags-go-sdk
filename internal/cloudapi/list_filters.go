package cloudapi

import (
	"sort"

	ags "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ags/v20250920"
)

func listFilters(in ListInput) []*ags.Filter {
	var filters []*ags.Filter
	add := func(name string, values []string) {
		filter := &ags.Filter{Name: &name}
		for _, value := range values {
			value := value
			filter.Values = append(filter.Values, &value)
		}
		filters = append(filters, filter)
	}
	if len(in.Statuses) > 0 {
		add("Status", in.Statuses)
	}
	keys := make([]string, 0, len(in.Metadata))
	for key := range in.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		add("metadata:"+key, in.Metadata[key])
	}
	return filters
}
