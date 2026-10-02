package unifiedresources

import "reflect"

// RegistryCloneDriftForTest reports every stored resource whose registry
// read copy differs from a full cloneResource (which recomputes canonical
// metadata). Reads that skip the recompute are only correct while this is
// empty.
func RegistryCloneDriftForTest(rr *ResourceRegistry) []string {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.ensureViewsLocked()

	var drift []string
	for id, r := range rr.resources {
		if !reflect.DeepEqual(cloneRegistryResource(r), cloneResource(r)) {
			drift = append(drift, id)
		}
	}
	return drift
}
