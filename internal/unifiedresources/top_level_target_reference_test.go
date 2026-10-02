package unifiedresources

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// The fallback attachment search is the resolver's hot loop on estates where
// one hostname is shared by many groups (a k3s node carrying hundreds of
// Kubernetes objects). Its optimised form must return exactly what the
// original returned - same target, same evidence - for every group.
func TestUniqueBetterTopLevelSystemTargetMatchesReference(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		groups := randomTopLevelSystemGroups(rand.New(rand.NewSource(seed)))
		hostOwners, ipOwners := buildTopLevelSystemFallbackOwners(groups)
		hostFormOwners := buildTopLevelSystemHostFormOwners(groups)
		owners := newTopLevelSystemOwnerIndex(groups)
		for root, group := range groups {
			wantTarget, wantOK := referenceUniqueBetterTopLevelSystemTarget(root, group, hostOwners, ipOwners, hostFormOwners, groups)
			gotTarget, gotOK := uniqueBetterTopLevelSystemTarget(root, group, owners, groups)
			if gotOK != wantOK || !reflect.DeepEqual(gotTarget, wantTarget) {
				t.Fatalf("seed %d group %d: got (%+v, %v), want (%+v, %v)", seed, root, gotTarget, gotOK, wantTarget, wantOK)
			}
		}
	}
}

// randomTopLevelSystemGroups draws groups from a small pool of FQDNs, their
// short forms and IPs so owners overlap heavily, with mixed priorities.
func randomTopLevelSystemGroups(rng *rand.Rand) map[int]topLevelSystemResolvedGroup {
	hosts := []string{"k3s-1", "k3s-1.lab.example", "pve-a", "pve-a.lab.example", "nas", "nas.lab.example", "nas.other.example", "db"}
	ips := []string{"10.0.20.11", "10.0.20.12", "10.0.20.13", "192.168.1.5"}
	groups := make(map[int]topLevelSystemResolvedGroup)
	count := 2 + rng.Intn(60)
	for i := 0; i < count; i++ {
		root := i * 3 // roots need not be dense
		group := topLevelSystemResolvedGroup{
			id:           fmt.Sprintf("g%d", root),
			exactHosts:   map[string]struct{}{},
			exactIPs:     map[string]struct{}{},
			priority:     rng.Intn(5),
			attachByHost: rng.Intn(4) != 0,
		}
		for n := rng.Intn(3); n > 0; n-- {
			group.exactHosts[hosts[rng.Intn(len(hosts))]] = struct{}{}
		}
		for n := rng.Intn(2); n > 0; n-- {
			group.exactIPs[ips[rng.Intn(len(ips))]] = struct{}{}
		}
		groups[root] = group
	}
	return groups
}

func referenceUniqueBetterTopLevelSystemTarget(
	groupRoot int,
	group topLevelSystemResolvedGroup,
	hostOwners map[string]map[int]struct{},
	ipOwners map[string]map[int]struct{},
	hostFormOwners map[string]map[int]struct{},
	groups map[int]topLevelSystemResolvedGroup,
) (topLevelSystemFallbackTarget, bool) {
	targets := make(map[int]topLevelSystemGroupingEvidence)
	candidateRoots := make(map[int]struct{})
	for host := range group.exactHosts {
		comparable := normalizeComparableHostname(host)
		if comparable == "" {
			continue
		}
		for root := range hostFormOwners[comparable] {
			candidateRoots[root] = struct{}{}
		}
		if short := NormalizeHostname(comparable); short != "" && short != comparable {
			for root := range hostFormOwners[short] {
				candidateRoots[root] = struct{}{}
			}
		}
	}

	for _, host := range topLevelSystemSortedSet(group.exactHosts) {
		for _, targetRoot := range topLevelSystemSortedRoots(hostOwners[host]) {
			if targetRoot == groupRoot {
				continue
			}
			if groups[targetRoot].priority >= group.priority {
				continue
			}
			if _, ok := targets[targetRoot]; !ok {
				targets[targetRoot] = topLevelSystemAttachmentEvidence(
					group,
					groups[targetRoot],
					"exact-host-attachment",
					"exact-host",
					host,
				)
			}
		}
	}
	for _, targetRoot := range topLevelSystemSortedRoots(candidateRoots) {
		if targetRoot == groupRoot {
			continue
		}
		if groups[targetRoot].priority >= group.priority {
			continue
		}
		if _, ok := targets[targetRoot]; ok {
			continue
		}
		if value, ok := topLevelSystemShortFormHostMatchValue(group.exactHosts, groups[targetRoot].exactHosts); ok {
			targets[targetRoot] = topLevelSystemAttachmentEvidence(
				group,
				groups[targetRoot],
				"hostname-form-attachment",
				"short-hostname",
				value,
			)
		}
	}
	for _, ip := range topLevelSystemSortedSet(group.exactIPs) {
		for _, targetRoot := range topLevelSystemSortedRoots(ipOwners[ip]) {
			if targetRoot == groupRoot {
				continue
			}
			if groups[targetRoot].priority >= group.priority {
				continue
			}
			if _, ok := targets[targetRoot]; !ok {
				targets[targetRoot] = topLevelSystemAttachmentEvidence(
					group,
					groups[targetRoot],
					"exact-ip-attachment",
					"exact-ip",
					ip,
				)
			}
		}
	}

	if len(targets) != 1 {
		return topLevelSystemFallbackTarget{}, false
	}
	for targetRoot, evidence := range targets {
		return topLevelSystemFallbackTarget{
			root:     targetRoot,
			evidence: evidence,
		}, true
	}
	return topLevelSystemFallbackTarget{}, false
}
