package unifiedresources

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The top-level system resolvers only ever act on matches at or above
// HighConfidenceThreshold. FindHighConfidenceCandidates must return exactly
// those matches, in the same order, without paying for the lower tiers
// (hostname-only, ip-only), which collect and sort every id in a shared bucket.

func filterHighConfidence(matches []MatchCandidate) []MatchCandidate {
	var out []MatchCandidate
	for _, match := range matches {
		if match.Confidence >= HighConfidenceThreshold {
			out = append(out, match)
		}
	}
	return out
}

func randomIdentity(r *rand.Rand) ResourceIdentity {
	pick := func(prefix string, n int) []string {
		var out []string
		for i, count := 0, r.Intn(3); i < count; i++ {
			out = append(out, fmt.Sprintf("%s%d", prefix, r.Intn(n)))
		}
		return out
	}
	identity := ResourceIdentity{
		Hostnames:    pick("Host-", 6),
		IPAddresses:  pick("10.0.0.", 6),
		MACAddresses: pick("aa:bb:cc:dd:ee:0", 6),
	}
	if r.Intn(3) == 0 {
		identity.MachineID = fmt.Sprintf("machine-%d", r.Intn(5))
	}
	if r.Intn(4) == 0 {
		identity.DMIUUID = fmt.Sprintf("dmi-%d", r.Intn(5))
	}
	return identity
}

func TestFindHighConfidenceCandidatesMatchesFilteredFindCandidates(t *testing.T) {
	for seed := int64(1); seed <= 600; seed++ {
		r := rand.New(rand.NewSource(seed))
		matcher := NewIdentityMatcher()
		identities := make([]ResourceIdentity, 25)
		for i := range identities {
			identities[i] = randomIdentity(r)
			matcher.Add(fmt.Sprintf("res-%02d", i), identities[i])
		}
		for i, identity := range identities {
			want := filterHighConfidence(matcher.FindCandidates(identity))
			got := matcher.FindHighConfidenceCandidates(identity)
			if !slices.Equal(got, want) {
				t.Fatalf("seed %d identity %d (%+v):\n got  %+v\n want %+v", seed, i, identity, got, want)
			}
		}
	}
}

func TestFindHighConfidenceCandidatesSkipsSharedLowTierBuckets(t *testing.T) {
	// Every resource shares one hostname and one IP: the lower tiers would
	// collect all of them for every query.
	matcher := NewIdentityMatcher()
	shared := ResourceIdentity{Hostnames: []string{"shared.local"}, IPAddresses: []string{"10.1.1.1"}}
	for i := 0; i < 1000; i++ {
		matcher.Add(fmt.Sprintf("res-%04d", i), shared)
	}
	if got := matcher.FindHighConfidenceCandidates(shared); len(got) != 0 {
		t.Fatalf("hostname+ip alone is below the threshold, got %+v", got)
	}
	allocs := testing.AllocsPerRun(20, func() { matcher.FindHighConfidenceCandidates(shared) })
	if allocs > 10 {
		t.Fatalf("FindHighConfidenceCandidates allocated %.0f times against a 1000-entry shared bucket; want <= 10", allocs)
	}
}

func randomTopLevelFleet(r *rand.Rand, size int) []Resource {
	hosts := []string{"tower.local", "tower", "qnap.local", "qnap", "esxi-01.lab.local", "archive.local", "pbs.lab", "node-a"}
	machines := []string{"machine-1", "machine-2", "machine-3", ""}
	pickHost := func() string { return hosts[r.Intn(len(hosts))] }
	var resources []Resource
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("res-%03d", i)
		agent := fmt.Sprintf("agent-%d", r.Intn(6))
		switch r.Intn(8) {
		case 0:
			resources = append(resources, topLevelTestAgent(id, pickHost(), machines[r.Intn(3)], agent))
		case 1:
			resources = append(resources, topLevelTestAgentWithoutMachineID(id, pickHost(), agent))
		case 2:
			resources = append(resources, topLevelTestDockerHost(id, pickHost(), fmt.Sprintf("runtime-%d", r.Intn(6)), agent))
		case 3:
			resources = append(resources, topLevelTestProxmoxNode(id, pickHost(), fmt.Sprintf("pve-%d", r.Intn(4)), "https://"+pickHost()+":8006"))
		case 4:
			resources = append(resources, topLevelTestTrueNAS(id, pickHost()))
		case 5:
			resources = append(resources, topLevelTestVMwareHost(id, pickHost(), fmt.Sprintf("vc-%d", r.Intn(3)), machines[r.Intn(3)]))
		case 6:
			resources = append(resources, topLevelTestPBS(id, pickHost(), fmt.Sprintf("pbs-%d", r.Intn(4))))
		default:
			resources = append(resources, topLevelTestK8sCluster(id, fmt.Sprintf("cluster-%d", r.Intn(4)), agent, "https://"+pickHost()+":6443"))
		}
	}
	return resources
}

// resolverSignature captures what callers rely on from a resolver while
// ignoring the order of grouping evidence, which the resolver itself does not
// keep stable from run to run (it appends it while ranging over maps).
func resolverSignature(resolver TopLevelSystemResolver) string {
	sortedKeys := func(set map[string]struct{}) string {
		keys := make([]string, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return strings.Join(keys, ",")
	}
	var b strings.Builder
	for _, group := range resolver.groups {
		ids := make([]string, 0, len(group.resources))
		for _, resource := range group.resources {
			ids = append(ids, resource.ID)
		}
		sort.Strings(ids)
		fmt.Fprintf(&b, "group %s prio=%d attach=%v resources=[%s] hosts=[%s] ips=[%s] strong=[%s]\n",
			group.id, group.priority, group.attachByHost, strings.Join(ids, ","),
			sortedKeys(group.exactHosts), sortedKeys(group.exactIPs), sortedKeys(group.strongIDs))
	}
	mapped := make([]string, 0, len(resolver.resourceToGroup))
	for resource, group := range resolver.resourceToGroup {
		mapped = append(mapped, resource+"="+group)
	}
	sort.Strings(mapped)
	fmt.Fprintf(&b, "count=%d map=%s", resolver.Count(), strings.Join(mapped, ";"))
	return b.String()
}

func TestResolverSignatureIsStableForTheOriginalLookup(t *testing.T) {
	// Guards the comparison below: if the oracle disagreed with itself, a
	// mismatch would prove nothing.
	for seed := int64(1); seed <= 100; seed++ {
		r := rand.New(rand.NewSource(seed))
		fleet := randomTopLevelFleet(r, 2+r.Intn(40))
		want := resolverSignature(resolveTopLevelSystems(fleet, (*IdentityMatcher).FindCandidates))
		for i := 0; i < 10; i++ {
			if got := resolverSignature(resolveTopLevelSystems(fleet, (*IdentityMatcher).FindCandidates)); got != want {
				t.Fatalf("seed %d: the original lookup disagrees with itself:\n%s\n---\n%s", seed, want, got)
			}
		}
	}
}

func TestResolveTopLevelSystemsIsIndependentOfLowConfidenceLookups(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		r := rand.New(rand.NewSource(seed))
		fleet := randomTopLevelFleet(r, 2+r.Intn(40))
		want := resolverSignature(resolveTopLevelSystems(fleet, (*IdentityMatcher).FindCandidates))
		for i := 0; i < 5; i++ { // the resolver ranges over maps; look at several runs
			got := resolverSignature(resolveTopLevelSystems(fleet, (*IdentityMatcher).FindHighConfidenceCandidates))
			if got != want {
				t.Fatalf("seed %d: grouping changed when low-confidence lookups were skipped (%d resources):\n%s\n---\n%s",
					seed, len(fleet), want, got)
			}
		}
	}
}
