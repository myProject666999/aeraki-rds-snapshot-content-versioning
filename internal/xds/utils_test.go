// Copyright Aeraki Authors
//
// Licensed under the Apache License, License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xds

import (
	"regexp"
	"testing"

	httproute "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"

	metaroute "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/config/route/v1alpha"
)

func newMetaRouteConfig(name string, routeNames ...string) *metaroute.RouteConfiguration {
	routes := make([]*metaroute.Route, 0, len(routeNames))
	for _, routeName := range routeNames {
		routes = append(routes, &metaroute.Route{
			Name:  routeName,
			Match: &metaroute.RouteMatch{},
			Route: &metaroute.RouteAction{
				ClusterSpecifier: &metaroute.RouteAction_Cluster{
					Cluster: "outbound|8000||" + name,
				},
			},
		})
	}
	return &metaroute.RouteConfiguration{
		Name:   name,
		Routes: routes,
	}
}

func snapshotVersion(t *testing.T, snapshot interface {
	GetVersion(string) string
}) string {
	t.Helper()
	version := snapshot.GetVersion(resource.RouteType)
	// The version is a SHA256 hex digest, i.e. 64 lowercase hex characters, and must no longer
	// look like a unix timestamp.
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(version) {
		t.Fatalf("snapshot version %q is not a 64 chars hex digest", version)
	}
	return version
}

// TestGenerateSnapshot_DeterministicVersion verifies that the snapshot version is derived from
// content only: re-generating a snapshot from the same input, in any order, yields the same
// version without involving a clock.
func TestGenerateSnapshot_DeterministicVersion(t *testing.T) {
	routeA := newMetaRouteConfig("a.example.com_8000", "route-a")
	routeB := newMetaRouteConfig("b.example.com_9000", "route-b")

	snapshotAB, err := generateSnapshot([]*metaroute.RouteConfiguration{routeA, routeB})
	if err != nil {
		t.Fatalf("generateSnapshot failed: %v", err)
	}
	snapshotBA, err := generateSnapshot([]*metaroute.RouteConfiguration{routeB, routeA})
	if err != nil {
		t.Fatalf("generateSnapshot failed: %v", err)
	}

	versionAB := snapshotVersion(t, snapshotAB)
	versionBA := snapshotVersion(t, snapshotBA)
	if versionAB != versionBA {
		t.Errorf("swapping the input order changed the version: %q vs %q", versionAB, versionBA)
	}

	// The two independent snapshots must contain the exact same resources regardless of order.
	resourcesAB := snapshotAB.GetResources(resource.RouteType)
	resourcesBA := snapshotBA.GetResources(resource.RouteType)
	if len(resourcesAB) != len(resourcesBA) || len(resourcesAB) != 2 {
		t.Fatalf("unexpected resource count: %d vs %d", len(resourcesAB), len(resourcesBA))
	}
	for name := range resourcesAB {
		if _, ok := resourcesBA[name]; !ok {
			t.Errorf("resource %q missing from the second snapshot", name)
		}
	}
}

// TestGenerateSnapshot_ContentChangeChangesVersion verifies that two snapshots generated back to
// back (which may happen within the same wall-clock second) get different versions when the
// content differs, and equal versions when the content is identical.
func TestGenerateSnapshot_ContentChangeChangesVersion(t *testing.T) {
	first := newMetaRouteConfig("a.example.com_8000", "route-a")
	snapshotFirst, err := generateSnapshot([]*metaroute.RouteConfiguration{first})
	if err != nil {
		t.Fatalf("generateSnapshot failed: %v", err)
	}

	// Same content, generated immediately afterwards: versions must be identical even though the
	// old time-based implementation would have produced the same unix timestamp anyway.
	snapshotSame, err := generateSnapshot([]*metaroute.RouteConfiguration{
		newMetaRouteConfig("a.example.com_8000", "route-a"),
	})
	if err != nil {
		t.Fatalf("generateSnapshot failed: %v", err)
	}
	if version := snapshotVersion(t, snapshotFirst); version != snapshotVersion(t, snapshotSame) {
		t.Errorf("identical content produced different versions: %q vs %q",
			version, snapshotSame.GetVersion(resource.RouteType))
	}

	// Different content, also generated immediately afterwards: the versions must differ. This is
	// the collision the old time.Now().Unix() version could not distinguish within one second.
	changed := newMetaRouteConfig("a.example.com_8000", "route-a", "route-b")
	snapshotChanged, err := generateSnapshot([]*metaroute.RouteConfiguration{changed})
	if err != nil {
		t.Fatalf("generateSnapshot failed: %v", err)
	}
	if snapshotVersion(t, snapshotFirst) == snapshotVersion(t, snapshotChanged) {
		t.Error("different content produced the same version")
	}
}

// TestGenerateSnapshot_IndependentInstancesEqualVersion verifies that two snapshots built from
// equivalent inputs are byte-for-byte version-equal, as two independent Aeraki replicas would
// build them from their own clocks. Since the version no longer involves any clock state,
// replica clock skew can never make a new replica report a smaller version.
func TestGenerateSnapshot_IndependentInstancesEqualVersion(t *testing.T) {
	build := func() string {
		snapshot, err := generateSnapshot([]*metaroute.RouteConfiguration{
			newMetaRouteConfig("a.example.com_8000", "route-a"),
			newMetaRouteConfig("b.example.com_9000", "route-b"),
		})
		if err != nil {
			t.Fatalf("generateSnapshot failed: %v", err)
		}
		return snapshotVersion(t, snapshot)
	}

	if build() != build() {
		t.Error("two independent snapshot generations produced different versions for equal content")
	}
}

// TestComputeSnapshotVersion_EmptyAndStable covers the empty input edge case and exercises the
// hash helper directly with http route resources.
func TestComputeSnapshotVersion_EmptyAndStable(t *testing.T) {
	emptyVersion, err := computeSnapshotVersion(nil)
	if err != nil {
		t.Fatalf("computeSnapshotVersion failed: %v", err)
	}
	if emptyVersion == "" {
		t.Fatal("empty input produced an empty version")
	}
	emptyVersionAgain, err := computeSnapshotVersion([]types.Resource{})
	if err != nil {
		t.Fatalf("computeSnapshotVersion failed: %v", err)
	}
	if emptyVersion != emptyVersionAgain {
		t.Error("empty inputs produced different versions")
	}

	routes := []types.Resource{
		&httproute.RouteConfiguration{Name: "z.example.com_8000"},
		&httproute.RouteConfiguration{Name: "a.example.com_8000"},
	}
	reversed := []types.Resource{
		&httproute.RouteConfiguration{Name: "a.example.com_8000"},
		&httproute.RouteConfiguration{Name: "z.example.com_8000"},
	}
	version, err := computeSnapshotVersion(routes)
	if err != nil {
		t.Fatalf("computeSnapshotVersion failed: %v", err)
	}
	versionReversed, err := computeSnapshotVersion(reversed)
	if err != nil {
		t.Fatalf("computeSnapshotVersion failed: %v", err)
	}
	if version != versionReversed {
		t.Error("resource order changed the computed version")
	}
	if version == emptyVersion {
		t.Error("non-empty content hashed to the same version as empty content")
	}
}
