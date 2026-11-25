// Copyright Aeraki Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
	"context"
	"sync/atomic"
	"testing"
	"time"

	metaprotocolapi "github.com/aeraki-mesh/api/metaprotocol/v1alpha1"
	metaprotocol "github.com/aeraki-mesh/client-go/pkg/apis/metaprotocol/v1alpha1"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
	networking "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/config/memory"
	istiomodel "istio.io/istio/pilot/pkg/model"
	istioconfig "istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/schema/collection"
	"istio.io/istio/pkg/config/schema/collections"
	"istio.io/istio/pkg/config/schema/gvk"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace = "meta-test"
	testNode      = "node-1"
	routeHostA    = "a.meta-test.svc.cluster.local"
	routeHostB    = "b.meta-test.svc.cluster.local"
)

func newTestStore() istiomodel.ConfigStore {
	schemas := collection.NewSchemasBuilder().
		MustAdd(collections.ServiceEntry).
		MustAdd(collections.DestinationRule).Build()
	return memory.MakeSkipValidation(schemas)
}

func newServiceEntry(host string) istioconfig.Config {
	return istioconfig.Config{
		Meta: istioconfig.Meta{
			Name:             host,
			Namespace:        testNamespace,
			GroupVersionKind: gvk.ServiceEntry,
		},
		Spec: &networking.ServiceEntry{
			Hosts: []string{host},
			Ports: []*networking.ServicePort{
				{
					Number:   8000,
					Name:     "tcp-metaprotocol-dubbo",
					Protocol: "TCP",
				},
			},
		},
	}
}

func metaRouterName(host string) string {
	return "meta-router-" + host
}

func newMetaRouter(host string, routeDestinations ...string) *metaprotocol.MetaRouter {
	routes := make([]*metaprotocolapi.MetaRoute, 0, len(routeDestinations))
	for _, destination := range routeDestinations {
		routes = append(routes, &metaprotocolapi.MetaRoute{
			Name: "route-" + destination,
			Route: []*metaprotocolapi.MetaRouteDestination{
				{
					Destination: &metaprotocolapi.Destination{
						Host:   host,
						Subset: destination,
					},
				},
			},
		})
	}
	return &metaprotocol.MetaRouter{
		TypeMeta: metav1.TypeMeta{
			APIVersion: metaprotocol.SchemeGroupVersion.String(),
			Kind:       "MetaRouter",
		},
		ObjectMeta: metav1.ObjectMeta{Name: metaRouterName(host), Namespace: testNamespace},
		Spec: metaprotocolapi.MetaRouter{
			Hosts:  []string{host},
			Routes: routes,
		},
	}
}

func newFakeMetaRouterClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := metaprotocol.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to build scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func newTestCacheMgr(t *testing.T, store istiomodel.ConfigStore, metaRouters ...client.Object) *CacheMgr {
	t.Helper()
	mgr := NewCacheMgr(store)
	mgr.MetaRouterControllerClient = newFakeMetaRouterClient(t, metaRouters...)
	return mgr
}

// updateMetaRouter replaces the routes of the MetaRouter associated with host, preserving the
// resource version held by the fake client (a plain Update with a freshly built object would be
// rejected as a stale object). The spec is mutated in place instead of copied, as it embeds a
// protobuf message state containing a lock.
func updateMetaRouter(t *testing.T, mgr *CacheMgr, host string, routeDestinations ...string) {
	t.Helper()
	metaRouter := &metaprotocol.MetaRouter{}
	if err := mgr.MetaRouterControllerClient.Get(context.Background(), client.ObjectKey{
		Namespace: testNamespace,
		Name:      metaRouterName(host),
	}, metaRouter); err != nil {
		t.Fatalf("get meta router failed: %v", err)
	}
	metaRouter.Spec.Routes = newMetaRouter(host, routeDestinations...).Spec.Routes
	if err := mgr.MetaRouterControllerClient.Update(context.Background(), metaRouter); err != nil {
		t.Fatalf("update meta router failed: %v", err)
	}
}

func routeConfigName(host string) string {
	return host + "_8000"
}

// countingSnapshotCache wraps a SnapshotCache and counts how many times SetSnapshot is invoked,
// so tests can assert that content-equal updates skip the cache write.
type countingSnapshotCache struct {
	cachev3.SnapshotCache
	setSnapshotCalls atomic.Int32
}

func (c *countingSnapshotCache) SetSnapshot(ctx context.Context, node string,
	snapshot cachev3.ResourceSnapshot) error {
	c.setSnapshotCalls.Add(1)
	return c.SnapshotCache.SetSnapshot(ctx, node, snapshot)
}

// TestUpdateRouteCache_OrderIndependentVersion verifies the acceptance scenario where two
// ServiceEntries are returned in swapped order: the generated snapshot version must not change.
// The in-memory config store is map-backed, so List order is intentionally unstable.
func TestUpdateRouteCache_OrderIndependentVersion(t *testing.T) {
	storeA := newTestStore()
	if _, err := storeA.Create(newServiceEntry(routeHostA)); err != nil {
		t.Fatalf("create service entry A failed: %v", err)
	}
	if _, err := storeA.Create(newServiceEntry(routeHostB)); err != nil {
		t.Fatalf("create service entry B failed: %v", err)
	}

	// A second, independent store holding the same config simulates another Aeraki replica.
	storeB := newTestStore()
	if _, err := storeB.Create(newServiceEntry(routeHostB)); err != nil {
		t.Fatalf("create service entry B failed: %v", err)
	}
	if _, err := storeB.Create(newServiceEntry(routeHostA)); err != nil {
		t.Fatalf("create service entry A failed: %v", err)
	}

	metaRouterA := newMetaRouter(routeHostA, "v1")
	metaRouterB := newMetaRouter(routeHostB, "v1")

	versions := make([]string, 3)
	for i, store := range []istiomodel.ConfigStore{storeA, storeB, storeA} {
		mgr := newTestCacheMgr(t, store, metaRouterA, metaRouterB)
		// Register a subscriber so updateRouteCache does not short-circuit.
		mgr.routeCache.SetSnapshot(context.Background(), testNode, mustEmptySnapshot(t))
		if err := mgr.updateRouteCache(); err != nil {
			t.Fatalf("updateRouteCache failed: %v", err)
		}
		snapshot, err := mgr.routeCache.GetSnapshot(testNode)
		if err != nil {
			t.Fatalf("get snapshot failed: %v", err)
		}
		versions[i] = snapshot.GetVersion(resource.RouteType)
	}

	if versions[0] != versions[1] {
		t.Errorf("independent CacheMgr instances produced different versions: %q vs %q",
			versions[0], versions[1])
	}
	if versions[0] != versions[2] {
		t.Error("re-generating from the same store produced a different version")
	}
}

// TestUpdateRouteCache_ContentChangePushesAndUnchangedSkips verifies that:
//  1. consecutive content changes within the same wall-clock second get distinct versions and are
//     pushed to a watch created via SnapshotCache.CreateWatch;
//  2. a re-run with unchanged content skips SetSnapshot entirely and does not push.
func TestUpdateRouteCache_ContentChangePushesAndUnchangedSkips(t *testing.T) {
	store := newTestStore()
	if _, err := store.Create(newServiceEntry(routeHostA)); err != nil {
		t.Fatalf("create service entry failed: %v", err)
	}
	mgr := newTestCacheMgr(t, store, newMetaRouter(routeHostA, "v1"))

	countingCache := &countingSnapshotCache{SnapshotCache: cachev3.NewSnapshotCache(false, cachev3.IDHash{}, logger{})}
	mgr.routeCache = countingCache

	// Simulate an Envoy subscriber through CreateWatch; no gRPC server is involved. A watch in
	// SnapshotCache is one-shot: after being responded it is removed and Envoy re-subscribes with
	// the acknowledged version and the stream's known resource names. createWatch mirrors that.
	watchCh := make(chan cachev3.Response, 1)
	resourceNames := []string{routeConfigName(routeHostA)}
	ackVersion := ""
	createWatch := func() {
		streamState := stream.NewStreamState(false, nil)
		// Envoy retains known resource names on the stream; without this the cache would replay the
		// current snapshot on every re-subscription, hiding the watch semantics under test.
		streamState.SetKnownResourceNamesAsList(resource.RouteType, resourceNames)
		countingCache.CreateWatch(&discovery.DiscoveryRequest{
			Node:          coreNode(testNode),
			TypeUrl:       resource.RouteType,
			ResourceNames: resourceNames,
			VersionInfo:   ackVersion,
		}, streamState, watchCh)
	}
	createWatch()

	// First update: an open watch exists, so the push is delivered immediately.
	if err := mgr.updateRouteCache(); err != nil {
		t.Fatalf("first updateRouteCache failed: %v", err)
	}
	firstVersion := watchVersion(t, receiveWatchResponse(t, watchCh))
	if firstVersion == "" {
		t.Fatal("first push carried an empty version")
	}
	if calls := countingCache.setSnapshotCalls.Load(); calls != 1 {
		t.Fatalf("expected 1 SetSnapshot call after first update, got %d", calls)
	}

	// Re-subscribe with the acknowledged version after Envoy applies the response.
	ackVersion = firstVersion
	createWatch()

	// Re-run without any config change: the content is equal, so SetSnapshot must be skipped and
	// no new push must happen.
	if err := mgr.updateRouteCache(); err != nil {
		t.Fatalf("second updateRouteCache failed: %v", err)
	}
	if calls := countingCache.setSnapshotCalls.Load(); calls != 1 {
		t.Errorf("unchanged content triggered a SetSnapshot call, total calls: %d", calls)
	}
	assertNoWatchResponse(t, watchCh)

	// Change the MetaRouter twice in quick succession. Both updates happen in the same wall-clock
	// second in practice, yet content-based versions must differ on each change.
	updateMetaRouter(t, mgr, routeHostA, "v2")
	if err := mgr.updateRouteCache(); err != nil {
		t.Fatalf("updateRouteCache after first change failed: %v", err)
	}
	secondVersion := watchVersion(t, receiveWatchResponse(t, watchCh))
	if secondVersion == firstVersion {
		t.Error("first content change did not produce a new version")
	}
	ackVersion = secondVersion
	createWatch()

	updateMetaRouter(t, mgr, routeHostA, "v2", "v3")
	if err := mgr.updateRouteCache(); err != nil {
		t.Fatalf("updateRouteCache after second change failed: %v", err)
	}
	thirdVersion := watchVersion(t, receiveWatchResponse(t, watchCh))
	if thirdVersion == secondVersion {
		t.Error("second content change did not produce a new version")
	}
	if calls := countingCache.setSnapshotCalls.Load(); calls != 3 {
		t.Errorf("expected 3 SetSnapshot calls in total, got %d", calls)
	}

	// Re-subscribe and verify another no-op update is still skipped.
	ackVersion = thirdVersion
	createWatch()
	if err := mgr.updateRouteCache(); err != nil {
		t.Fatalf("final updateRouteCache failed: %v", err)
	}
	if calls := countingCache.setSnapshotCalls.Load(); calls != 3 {
		t.Errorf("final no-op update was not skipped, total calls: %d", calls)
	}
	assertNoWatchResponse(t, watchCh)
}

// TestGenerateMetaRoutes_SortedByName verifies the generation order is stable regardless of the
// ServiceEntry listing order.
func TestGenerateMetaRoutes_SortedByName(t *testing.T) {
	store := newTestStore()
	if _, err := store.Create(newServiceEntry(routeHostB)); err != nil {
		t.Fatalf("create service entry B failed: %v", err)
	}
	if _, err := store.Create(newServiceEntry(routeHostA)); err != nil {
		t.Fatalf("create service entry A failed: %v", err)
	}
	mgr := newTestCacheMgr(t, store,
		newMetaRouter(routeHostA, "v1"), newMetaRouter(routeHostB, "v1"))

	routes := mgr.generateMetaRoutes(store.List(gvk.ServiceEntry, ""))
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
	if routes[0].Name != routeConfigName(routeHostA) || routes[1].Name != routeConfigName(routeHostB) {
		t.Errorf("routes are not sorted by name: %q, %q", routes[0].Name, routes[1].Name)
	}
}

func mustEmptySnapshot(t *testing.T) *cachev3.Snapshot {
	t.Helper()
	snapshot, err := cachev3.NewSnapshot("", map[resource.Type][]types.Resource{
		resource.RouteType: {},
	})
	if err != nil {
		t.Fatalf("create empty snapshot failed: %v", err)
	}
	return snapshot
}

func receiveWatchResponse(t *testing.T, ch chan cachev3.Response) cachev3.Response {
	t.Helper()
	select {
	case response := <-ch:
		return response
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a watch response")
		return nil
	}
}

func assertNoWatchResponse(t *testing.T, ch chan cachev3.Response) {
	t.Helper()
	select {
	case response := <-ch:
		t.Fatalf("received an unexpected watch response with version %q", watchVersion(t, response))
	default:
	}
}

func watchVersion(t *testing.T, response cachev3.Response) string {
	t.Helper()
	version, err := response.GetVersion()
	if err != nil {
		t.Fatalf("failed to get watch response version: %v", err)
	}
	return version
}

func coreNode(id string) *corev3.Node {
	return &corev3.Node{Id: id}
}
