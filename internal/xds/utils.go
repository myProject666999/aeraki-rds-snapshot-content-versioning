// Copyright 2020 Envoyproxy Authors
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package xds

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"

	httpcore "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	httproute "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	matcher "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"

	userapi "github.com/aeraki-mesh/api/metaprotocol/v1alpha1"
	metaroute "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/config/route/v1alpha"
)

var regexEngine = &matcher.RegexMatcher_GoogleRe2{GoogleRe2: &matcher.RegexMatcher_GoogleRE2{}}

// We use Envoy RDS(HTTP RouteConfiguration) to transmit Meta Protocol Configuration from the RDS server to the Proxy
func metaProtocolRoute2HttpRoute(metaRoute *metaroute.RouteConfiguration) *httproute.RouteConfiguration {
	httpRoute := &httproute.RouteConfiguration{
		Name: metaRoute.Name,
		VirtualHosts: []*httproute.VirtualHost{
			{
				Name:    "dummy",
				Domains: []string{"*"},
			},
		},
	}

	for _, route := range metaRoute.Routes {
		routeMatch := httpRouteMatch(route)
		routeAction := httpRouteAction(route)
		requestHeaders := httpRequestHeaders(route)
		responseHeaders := httpResponseHeaders(route)
		httpRoute.VirtualHosts[0].Routes = append(httpRoute.VirtualHosts[0].Routes, &httproute.Route{
			Name:  route.Name,
			Match: routeMatch,
			Action: &httproute.Route_Route{
				Route: routeAction,
			},
			RequestHeadersToAdd:  requestHeaders,
			ResponseHeadersToAdd: responseHeaders,
		})
	}
	return httpRoute
}

func httpResponseHeaders(route *metaroute.Route) []*httpcore.HeaderValueOption {
	var responseHeaders []*httpcore.HeaderValueOption
	for _, mutation := range route.ResponseMutation {
		responseHeaders = append(responseHeaders, &httpcore.HeaderValueOption{
			Header: &httpcore.HeaderValue{
				Key:   mutation.Key,
				Value: mutation.Value,
			},
		})
	}
	return responseHeaders
}

func httpRequestHeaders(route *metaroute.Route) []*httpcore.HeaderValueOption {
	var requestHeaders []*httpcore.HeaderValueOption
	for _, mutation := range route.RequestMutation {
		requestHeaders = append(requestHeaders, &httpcore.HeaderValueOption{
			Header: &httpcore.HeaderValue{
				Key:   mutation.Key,
				Value: mutation.Value,
			},
		})
	}
	return requestHeaders
}

func httpRouteAction(route *metaroute.Route) *httproute.RouteAction {
	var routeAction = &httproute.RouteAction{}
	if route.Route.GetWeightedClusters() != nil {
		routeAction.ClusterSpecifier = &httproute.RouteAction_WeightedClusters{
			WeightedClusters: route.Route.GetWeightedClusters(),
		}
	} else {
		routeAction.ClusterSpecifier = &httproute.RouteAction_Cluster{
			Cluster: route.Route.GetCluster(),
		}
	}
	routeAction.RequestMirrorPolicies = make([]*httproute.
		RouteAction_RequestMirrorPolicy, len(route.Route.RequestMirrorPolicies))
	for i, mirrorPolicy := range route.Route.RequestMirrorPolicies {
		routeAction.RequestMirrorPolicies[i] = &httproute.RouteAction_RequestMirrorPolicy{
			Cluster:         mirrorPolicy.Cluster,
			RuntimeFraction: mirrorPolicy.RuntimeFraction,
		}
	}
	if len(route.Route.HashPolicy) > 0 {
		for _, hashPolicy := range route.Route.HashPolicy {
			routeAction.HashPolicy = append(routeAction.HashPolicy, &httproute.RouteAction_HashPolicy{
				PolicySpecifier: &httproute.RouteAction_HashPolicy_Header_{
					Header: &httproute.RouteAction_HashPolicy_Header{
						HeaderName: hashPolicy,
					},
				},
			})
		}
	}
	return routeAction
}

func httpRouteMatch(route *metaroute.Route) *httproute.RouteMatch {
	routeMatch := &httproute.RouteMatch{
		PathSpecifier: &httproute.RouteMatch_Prefix{
			Prefix: "/",
		},
	}
	for _, metadata := range route.Match.Metadata {
		routeMatch.Headers = append(routeMatch.Headers, &httproute.HeaderMatcher{
			Name:                 metadata.Name,
			HeaderMatchSpecifier: metadata.HeaderMatchSpecifier,
		})
	}
	return routeMatch
}

// generateSnapshot builds a SnapshotCache snapshot from the given meta protocol routes.
//
// The snapshot version is derived solely from the content of the RouteConfiguration resources:
// identical content always produces the same version, while any content change produces a
// different version. This replaces the previous time.Now().Unix() based version, which could
// collide for changes happening within the same second (causing SnapshotCache to skip the push
// because the version was unchanged) and was different on every Aeraki replica (causing
// redundant full pushes and potentially non-monotonic versions when Envoy reconnected to another
// replica with a skewed clock).
//
// The resources are sorted by name and each one is deterministically serialized (see
// computeSnapshotVersion), so the version does not depend on the order in which the config store
// returns ServiceEntries. Note that the version string format changes from a unix timestamp to a
// SHA256 hex digest; already connected Envoys will observe one full RDS push on upgrade, which is
// an acceptable compatibility cost.
func generateSnapshot(metaRoutes []*metaroute.RouteConfiguration) (*cache.Snapshot, error) {
	httpRoutes := make([]types.Resource, 0, len(metaRoutes))
	for _, route := range metaRoutes {
		httpRoutes = append(httpRoutes, metaProtocolRoute2HttpRoute(route))
	}

	version, err := computeSnapshotVersion(httpRoutes)
	if err != nil {
		return nil, err
	}
	return cache.NewSnapshot(
		version,
		map[resource.Type][]types.Resource{
			resource.RouteType: httpRoutes,
		},
	)
}

// computeSnapshotVersion returns a content-based version string for the given route resources.
//
// The resources are sorted by their resource name first, so the result is independent of the
// ordering produced by the config store. Each resource is then marshaled with deterministic
// protobuf serialization (deterministic map ordering and no unstable fields), and the resulting
// byte slices are length-prefixed and fed into a SHA256 hash. The length prefixes guarantee that
// no concatenation of neighboring resources can produce the same digest.
func computeSnapshotVersion(routes []types.Resource) (string, error) {
	sortedRoutes := make([]types.Resource, len(routes))
	copy(sortedRoutes, routes)
	sort.Slice(sortedRoutes, func(i, j int) bool {
		return cache.GetResourceName(sortedRoutes[i]) < cache.GetResourceName(sortedRoutes[j])
	})

	hasher := sha256.New()
	for _, route := range sortedRoutes {
		marshaled, err := cache.MarshalResource(route)
		if err != nil {
			return "", err
		}
		// Prefix each marshaled resource with its length to keep the hash unambiguous even if two
		// adjacent resources were to be concatenated into the same byte sequence.
		var lengthBuf [8]byte
		binary.BigEndian.PutUint64(lengthBuf[:], uint64(len(marshaled)))
		hasher.Write(lengthBuf[:])
		hasher.Write(marshaled)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// MetaMatch2HttpHeaderMatch converts MetaMatch to HttpHeaderMatch
func MetaMatch2HttpHeaderMatch(matchCrd *userapi.MetaRouteMatch) []*httproute.HeaderMatcher {
	var headerMatchers []*httproute.HeaderMatcher
	if matchCrd != nil {
		for name, attribute := range matchCrd.Attributes {
			headerMatcher := &httproute.HeaderMatcher{}
			headerMatcher.Name = name
			if isCatchAllHeaderMatch(attribute) {
				headerMatcher.HeaderMatchSpecifier = &httproute.HeaderMatcher_PresentMatch{PresentMatch: true}
			} else {
				switch attribute.GetMatchType().(type) {
				case *userapi.StringMatch_Exact:
					headerMatcher.HeaderMatchSpecifier = &httproute.HeaderMatcher_ExactMatch{
						ExactMatch: attribute.GetExact(),
					}
				case *userapi.StringMatch_Prefix:
					headerMatcher.HeaderMatchSpecifier = &httproute.HeaderMatcher_PrefixMatch{
						PrefixMatch: attribute.GetPrefix(),
					}
				case *userapi.StringMatch_Regex:
					headerMatcher.HeaderMatchSpecifier = &httproute.HeaderMatcher_SafeRegexMatch{
						SafeRegexMatch: &matcher.RegexMatcher{
							EngineType: regexEngine,
							Regex:      attribute.GetRegex(),
						},
					}
				default:
					continue
				}
			}
			headerMatchers = append(headerMatchers, headerMatcher)
		}
	}
	return headerMatchers
}

func isCatchAllHeaderMatch(in *userapi.StringMatch) bool {
	catchall := false

	if in == nil {
		return true
	}

	switch m := in.MatchType.(type) {
	case *userapi.StringMatch_Regex:
		catchall = m.Regex == "*"
	}

	return catchall
}

func translatePercentToFractionalPercent(p float64) *typev3.FractionalPercent {
	return &typev3.FractionalPercent{
		Numerator:   uint32(p * 10000),
		Denominator: typev3.FractionalPercent_MILLION,
	}
}
