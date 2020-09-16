/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package router

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"mosn.io/api"
	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol"
	httpmosn "mosn.io/mosn/pkg/protocol/http"
	"mosn.io/mosn/pkg/types"
	"mosn.io/mosn/pkg/upstream/cluster"
)

type RouteRuleImplBase struct {
	// match
	vHost                 *VirtualHostImpl
	routerMatch           v2.RouterMatch
	configHeaders         []*types.HeaderData
	configQueryParameters []types.QueryParameterMatcher //TODO: not implement yet
	// rewrite
	prefixRewrite         string
	hostRewrite           string
	autoHostRewrite       bool
	autoHostRewriteHeader string
	requestHeadersParser  *headerParser
	responseHeadersParser *headerParser
	// information
	upstreamProtocol string
	perFilterConfig  map[string]interface{}
	// policy
	policy *policy
	// direct response
	directResponseRule *directResponseImpl
	// action
	routerAction       v2.RouteAction
	defaultCluster     *weightedClusterEntry // cluster name and metadata
	weightedClusters   map[string]weightedClusterEntry
	totalClusterWeight uint32
	lock               sync.Mutex
	randInstance       *rand.Rand
}

func NewRouteRuleImplBase(vHost *VirtualHostImpl, route *v2.Router) (*RouteRuleImplBase, error) {
	base := &RouteRuleImplBase{
		vHost:                 vHost,
		routerMatch:           route.Match,
		configHeaders:         getRouterHeaders(route.Match.Headers),
		prefixRewrite:         route.Route.PrefixRewrite,
		hostRewrite:           route.Route.HostRewrite,
		autoHostRewrite:       route.Route.AutoHostRewrite,
		autoHostRewriteHeader: route.Route.AutoHostRewriteHeader,
		requestHeadersParser:  getHeaderParser(route.Route.RequestHeadersToAdd, nil),
		responseHeadersParser: getHeaderParser(route.Route.ResponseHeadersToAdd, route.Route.ResponseHeadersToRemove),
		upstreamProtocol:      route.Route.UpstreamProtocol,
		perFilterConfig:       route.PerFilterConfig,
		policy:                &policy{},
		routerAction:          route.Route,
		defaultCluster: &weightedClusterEntry{
			clusterName: route.Route.ClusterName,
		},
		lock: sync.Mutex{},
	}
	// add clusters
	base.weightedClusters, base.totalClusterWeight = getWeightedClusterEntry(route.Route.WeightedClusters)
	if len(route.Route.MetadataMatch) > 0 {
		base.defaultCluster.clusterMetadataMatchCriteria = NewMetadataMatchCriteriaImpl(route.Route.MetadataMatch)
	}
	// add policy
	if route.Route.RetryPolicy != nil {
		base.policy.retryPolicy = &retryPolicyImpl{
			retryOn:      route.Route.RetryPolicy.RetryOn,
			retryTimeout: route.Route.RetryPolicy.RetryTimeout,
			numRetries:   route.Route.RetryPolicy.NumRetries,
		}
	}
	// add hash policy
	if route.Route.HashPolicy != nil && len(route.Route.HashPolicy) >= 1 {
		hp := route.Route.HashPolicy[0]
		if hp.Header != nil {
			base.policy.hashPolicy = &headerHashPolicyImpl{
				key: hp.Header.Key,
			}
		}
		if hp.Cookie != nil {
			base.policy.hashPolicy = &cookieHashPolicyImpl{
				name: hp.Cookie.Name,
				path: hp.Cookie.Path,
				ttl:  hp.Cookie.TTL,
			}
		}
	}
	// use source ip hash policy as default hash policy
	if base.policy.hashPolicy == nil {
		base.policy.hashPolicy = &sourceIPHashPolicyImpl{}
	}
	// add direct repsonse rule
	if route.DirectResponse != nil {
		base.directResponseRule = &directResponseImpl{
			status: route.DirectResponse.StatusCode,
			body:   route.DirectResponse.Body,
		}
	}
	return base, nil
}

func (rri *RouteRuleImplBase) DirectResponseRule() api.DirectResponseRule {
	return rri.directResponseRule
}

// types.RouteRule
// Select Cluster for Routing
// if weighted cluster is nil, return clusterName directly, else
// select cluster from weighted-clusters
func (rri *RouteRuleImplBase) ClusterName() string {
	if len(rri.weightedClusters) == 0 {
		return rri.defaultCluster.clusterName
	}
	rri.lock.Lock()
	if rri.randInstance == nil {
		rri.randInstance = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	rri.lock.Unlock()
	selectedValue := rri.randInstance.Intn(int(rri.totalClusterWeight))
	for _, weightCluster := range rri.weightedClusters {
		selectedValue = selectedValue - int(weightCluster.clusterWeight)
		if selectedValue <= 0 {
			return weightCluster.clusterName
		}
	}
	return rri.defaultCluster.clusterName
}

func (rri *RouteRuleImplBase) UpstreamProtocol() string {
	return rri.upstreamProtocol
}

func (rri *RouteRuleImplBase) GlobalTimeout() time.Duration {
	return rri.routerAction.Timeout
}

func (rri *RouteRuleImplBase) Policy() api.Policy {
	return rri.policy
}

func (rri *RouteRuleImplBase) MetadataMatchCriteria(clusterName string) api.MetadataMatchCriteria {
	criteria := rri.defaultCluster.clusterMetadataMatchCriteria
	if len(rri.weightedClusters) != 0 {
		if cluster, ok := rri.weightedClusters[clusterName]; ok {
			criteria = cluster.clusterMetadataMatchCriteria
		}
	}
	if criteria == nil {
		return nil
	}
	return criteria

}

func (rri *RouteRuleImplBase) PerFilterConfig() map[string]interface{} {
	return rri.perFilterConfig
}

// matchRoute is a common matched for http
func (rri *RouteRuleImplBase) matchRoute(headers api.HeaderMap, randomValue uint64) bool {
	// 1. match headers' KV
	if !ConfigUtilityInst.MatchHeaders(headers, rri.configHeaders) {
		log.DefaultLogger.Debugf(RouterLogFormat, "routerule", "match header", headers)
		return false
	}
	// 2. match query parameters
	if len(rri.configQueryParameters) != 0 {
		var queryParams types.QueryParams
		if QueryString, ok := headers.Get(protocol.MosnHeaderQueryStringKey); ok {
			queryParams = httpmosn.ParseQueryString(QueryString)
		}
		if len(queryParams) != 0 {
			if !ConfigUtilityInst.MatchQueryParams(queryParams, rri.configQueryParameters) {
				log.DefaultLogger.Debugf(RouterLogFormat, "routerule", "match query params", queryParams)
				return false
			}
		}
	}
	return true
}

func (rri *RouteRuleImplBase) finalizePathHeader(headers api.HeaderMap, matchedPath string) {
	if len(rri.prefixRewrite) < 1 {
		return
	}
	if path, ok := headers.Get(protocol.MosnHeaderPathKey); ok {
		if strings.HasPrefix(path, matchedPath) {
			headers.Set(protocol.MosnOriginalHeaderPathKey, path)
			headers.Set(protocol.MosnHeaderPathKey, rri.prefixRewrite+path[len(matchedPath):])
			log.DefaultLogger.Infof(RouterLogFormat, "routerule", "finalizePathHeader", "add prefix to path, prefix is "+rri.prefixRewrite)
		}
	}
}

func (rri *RouteRuleImplBase) FinalizeRequestHeaders(headers api.HeaderMap, requestInfo api.RequestInfo) {
	rri.finalizeRequestHeaders(headers, requestInfo)
}

func (rri *RouteRuleImplBase) finalizeRequestHeaders(headers api.HeaderMap, requestInfo api.RequestInfo) {
	rri.requestHeadersParser.evaluateHeaders(headers, requestInfo)
	rri.vHost.requestHeadersParser.evaluateHeaders(headers, requestInfo)
	rri.vHost.globalRouteConfig.requestHeadersParser.evaluateHeaders(headers, requestInfo)
	if len(rri.hostRewrite) > 0 {
		headers.Set(protocol.IstioHeaderHostKey, rri.hostRewrite)
	} else if len(rri.autoHostRewriteHeader) > 0 {
		if headerValue, ok := headers.Get(rri.autoHostRewriteHeader); ok {
			headers.Set(protocol.IstioHeaderHostKey, headerValue)
		}
	} else if rri.autoHostRewrite {

		clusterSnapshot := cluster.GetClusterMngAdapterInstance().GetClusterSnapshot(context.TODO(), rri.routerAction.ClusterName)
		if clusterSnapshot != nil && (clusterSnapshot.ClusterInfo().ClusterType() == v2.STRICT_DNS_CLUSTER) {
			headers.Set(protocol.IstioHeaderHostKey, requestInfo.UpstreamHost().Hostname())
		}

	}
}

func (rri *RouteRuleImplBase) FinalizeResponseHeaders(headers api.HeaderMap, requestInfo api.RequestInfo) {
	rri.responseHeadersParser.evaluateHeaders(headers, requestInfo)
	rri.vHost.responseHeadersParser.evaluateHeaders(headers, requestInfo)
	rri.vHost.globalRouteConfig.responseHeadersParser.evaluateHeaders(headers, requestInfo)
}
