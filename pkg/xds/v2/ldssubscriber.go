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

package v2

import (
	"errors"
	"github.com/golang/protobuf/ptypes"

	envoy_api_v2 "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	envoy_api_v2_core1 "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	ads "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
)

func (c *ADSClient) reqListeners(streamClient ads.AggregatedDiscoveryService_StreamAggregatedResourcesClient) error {
	if streamClient == nil {
		return errors.New("stream client is nil")
	}
	err := streamClient.Send(&envoy_api_v2.DiscoveryRequest{
		VersionInfo:   "",
		ResourceNames: []string{},
		TypeUrl:       "type.googleapis.com/envoy.api.v2.Listener",
		ResponseNonce: "",
		ErrorDetail:   nil,
		Node: &envoy_api_v2_core1.Node{
			Id:       types.GetGlobalXdsInfo().ServiceNode,
			Cluster:  types.GetGlobalXdsInfo().ServiceCluster,
			Metadata: types.GetGlobalXdsInfo().Metadata,
		},
	})
	if err != nil {
		log.DefaultLogger.Infof("get listener fail: %v", err)
		return err
	}
	return nil
}

func (c *ADSClient) handleListenersResp(resp *envoy_api_v2.DiscoveryResponse) []*envoy_api_v2.Listener {
	listeners := make([]*envoy_api_v2.Listener, 0, len(resp.Resources))
	for _, res := range resp.Resources {
		listener := &envoy_api_v2.Listener{}
		if err := ptypes.UnmarshalAny(res, listener); err != nil {
			log.DefaultLogger.Errorf("ADSClient unmarshal listener fail: %v", err)
		}
		listeners = append(listeners, listener)
	}
	return listeners
}
