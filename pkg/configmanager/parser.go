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

package configmanager

import (
	"encoding/json"
	"net"
	"os"
	"runtime"
	"strconv"

	"mosn.io/api"
	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol"
)

type ContentKey string

var ProtocolsSupported = map[string]bool{
	string(protocol.Auto):      true,
	string(protocol.HTTP1):     true,
	string(protocol.HTTP2):     true,
	string(protocol.Xprotocol): true,
}

const (
	MinHostWeight               = uint32(1)
	MaxHostWeight               = uint32(128)
	DefaultMaxRequestPerConn    = uint32(1024)
	DefaultConnBufferLimitBytes = uint32(16 * 1024)
)

// RegisterProtocolParser
// used to register parser
func RegisterProtocolParser(key string) bool {
	if _, ok := ProtocolsSupported[key]; ok {
		return false
	}
	log.StartLogger.Infof("[config] %s added to ProtocolsSupported", key)
	ProtocolsSupported[key] = true
	return true
}

// ParsedCallback is an
// alias for closure func(data interface{}, endParsing bool) error
type ParsedCallback func(data interface{}, endParsing bool) error

var configParsedCBMaps = make(map[ContentKey][]ParsedCallback)

// Group of ContentKey
// notes: configcontentkey equals to the key of config file
const (
	ParseCallbackKeyCluster        ContentKey = "clusters"
	ParseCallbackKeyServiceRgtInfo ContentKey = "service_registry"
	ParseCallbackKeyProcessor      ContentKey = "processor"
)

// RegisterConfigParsedListener
// used to register ParsedCallback
func RegisterConfigParsedListener(key ContentKey, cb ParsedCallback) {
	if cbs, ok := configParsedCBMaps[key]; ok {
		cbs = append(cbs, cb)
		// append maybe change the slice, should be assigned again
		configParsedCBMaps[key] = cbs
	} else {
		log.StartLogger.Infof("[config] %s added to configParsedCBMaps", key)
		cpc := []ParsedCallback{cb}
		configParsedCBMaps[key] = cpc
	}
}

// ParseClusterConfig parses config data to api data, verify whether the config is valid
func ParseClusterConfig(clusters []v2.Cluster) ([]v2.Cluster, map[string][]v2.Host) {
	if len(clusters) == 0 {
		log.StartLogger.Warnf("[config] [parse cluster] No Cluster provided in cluster config")
	}
	var pClusters []v2.Cluster
	clusterV2Map := make(map[string][]v2.Host)
	for _, c := range clusters {
		if c.Name == "" {
			log.StartLogger.Fatalf("[config] [parse cluster] name is required in cluster config")
		}
		if c.MaxRequestPerConn == 0 {
			c.MaxRequestPerConn = DefaultMaxRequestPerConn
			log.StartLogger.Infof("[config] [parse cluster] max_request_per_conn is not specified, use default value %d",
				DefaultMaxRequestPerConn)
		}
		if c.ConnBufferLimitBytes == 0 {
			c.ConnBufferLimitBytes = DefaultConnBufferLimitBytes
			log.StartLogger.Infof("[config] [parse cluster] conn_buffer_limit_bytes is not specified, use default value %d",
				DefaultConnBufferLimitBytes)
		}
		if c.LBSubSetConfig.FallBackPolicy > 2 {
			log.StartLogger.Fatalf("[config] [parse cluster] lb subset config 's fall back policy set error. " +
				"For 0, represent NO_FALLBACK" +
				"For 1, represent ANY_ENDPOINT" +
				"For 2, represent DEFAULT_SUBSET")
		}
		if _, ok := ProtocolsSupported[c.HealthCheck.Protocol]; !ok && c.HealthCheck.Protocol != "" {
			log.StartLogger.Fatalf("[config] [parse cluster] unsupported health check protocol: %v", c.HealthCheck.Protocol)
		}
		c.Hosts = parseHostConfig(c.Hosts)
		clusterV2Map[c.Name] = c.Hosts
		pClusters = append(pClusters, c)
	}
	// trigger all callbacks
	if cbs, ok := configParsedCBMaps[ParseCallbackKeyCluster]; ok {
		for _, cb := range cbs {
			cb(pClusters, false)
		}
	}

	return pClusters, clusterV2Map
}

func parseHostConfig(hosts []v2.Host) (hs []v2.Host) {
	for _, host := range hosts {
		host.Weight = transHostWeight(host.Weight)
		hs = append(hs, host)
	}
	return
}

func transHostWeight(weight uint32) uint32 {
	if weight > MaxHostWeight {
		return MaxHostWeight
	}
	if weight < MinHostWeight {
		return MinHostWeight
	}
	return weight
}

var logLevelMap = map[string]log.Level{
	"TRACE": log.TRACE,
	"DEBUG": log.DEBUG,
	"FATAL": log.FATAL,
	"ERROR": log.ERROR,
	"WARN":  log.WARN,
	"INFO":  log.INFO,
}

func ParseLogLevel(level string) log.Level {
	if logLevel, ok := logLevelMap[level]; ok {
		return logLevel
	}
	return log.INFO
}

// ParseListenerConfig
func ParseListenerConfig(lc *v2.Listener, inheritListeners []net.Listener) *v2.Listener {
	if lc.AddrConfig == "" {
		log.StartLogger.Fatalf("[config] [parse listener] Address is required in listener config")
	}
	addr, err := net.ResolveTCPAddr("tcp", lc.AddrConfig)
	if err != nil {
		log.StartLogger.Fatalf("[config] [parse listener] Address not valid: %v", lc.AddrConfig)
	}
	//try inherit legacy listener
	var old *net.TCPListener

	for i, il := range inheritListeners {
		if il == nil {
			continue
		}
		tl := il.(*net.TCPListener)
		ilAddr, err := net.ResolveTCPAddr("tcp", tl.Addr().String())
		if err != nil {
			log.StartLogger.Fatalf("[config] [parse listener] inheritListener not valid: %s", tl.Addr().String())
		}

		if addr.Port != ilAddr.Port {
			continue
		}

		if (addr.IP.IsUnspecified() && ilAddr.IP.IsUnspecified()) ||
			(addr.IP.IsLoopback() && ilAddr.IP.IsLoopback()) ||
			addr.IP.Equal(ilAddr.IP) {
			log.StartLogger.Infof("[config] [parse listener] inherit listener addr: %s", lc.AddrConfig)
			old = tl
			inheritListeners[i] = nil
			break
		}
	}

	lc.Addr = addr
	lc.PerConnBufferLimitBytes = 1 << 15
	lc.InheritListener = old
	return lc
}

func ParseRouterConfiguration(c *v2.FilterChain) (*v2.RouterConfiguration, error) {
	routerConfiguration := &v2.RouterConfiguration{}
	for _, f := range c.Filters {
		if f.Type == v2.CONNECTION_MANAGER {
			data, err := json.Marshal(f.Config)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(data, routerConfiguration); err != nil {
				return nil, err
			}
		}
	}
	return routerConfiguration, nil

}

func ParseServiceRegistry(src v2.ServiceRegistryInfo) {
	//trigger all callbacks
	if cbs, ok := configParsedCBMaps[ParseCallbackKeyServiceRgtInfo]; ok {
		for _, cb := range cbs {
			cb(src, true)
		}
	}
}

// ParseServerConfig
func ParseServerConfig(c *v2.ServerConfig) *v2.ServerConfig {
	if n, _ := strconv.Atoi(os.Getenv("GOMAXPROCS")); n > 0 && n <= runtime.NumCPU() {
		c.Processor = n
	} else if c.Processor == 0 {
		c.Processor = runtime.NumCPU()
	}

	// trigger processor callbacks
	if cbs, ok := configParsedCBMaps[ParseCallbackKeyProcessor]; ok {
		for _, cb := range cbs {
			cb(c.Processor, true)
		}
	}
	return c
}

// GetListenerFilters returns a listener filter factory by filter.Type
func GetListenerFilters(configs []v2.Filter) []api.ListenerFilterChainFactory {
	var factories []api.ListenerFilterChainFactory

	for _, c := range configs {
		sfcc, err := api.CreateListenerFilterChainFactory(c.Type, c.Config)
		if err != nil {
			log.DefaultLogger.Errorf("[config] get listener filter failed, type: %s, error: %v", c.Type, err)
			continue
		}
		factories = append(factories, sfcc)
	}

	return factories
}

// GetStreamFilters returns a stream filter factory by filter.Type
func GetStreamFilters(configs []v2.Filter) []api.StreamFilterChainFactory {
	var factories []api.StreamFilterChainFactory

	for _, c := range configs {
		sfcc, err := api.CreateStreamFilterChainFactory(c.Type, c.Config)
		if err != nil {
			log.DefaultLogger.Errorf("[config] get stream filter failed, type: %s, error: %v", c.Type, err)
			continue
		}
		factories = append(factories, sfcc)
	}

	return factories
}

// GetNetworkFilters returns a network filter factory by filter.Type
func GetNetworkFilters(c *v2.FilterChain) []api.NetworkFilterChainFactory {
	var factories []api.NetworkFilterChainFactory
	for _, f := range c.Filters {
		factory, err := api.CreateNetworkFilterChainFactory(f.Type, f.Config)
		if err != nil {
			log.StartLogger.Errorf("[config] network filter create failed, type:%s, error: %v", f.Type, err)
			continue
		}
		if factory != nil {
			factories = append(factories, factory)
		}
	}
	return factories
}
