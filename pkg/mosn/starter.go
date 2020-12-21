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

package mosn

import (
	"fmt"
	"net"
	"sync"

	admin "mosn.io/mosn/pkg/admin/server"
	"mosn.io/mosn/pkg/admin/store"
	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/configmanager"
	"mosn.io/mosn/pkg/featuregate"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/metrics"
	"mosn.io/mosn/pkg/metrics/shm"
	"mosn.io/mosn/pkg/metrics/sink"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/plugin"
	"mosn.io/mosn/pkg/router"
	"mosn.io/mosn/pkg/server"
	"mosn.io/mosn/pkg/server/keeper"
	"mosn.io/mosn/pkg/trace"
	"mosn.io/mosn/pkg/types"
	"mosn.io/mosn/pkg/upstream/cluster"
	"mosn.io/mosn/pkg/xds"
	"mosn.io/pkg/utils"
)

// Mosn class which wrapper server
type Mosn struct {
	servers        []server.Server
	clustermanager types.ClusterManager
	routerManager  types.RouterManager
	config         *v2.MOSNConfig
	adminServer    admin.Server
	xdsClient      *xds.Client
	wg             sync.WaitGroup
	// for smooth upgrade. reconfigure
	inheritListeners []net.Listener
	listenSockConn   net.Conn
}

// NewMosn
// Create server from mosn config
func NewMosn(c *v2.MOSNConfig) *Mosn {
	initializeDefaultPath(configmanager.GetConfigPath())
	initializePidFile(c.Pid)
	initializeTracing(c.Tracing)
	initializePlugin(c.Plugin.LogBase)

	store.SetMosnConfig(c)

	//get inherit fds
	//1.如果是第一次启动,那么返回的是nil,nil,nil;
	//2.无损迁移的话,返回的是需要监听的fd,以及与老mosn的连接(listen.sock)一会启动成功后,还得告诉老的mosn,老mson才好结束accept,关闭进程,否则10分钟后,认定为超时
	inheritListeners, listenSockConn, err := server.GetInheritListeners()
	if err != nil {
		log.StartLogger.Fatalf("[mosn] [NewMosn] getInheritListeners failed, exit")
	}
	if listenSockConn != nil {
		//2.1 如果new mosn与old mosn有连接,那么说明是平滑启动,需要继承老进程的listerner
		log.StartLogger.Infof("[mosn] [NewMosn] active reconfiguring")
		// set Mosn Active_Reconfiguring
		store.SetMosnState(store.Active_Reconfiguring)
		// parse MOSNConfig again
		c = configmanager.Load(configmanager.GetConfigPath())
	} else {
		//2.2 初始话服务
		log.StartLogger.Infof("[mosn] [NewMosn] new mosn created")
		// start init services
		//2.3 监听服务端口了~~~错了,并没有监听端口,因为进去service是nil
		if err := store.StartService(nil); err != nil {
			log.StartLogger.Fatalf("[mosn] [NewMosn] start service failed: %v,  exit", err)
		}
	}

	//===[ljl]3.初始化指标监控.使用了mmap,需要深入看下
	initializeMetrics(c.Metrics)

	m := &Mosn{
		config:           c,
		wg:               sync.WaitGroup{},
		inheritListeners: inheritListeners,
		listenSockConn:   listenSockConn,
	}
	mode := c.Mode()

	//4.如果是xds模式,那么初始化mosnConfig
	if mode == v2.Xds {
		c.Servers = []v2.ServerConfig{
			{
				DefaultLogPath:  "stdout",
				DefaultLogLevel: "INFO",
			},
		}
	} else {
		if len(c.ClusterManager.Clusters) == 0 && !c.ClusterManager.AutoDiscovery {
			log.StartLogger.Fatalf("[mosn] [NewMosn] no cluster found and cluster manager doesn't support auto discovery")
		}
	}

	//5.看下service的个数,如果是0,那么监听个寂寞.如果大于1,也扯淡,mosn不支持
	srvNum := len(c.Servers)

	if srvNum == 0 {
		log.StartLogger.Fatalf("[mosn] [NewMosn] no server found")
	} else if srvNum > 1 {
		log.StartLogger.Fatalf("[mosn] [NewMosn] multiple server not supported yet, got %d", srvNum)
	}

	//cluster manager filter
	cmf := &clusterManagerFilter{}

	// parse cluster all in one
	clusters, clusterMap := configmanager.ParseClusterConfig(c.ClusterManager.Clusters)
	// create cluster manager
	//6.1如果是从pilot获取xds,
	if mode == v2.Xds {
		m.clustermanager = cluster.NewClusterManagerSingleton(nil, nil)
	} else {
		//6.2 如果是静态配置的,那么解析静态配置
		m.clustermanager = cluster.NewClusterManagerSingleton(clusters, clusterMap)
	}

	//7.初始胡routerManager
	// initialize the routerManager
	m.routerManager = router.NewRouterManager()

	for _, serverConfig := range c.Servers {
		//1. server config prepare
		//server config
		c := configmanager.ParseServerConfig(&serverConfig)

		// new server config
		sc := server.NewConfig(c)

		// init default log
		server.InitDefaultLogger(sc)

		var srv server.Server
		if mode == v2.Xds {
			srv = server.NewServer(sc, cmf, m.clustermanager)
		} else {
			//initialize server instance
			srv = server.NewServer(sc, cmf, m.clustermanager)

			//add listener
			if len(serverConfig.Listeners) == 0 {
				log.StartLogger.Fatalf("[mosn] [NewMosn] no listener found")
			}

			for idx, _ := range serverConfig.Listeners {
				// parse ListenerConfig
				lc := configmanager.ParseListenerConfig(&serverConfig.Listeners[idx], inheritListeners)
				// deprecated: keep compatible for route config in listener's connection_manager
				deprecatedRouter, err := configmanager.ParseRouterConfiguration(&lc.FilterChains[0])
				if err != nil {
					log.StartLogger.Fatalf("[mosn] [NewMosn] compatible router: %v", err)
				}
				if deprecatedRouter.RouterConfigName != "" {
					m.routerManager.AddOrUpdateRouters(deprecatedRouter)
				}
				if _, err := srv.AddListener(lc); err != nil {
					log.StartLogger.Fatalf("[mosn] [NewMosn] AddListener error:%s", err.Error())
				}
			}
			// Add Router Config
			for _, routerConfig := range serverConfig.Routers {
				if routerConfig.RouterConfigName != "" {
					m.routerManager.AddOrUpdateRouters(routerConfig)
				}
			}
		}
		m.servers = append(m.servers, srv)
	}

	return m
}

// beforeStart prepares some actions before mosn start proxy listener
func (m *Mosn) beforeStart() {
	// start adminApi
	m.adminServer = admin.Server{}
	m.adminServer.Start(m.config)

	// SetTransferTimeout
	network.SetTransferTimeout(server.GracefulTimeout)

	if store.GetMosnState() == store.Active_Reconfiguring {
		// start other services
		if err := store.StartService(m.inheritListeners); err != nil {
			log.StartLogger.Fatalf("[mosn] [NewMosn] start service failed: %v,  exit", err)
		}

		// notify old mosn to transfer connection
		if _, err := m.listenSockConn.Write([]byte{0}); err != nil {
			log.StartLogger.Fatalf("[mosn] [NewMosn] graceful failed, exit")
		}

		m.listenSockConn.Close()
		fmt.Println("关闭oldmosn的连接")
		// transfer old mosn connections
		utils.GoWithRecover(func() {
			network.TransferServer(m.servers[0].Handler())
		}, nil)
	} else {
		// start other services
		if err := store.StartService(nil); err != nil {
			log.StartLogger.Fatalf("[mosn] [NewMosn] start service failed: %v,  exit", err)
		}
		store.SetMosnState(store.Running)
	}

	//close legacy listeners
	for _, ln := range m.inheritListeners {
		if ln != nil {
			log.StartLogger.Infof("[mosn] [NewMosn] close useless legacy listener: %s", ln.Addr().String())
			ln.Close()
		}
	}

	// start dump config process
	utils.GoWithRecover(func() {
		configmanager.DumpConfigHandler()
	}, nil)

	// start reconfig domain socket
	utils.GoWithRecover(func() {
		server.ReconfigureHandler()
	}, nil)
}

// Start mosn's server
func (m *Mosn) Start() {
	m.wg.Add(1)
	// Start XDS if configured
	log.StartLogger.Infof("mosn start xds client")
	m.xdsClient = &xds.Client{}
	utils.GoWithRecover(func() {
		m.xdsClient.Start(m.config)
	}, nil)
	// start mosn feature
	featuregate.StartInit()
	// TODO: remove it
	//parse service registry info
	log.StartLogger.Infof("mosn parse registry info")
	configmanager.ParseServiceRegistry(m.config.ServiceRegistry)

	// beforestart starts transfer connection and non-proxy listeners
	log.StartLogger.Infof("mosn prepare for start")
	m.beforeStart()

	// start mosn server
	log.StartLogger.Infof("mosn start server")
	for _, srv := range m.servers {

		// TODO
		// This can't be deleted, otherwise the code behind is equivalent at
		// utils.GoWithRecover(func() {
		//	 m.servers[0].Start()
		// },
		srv := srv
		//[ljl!!]这里才是启动服务
		utils.GoWithRecover(func() {
			srv.Start()
		}, nil)
	}
}

// Close mosn's server
func (m *Mosn) Close() {
	// close service
	store.CloseService()

	// stop reconfigure domain socket
	server.StopReconfigureHandler()

	// stop mosn server
	for _, srv := range m.servers {
		srv.Close()
	}
	m.xdsClient.Stop()
	m.clustermanager.Destroy()
	m.wg.Done()
}

// Start mosn project
// step1. NewMosn
// step2. Start Mosn
func Start(c *v2.MOSNConfig) {
	//log.StartLogger.Infof("[mosn] [start] start by config : %+v", c)
	//[ljl]1.初始化配置,fd迁移.生成一个mosn类,包装了server
	fmt.Println("[starter.go===服务启动]")
	Mosn := NewMosn(c)
	//[ljl]2.启动mosn
	Mosn.Start()
	Mosn.wg.Wait()
}

func initializeTracing(config v2.TracingConfig) {
	if config.Enable && config.Driver != "" {
		err := trace.Init(config.Driver, config.Config)
		if err != nil {
			log.StartLogger.Errorf("[mosn] [init tracing] init driver '%s' failed: %s, tracing functionality is turned off.", config.Driver, err)
			trace.Disable()
			return
		}
		log.StartLogger.Infof("[mosn] [init tracing] enable tracing")
		trace.Enable()
	} else {
		log.StartLogger.Infof("[mosn] [init tracing] disbale tracing")
		trace.Disable()
	}
}

func initializeMetrics(config v2.MetricsConfig) {
	// init shm zone
	if config.ShmZone != "" && config.ShmSize > 0 {
		shm.InitDefaultMetricsZone(config.ShmZone, int(config.ShmSize), store.GetMosnState() != store.Active_Reconfiguring)
	}

	// set metrics package
	statsMatcher := config.StatsMatcher
	metrics.SetStatsMatcher(statsMatcher.RejectAll, statsMatcher.ExclusionLabels, statsMatcher.ExclusionKeys)
	// create sinks
	for _, cfg := range config.SinkConfigs {
		_, err := sink.CreateMetricsSink(cfg.Type, cfg.Config)
		// abort
		if err != nil {
			log.StartLogger.Errorf("[mosn] [init metrics] %s. %v metrics sink is turned off", err, cfg.Type)
			return
		}
		log.StartLogger.Infof("[mosn] [init metrics] create metrics sink: %v", cfg.Type)
	}
}

func initializePidFile(pid string) {
	keeper.SetPid(pid)
}

func initializeDefaultPath(path string) {
	types.InitDefaultPath(path)
}

func initializePlugin(log string) {
	if log == "" {
		log = types.MosnLogBasePath
	}
	plugin.InitPlugin(log)
}

type clusterManagerFilter struct {
	cccb types.ClusterConfigFactoryCb
	chcb types.ClusterHostFactoryCb
}

func (cmf *clusterManagerFilter) OnCreated(cccb types.ClusterConfigFactoryCb, chcb types.ClusterHostFactoryCb) {
	cmf.cccb = cccb
	cmf.chcb = chcb
}
