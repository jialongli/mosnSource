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

package server

import (
	"fmt"
	"os"
	"runtime/debug"
	"syscall"
	"time"

	"net"

	"mosn.io/mosn/pkg/admin/store"
	"mosn.io/mosn/pkg/configmanager"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/server/keeper"
	"mosn.io/mosn/pkg/types"
)

/**

这个类是平滑启动的类
1.init里面会监听 sighup信号,回调reconfigure()函数
2.mosn启动后,也会启动reconfigHandler.监听uds端口

*/
func init() {
	keeper.AddSignalCallback(syscall.SIGHUP, func() {
		// reload, fork new mosn
		reconfigure(true)
	})
}

var GracefulTimeout = time.Second * 30 //default 30s

func startNewMosn() error {
	execSpec := &syscall.ProcAttr{
		Env:   os.Environ(),
		Files: append([]uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()}),
	}

	// Fork exec the new version of your server
	fork, err := syscall.ForkExec(os.Args[0], os.Args, execSpec)
	if err != nil {
		log.DefaultLogger.Errorf("[server] [reconfigure] Fail to fork %v", err)
		return err
	}

	log.DefaultLogger.Infof("[server] [reconfigure] SIGHUP received: fork-exec to %d", fork)
	return nil
}

/**

 */
func reconfigure(start bool) {
	//======[ljl]如果是启动,调用这个逻辑 ,否则那肯定是有新mosn启动,我监听到uds的请求了.
	if start {
		startNewMosn()
		return
	}
	// set mosn State Passive_Reconfiguring
	store.SetMosnState(store.Passive_Reconfiguring)
	// if reconfigure failed, set mosn state to Running
	defer store.SetMosnState(store.Running)

	// dump lastest config, and stop DumpConfigHandler()
	configmanager.DumpLock()
	configmanager.DumpConfig()
	// if reconfigure failed, enable DumpConfigHandler()
	defer configmanager.DumpUnlock()

	// transfer listen fd
	var listenSockConn net.Conn
	var err error
	var n int
	var buf [1]byte
	//====[ljl]重要逻辑,发送要监听的文件描述符==========
	fmt.Println("[优雅重启,老进程]发送正在监听的fd列表=============")
	if listenSockConn, err = sendInheritListeners(); err != nil {
		return
	}

	// Wait new mosn parse configuration
	listenSockConn.SetReadDeadline(time.Now().Add(10 * time.Minute))
	fmt.Println("[优雅重启,老进程]发送正在监听的fd列表结束.等待新mosn发送启动成功的通知,我好去关闭accept=============")
	n, err = listenSockConn.Read(buf[:])
	fmt.Println("[优雅重启,老进程]发送正在监听的fd列表结束.等待新mosn发送启动成功的通知,我好去关闭accept,等待中=============")
	if n != 1 {
		fmt.Println("[优雅重启,老进程]发送正在监听的fd列表结束.等待新mosn发送启动成功的通知,我好去关闭accept,n!=1,返回失败=============")
		log.DefaultLogger.Alertf(types.ErrorKeyReconfigure, "new mosn start failed")
		return
	}

	fmt.Println("[优雅重启,老进程]stopService,start=============")
	// stop other services
	store.StopService()
	fmt.Println("[优雅重启,老进程]stopService,send=============")

	fmt.Println("[优雅重启,老进程]sleep 3s =============")
	// Wait for new mosn start
	time.Sleep(3 * time.Second)

	fmt.Println("[优雅重启,老进程]停止accept =============")
	// Stop accepting requests
	StopAccept()

	fmt.Println("[优雅重启,老进程]fsdfjasldfj =============")
	// Wait for all connections to be finished
	WaitConnectionsDone(GracefulTimeout)

	log.DefaultLogger.Infof("[server] [reconfigure] process %d gracefully shutdown", os.Getpid())

	keeper.ExecuteShutdownCallbacks("")

	// Stop the old server, all the connections have been closed and the new one is running
	os.Exit(0)
}

func ReconfigureHandler() {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[server] [reconfigure] transferServer panic %v\n%s", r, string(debug.Stack()))
		}
	}()
	time.Sleep(time.Second)
	fmt.Println("启动成功后,删除reconfig.sock")
	syscall.Unlink(types.ReconfigureDomainSocket)
	//=====1.mosn启动后,来监听reconfig这个uds
	l, err := net.Listen("unix", types.ReconfigureDomainSocket)
	if err != nil {
		log.StartLogger.Errorf("[server] [reconfigure] reconfigureHandler net listen error: %v", err)
		return
	}
	defer l.Close()

	log.DefaultLogger.Infof("[server] [reconfigure] reconfigureHandler start")

	ul := l.(*net.UnixListener)
	fmt.Println("启动成功后,监听reconfig.sock,阻塞中=============")
	for {
		//=======2.当accept到连接后.
		uc, err := ul.AcceptUnix()
		fmt.Println("!!!!!!监听reconfig.sock,收到请求,有新的mosn进程启动了=============")
		if err != nil {
			log.DefaultLogger.Errorf("[server] [reconfigure] reconfigureHandler Accept error :%v", err)
			return
		}
		log.DefaultLogger.Infof("[server] [reconfigure] reconfigureHandler Accept")

		//3.写入'0'数据作为回应
		_, err = uc.Write([]byte{0})
		fmt.Println("写回一个'0'作为回应=============")
		if err != nil {
			log.DefaultLogger.Errorf("[server] [reconfigure] reconfigureHandler %v", err)
			continue
		}
		//4.可以关闭连接了
		uc.Close()

		//5.
		reconfigure(false)
	}
}

func StopReconfigureHandler() {
	syscall.Unlink(types.ReconfigureDomainSocket)
}

/**
[ljl]判断当前机器是否已经有一个 mosn进程了.
怎么判断呢?mosn在启动后,会建立一个监听事件reconfig.sock.
这里去请求reconfig.sock,如果能建立连接成功,那么就说明此时已经有一个mosn进程了.
*/
func isReconfigure() bool {
	var unixConn net.Conn
	var err error
	unixConn, err = net.DialTimeout("unix", types.ReconfigureDomainSocket, 1*time.Second)
	if err != nil {
		log.DefaultLogger.Infof("[server] [reconfigure] not reconfigure: %v", err)
		return false
	}
	defer unixConn.Close()
	//在restart函数中,mosn启动最后,会server.ReconfigureHandler(). 会监听reconfig.sock.  当请求时,会
	uc := unixConn.(*net.UnixConn)
	buf := make([]byte, 1)
	n, _ := uc.Read(buf)
	if n != 1 {
		return false
	}
	return true
}
