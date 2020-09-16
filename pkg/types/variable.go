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

package types

// [Proxy]: the identification of a request info's content
const (
	VarStartTime                string = "start_time"
	VarRequestReceivedDuration  string = "request_received_duration"
	VarResponseReceivedDuration string = "response_received_duration"
	VarRequestFinishedDuration  string = "request_finished_duration"
	VarBytesSent                string = "bytes_sent"
	VarBytesReceived            string = "bytes_received"
	VarProtocol                 string = "protocol"
	VarResponseCode             string = "response_code"
	VarDuration                 string = "duration"
	VarResponseFlag             string = "response_flag"
	VarUpstreamLocalAddress     string = "upstream_local_address"
	VarDownstreamLocalAddress   string = "downstream_local_address"
	VarDownstreamRemoteAddress  string = "downstream_remote_address"
	VarUpstreamHost             string = "upstream_host"

	// ReqHeaderPrefix is the prefix of request header's formatter
	VarPrefixReqHeader string = "request_header_"
	// RespHeaderPrefix is the prefix of response header's formatter
	VarPrefixRespHeader string = "response_header_"
)

// [Proxy]: internal communication
const (
	VarProxyTryTimeout    string = "proxy_try_timeout"
	VarProxyGlobalTimeout string = "proxy_global_timeout"
	VarProxyHijackStatus  string = "proxy_hijack_status"
	VarProxyGzipSwitch    string = "proxy_gzip_switch"
)

// [Protocol]: common
const (
	VarProtocolRequestMethod    = "request_method"
	VarProtocolRequestLength    = "request_length"
	VarProtocolRequestHeader    = "request_header_"
	VarProtocolCookie           = "cookie_"
	VarProtocolRequestPath      = "request_path"
	VarProtocolRequestArgPrefix = "request_arg_"
	VarProtocolRequestArg       = "request_arg"
	VarProtocolRequestUri       = "request_uri"
)

// [Protocol]: http1
const (
	// the httpProtocolName value is protocol.HTTP1
	httpProtocolName     = "Http1"
	VarHttpRequestMethod = httpProtocolName + "_" + VarProtocolRequestMethod
	VarHttpRequestLength = httpProtocolName + "_" + VarProtocolRequestLength
	VarHttpRequestUri    = httpProtocolName + "_" + VarProtocolRequestUri
	VarHttpRequestPath   = httpProtocolName + "_" + VarProtocolRequestPath
	VarHttpRequestArg    = httpProtocolName + "_" + VarProtocolRequestArg
	VarPrefixHttpHeader  = httpProtocolName + "_" + VarProtocolRequestHeader
	VarPrefixHttpArg     = httpProtocolName + "_" + VarProtocolRequestArgPrefix
	VarPrefixHttpCookie  = httpProtocolName + "_" + VarProtocolCookie
)

// [Protocol]: http2
const (
	// the http2ProtocolName value is protocol.HTTP2
	http2ProtocolName     = "Http2"
	VarHttp2RequestMethod = http2ProtocolName + "_" + VarProtocolRequestMethod
	VarHttp2RequestLength = http2ProtocolName + "_" + VarProtocolRequestLength
	VarHttp2RequestUri    = http2ProtocolName + "_" + VarProtocolRequestUri
	VarHttp2RequestPath   = http2ProtocolName + "_" + VarProtocolRequestPath
	VarHttp2RequestArg    = http2ProtocolName + "_" + VarProtocolRequestArg
	VarPrefixHttp2Header  = http2ProtocolName + "_" + VarProtocolRequestHeader
	VarPrefixHttp2Arg     = http2ProtocolName + "_" + VarProtocolRequestArgPrefix
	VarPrefixHttp2Cookie  = http2ProtocolName + "_" + VarProtocolCookie
)
