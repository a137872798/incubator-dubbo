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
package org.apache.dubbo.config;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.status.StatusChecker;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Dispatcher;
import org.apache.dubbo.remoting.Transporter;
import org.apache.dubbo.remoting.exchange.Exchanger;
import org.apache.dubbo.remoting.telnet.TelnetHandler;
import org.apache.dubbo.rpc.Protocol;

import java.util.Map;

/**
 * ProtocolConfig
 *
 * @export
 */
//协议 配置
public class ProtocolConfig extends AbstractConfig {

    private static final long serialVersionUID = 6913423882496634749L;

    /**
     * 协议名称
     */
    // protocol name
    private String name;

    /**
     * 服务主机名，多网卡选择或指定VIP及域名时使用，为空则自动查找本机IP，-建议不要配置，让Dubbo自动获取本机IP
     */
    // service IP address (when there are multiple network cards available)
    private String host;

    /**
     * 服务端口
     */
    // service port
    private Integer port;

    // context path
    private String contextpath;

    /**
     * 线程池 类型 fixed / cached
     */
    // thread pool
    private String threadpool;

    /**
     * 核心线程池数量
     */
    // thread pool core thread size
    private Integer corethreads;

    /**
     * 固定线程数量
     */
    // thread pool size (fixed size)
    private Integer threads;

    /**
     * io线程大小
     */
    // IO thread pool size (fixed size)
    private Integer iothreads;

    /**
     * 线程池队列大小
     */
    // thread pool's queue length
    private Integer queues;

    /**
     * 服务提供方 可接受的最大连接数 应该是做限流的
     */
    // max acceptable connections
    private Integer accepts;

    /**
     * 协议编码方式
     */
    // protocol codec
    private String codec;

    /**
     * 序列化方式
     */
    // serialization
    private String serialization;

    /**
     * 字符集
     */
    // charset
    private String charset;

    /**
     * 请求和响应数据包大小
     */
    // payload max length
    private Integer payload;

    /**
     * 网络缓冲区大小
     */
    // buffer size
    private Integer buffer;

    /**
     * 心跳检测间隔
     */
    // heartbeat interval
    private Integer heartbeat;

    // access log
    private String accesslog;

    /**
     * 协议传输 类型 netty/mina
     */
    // transfort
    private String transporter;

    // how information is exchanged
    private String exchanger;

    /**
     * 协议的消息派发方式，用于指定线程模型，比如：dubbo协议的all, direct, message, execution, connection等
     */
    // thread dispatch mode
    private String dispatcher;

    // networker
    private String networker;

    // sever impl
    private String server;

    // client impl
    private String client;

    // supported telnet commands, separated with comma.
    private String telnet;

    // command line prompt
    private String prompt;

    // status check
    private String status;

    /**
     * 该协议的服务是否注册到注册中心	
     */
    // whether to register
    private Boolean register;

    // parameters
    // 是否长连接
    // TODO add this to provider config
    private Boolean keepAlive;

    // TODO add this to provider config
    private String optimizer;

    private String extension;

    // parameters
    private Map<String, String> parameters;

    // if it's default
    private Boolean isDefault;

    public ProtocolConfig() {
    }

    public ProtocolConfig(String name) {
        setName(name);
    }

    public ProtocolConfig(String name, int port) {
        setName(name);
        setPort(port);
    }

    @Parameter(excluded = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkName("name", name);
        this.name = name;
        if (id == null || id.length() == 0) {
            id = name;
        }
    }

    @Parameter(excluded = true)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        checkName("host", host);
        this.host = host;
    }

    @Parameter(excluded = true)
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Deprecated
    @Parameter(excluded = true)
    public String getPath() {
        return getContextpath();
    }

    @Deprecated
    public void setPath(String path) {
        setContextpath(path);
    }

    @Parameter(excluded = true)
    public String getContextpath() {
        return contextpath;
    }

    public void setContextpath(String contextpath) {
        checkPathName("contextpath", contextpath);
        this.contextpath = contextpath;
    }

    public String getThreadpool() {
        return threadpool;
    }

    public void setThreadpool(String threadpool) {
        checkExtension(ThreadPool.class, "threadpool", threadpool);
        this.threadpool = threadpool;
    }

    public Integer getCorethreads() {
        return corethreads;
    }

    public void setCorethreads(Integer corethreads) {
        this.corethreads = corethreads;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public Integer getIothreads() {
        return iothreads;
    }

    public void setIothreads(Integer iothreads) {
        this.iothreads = iothreads;
    }

    public Integer getQueues() {
        return queues;
    }

    public void setQueues(Integer queues) {
        this.queues = queues;
    }

    public Integer getAccepts() {
        return accepts;
    }

    public void setAccepts(Integer accepts) {
        this.accepts = accepts;
    }

    public String getCodec() {
        return codec;
    }

    public void setCodec(String codec) {
        if ("dubbo".equals(name)) {
            checkMultiExtension(Codec.class, "codec", codec);
        }
        this.codec = codec;
    }

    public String getSerialization() {
        return serialization;
    }

    public void setSerialization(String serialization) {
        if ("dubbo".equals(name)) {
            checkMultiExtension(Serialization.class, "serialization", serialization);
        }
        this.serialization = serialization;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public Integer getPayload() {
        return payload;
    }

    public void setPayload(Integer payload) {
        this.payload = payload;
    }

    public Integer getBuffer() {
        return buffer;
    }

    public void setBuffer(Integer buffer) {
        this.buffer = buffer;
    }

    public Integer getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(Integer heartbeat) {
        this.heartbeat = heartbeat;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        if ("dubbo".equals(name)) {
            checkMultiExtension(Transporter.class, "server", server);
        }
        this.server = server;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        if ("dubbo".equals(name)) {
            checkMultiExtension(Transporter.class, "client", client);
        }
        this.client = client;
    }

    public String getAccesslog() {
        return accesslog;
    }

    public void setAccesslog(String accesslog) {
        this.accesslog = accesslog;
    }

    public String getTelnet() {
        return telnet;
    }

    public void setTelnet(String telnet) {
        checkMultiExtension(TelnetHandler.class, "telnet", telnet);
        this.telnet = telnet;
    }

    @Parameter(escaped = true)
    public String getPrompt() {
        return prompt;
    }

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        checkMultiExtension(StatusChecker.class, "status", status);
        this.status = status;
    }

    public Boolean isRegister() {
        return register;
    }

    public void setRegister(Boolean register) {
        this.register = register;
    }

    public String getTransporter() {
        return transporter;
    }

    public void setTransporter(String transporter) {
        checkExtension(Transporter.class, "transporter", transporter);
        this.transporter = transporter;
    }

    public String getExchanger() {
        return exchanger;
    }

    public void setExchanger(String exchanger) {
        checkExtension(Exchanger.class, "exchanger", exchanger);
        this.exchanger = exchanger;
    }

    /**
     * typo, switch to use {@link #getDispatcher()}
     *
     * @deprecated {@link #getDispatcher()}
     */
    @Deprecated
    @Parameter(excluded = true)
    public String getDispather() {
        return getDispatcher();
    }

    /**
     * typo, switch to use {@link #getDispatcher()}
     *
     * @deprecated {@link #setDispatcher(String)}
     */
    @Deprecated
    public void setDispather(String dispather) {
        setDispatcher(dispather);
    }

    public String getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(String dispatcher) {
        checkExtension(Dispatcher.class, "dispacther", dispatcher);
        this.dispatcher = dispatcher;
    }

    public String getNetworker() {
        return networker;
    }

    public void setNetworker(String networker) {
        this.networker = networker;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Boolean isDefault() {
        return isDefault;
    }

    public void setDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

    public Boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public String getOptimizer() {
        return optimizer;
    }

    public void setOptimizer(String optimizer) {
        this.optimizer = optimizer;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public void destroy() {
        if (name != null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(name).destroy();
        }
    }
}
