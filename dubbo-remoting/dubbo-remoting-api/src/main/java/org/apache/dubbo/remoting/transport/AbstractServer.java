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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Server;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * AbstractServer
 * 抽象服务类
 *
 * server or client 初始化都需要 传入 channelHandler
 */
public abstract class AbstractServer extends AbstractEndpoint implements Server {

    protected static final String SERVER_THREAD_POOL_NAME = "DubboServerHandler";
    private static final Logger logger = LoggerFactory.getLogger(AbstractServer.class);
    /**
     * 线程池对象
     */
    ExecutorService executor;
    /**
     *  本地地址 和 绑定地址还能不一样吗???
     */
    private InetSocketAddress localAddress;
    private InetSocketAddress bindAddress;
    /**
     * 服务器 能接受的最大连接次数 应该是做限流的
     */
    private int accepts;
    /**
     * 心跳检测时间
     */
    private int idleTimeout = 600; //600 seconds

    public AbstractServer(URL url, ChannelHandler handler) throws RemotingException {
        super(url, handler);
        //从url 中 获取地址
        localAddress = getUrl().toInetSocketAddress();

        //获取 端口和ip
        String bindIp = getUrl().getParameter(Constants.BIND_IP_KEY, getUrl().getHost());
        int bindPort = getUrl().getParameter(Constants.BIND_PORT_KEY, getUrl().getPort());
        //如果 端口无效 或者 url 带有 anyhost  绑定到 0.0.0.0
        if (url.getParameter(Constants.ANYHOST_KEY, false) || NetUtils.isInvalidLocalHost(bindIp)) {
            bindIp = NetUtils.ANYHOST;
        }
        //创建新的地址对象
        bindAddress = new InetSocketAddress(bindIp, bindPort);
        //获取连接数和心跳检测时间
        this.accepts = url.getParameter(Constants.ACCEPTS_KEY, Constants.DEFAULT_ACCEPTS);
        this.idleTimeout = url.getParameter(Constants.IDLE_TIMEOUT_KEY, Constants.DEFAULT_IDLE_TIMEOUT);
        try {
            //执行启动方法 子类实现 服务端是不需要开启重连机制的
            doOpen();
            if (logger.isInfoEnabled()) {
                logger.info("Start " + getClass().getSimpleName() + " bind " + getBindAddress() + ", export " + getLocalAddress());
            }
        } catch (Throwable t) {
            throw new RemotingException(url.toInetSocketAddress(), null, "Failed to bind " + getClass().getSimpleName()
                    + " on " + getLocalAddress() + ", cause: " + t.getMessage(), t);
        }
        //fixme replace this with better method
        //从 dataStore 中 获取线程池对象  这个对象应该还没被初始化吧那么属性是哪里获取的
        DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        executor = (ExecutorService) dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY, Integer.toString(url.getPort()));
    }

    protected abstract void doOpen() throws Throwable;

    protected abstract void doClose() throws Throwable;

    /**
     * 根据 指定的url 重置 服务器
     * @param url
     */
    @Override
    public void reset(URL url) {
        if (url == null) {
            return;
        }
        try {
            if (url.hasParameter(Constants.ACCEPTS_KEY)) {
                int a = url.getParameter(Constants.ACCEPTS_KEY, 0);
                if (a > 0) {
                    this.accepts = a;
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        try {
            if (url.hasParameter(Constants.IDLE_TIMEOUT_KEY)) {
                int t = url.getParameter(Constants.IDLE_TIMEOUT_KEY, 0);
                if (t > 0) {
                    this.idleTimeout = t;
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        try {
            if (url.hasParameter(Constants.THREADS_KEY)
                    && executor instanceof ThreadPoolExecutor && !executor.isShutdown()) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
                int threads = url.getParameter(Constants.THREADS_KEY, 0);
                int max = threadPoolExecutor.getMaximumPoolSize();
                int core = threadPoolExecutor.getCorePoolSize();
                if (threads > 0 && (threads != max || threads != core)) {
                    if (threads < core) {
                        threadPoolExecutor.setCorePoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setMaximumPoolSize(threads);
                        }
                    } else {
                        threadPoolExecutor.setMaximumPoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setCorePoolSize(threads);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        super.setUrl(getUrl().addParameters(url.getParameters()));
    }

    /**
     * 为连接到本地的所有channel 发送消息
     * @param message
     * @param sent    already sent to socket?
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        Collection<Channel> channels = getChannels();
        for (Channel channel : channels) {
            if (channel.isConnected()) {
                channel.send(message, sent);
            }
        }
    }

    /**
     * 关闭 服务器
     */
    @Override
    public void close() {
        if (logger.isInfoEnabled()) {
            logger.info("Close " + getClass().getSimpleName() + " bind " + getBindAddress() + ", export " + getLocalAddress());
        }
        ExecutorUtil.shutdownNow(executor, 100);
        try {
            super.close();
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            doClose();
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public void close(int timeout) {
        //这里会阻塞一会当前线程
        ExecutorUtil.gracefulShutdown(executor, timeout);
        close();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    public InetSocketAddress getBindAddress() {
        return bindAddress;
    }

    public int getAccepts() {
        return accepts;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * 接受 客户端的 连接
     * @param ch
     * @throws RemotingException
     */
    @Override
    public void connected(Channel ch) throws RemotingException {
        // If the server has entered the shutdown process, reject any new connection
        // 如果当前状态就是关闭 就 关闭 channel
        if (this.isClosing() || this.isClosed()) {
            logger.warn("Close new channel " + ch + ", cause: server is closing or has been closed. For example, receive a new connect request while in shutdown process.");
            ch.close();
            return;
        }

        Collection<Channel> channels = getChannels();
        //如果超过 最大连接数
        if (accepts > 0 && channels.size() > accepts) {
            logger.error("Close channel " + ch + ", cause: The server " + ch.getLocalAddress() + " connections greater than max config " + accepts);
            ch.close();
            return;
        }
        //委托给 父类 连接
        super.connected(ch);
    }

    /**
     * 断开连接时
     * @param ch
     * @throws RemotingException
     */
    @Override
    public void disconnected(Channel ch) throws RemotingException {
        Collection<Channel> channels = getChannels();
        if (channels.isEmpty()) {
            logger.warn("All clients has discontected from " + ch.getLocalAddress() + ". You can graceful shutdown now.");
        }
        //委托
        super.disconnected(ch);
    }

}
