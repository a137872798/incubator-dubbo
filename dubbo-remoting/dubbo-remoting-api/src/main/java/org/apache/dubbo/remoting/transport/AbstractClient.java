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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelHandlers;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractClient
 * client 的骨架类
 */
public abstract class AbstractClient extends AbstractEndpoint implements Client {

    /**
     * 线程池 名
     */
    protected static final String CLIENT_THREAD_POOL_NAME = "DubboClientHandler";
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);
    /**
     * client 线程池id
     */
    private static final AtomicInteger CLIENT_THREAD_POOL_ID = new AtomicInteger();
    /**
     * 重连定时器
     */
    private static final ScheduledThreadPoolExecutor reconnectExecutorService = new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("DubboClientReconnectTimer", true));
    /**
     * 连接锁
     */
    private final Lock connectLock = new ReentrantLock();
    /**
     * 发送消息时 如果断开 是否重连
     */
    private final boolean send_reconnect;
    /**
     * 重连次数的 计数器
     */
    private final AtomicInteger reconnect_count = new AtomicInteger(0);
    /**
     * Reconnection error log has been called before?
     * 代表 是否打印过重连日志
     */
    private final AtomicBoolean reconnect_error_log_flag = new AtomicBoolean(false);
    // reconnect warning period. Reconnect warning interval (log warning after how many times) //for test
    /**
     * 重连的警告时间
     */
    private final int reconnect_warning_period;
    /**
     * 关闭的超时时间
     */
    private final long shutdown_timeout;
    /**
     * 线程池对象
     */
    protected volatile ExecutorService executor;
    /**
     * future 对象
     */
    private volatile ScheduledFuture<?> reconnectExecutorFuture = null;
    // the last successed connected time
    private long lastConnectedTime = System.currentTimeMillis();


    public AbstractClient(URL url, ChannelHandler handler) throws RemotingException {
        super(url, handler);

        //属性基本都是从url 中抽取出来的

        //是否在发送失败后重连
        send_reconnect = url.getParameter(Constants.SEND_RECONNECT_KEY, false);

        //终止的 超时时间
        shutdown_timeout = url.getParameter(Constants.SHUTDOWN_TIMEOUT_KEY, Constants.DEFAULT_SHUTDOWN_TIMEOUT);

        // The default reconnection interval is 2s, 1800 means warning interval is 1 hour.
        reconnect_warning_period = url.getParameter("reconnect.waring.period", 1800);

        try {
            //启动程序
            doOpen();
        } catch (Throwable t) {
            //异常就关闭
            close();
            throw new RemotingException(url.toInetSocketAddress(), null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(), t);
        }
        try {
            //开始连接
            connect();
            if (logger.isInfoEnabled()) {
                logger.info("Start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress() + " connect to the server " + getRemoteAddress());
            }
        } catch (RemotingException t) {
            //如果 开启了 检查 就 关闭 否则打印日志
            if (url.getParameter(Constants.CHECK_KEY, true)) {
                close();
                //这个t 不会被下面捕获到 而是抛到方法外
                throw t;
            } else {
                logger.warn("Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                        + " connect to the server " + getRemoteAddress() + " (check == false, ignore and retry later!), cause: " + t.getMessage(), t);
            }
        } catch (Throwable t) {
            close();
            throw new RemotingException(url.toInetSocketAddress(), null,
                    "Failed to start " + getClass().getSimpleName() + " " + NetUtils.getLocalAddress()
                            + " connect to the server " + getRemoteAddress() + ", cause: " + t.getMessage(), t);
        }

        //获取 默认的 拓展对象
        executor = (ExecutorService) ExtensionLoader.getExtensionLoader(DataStore.class)
                //从 simpleDataStore 中以 consumer 作为组件名 端口作为key 获取 线程池对象
                .getDefaultExtension().get(Constants.CONSUMER_SIDE, Integer.toString(url.getPort()));
        //移除线程池 关联关系 为什么???  server端没有移除
        ExtensionLoader.getExtensionLoader(DataStore.class)
                .getDefaultExtension().remove(Constants.CONSUMER_SIDE, Integer.toString(url.getPort()));
    }

    /**
     * 包装 handler
     * @param url
     * @param handler
     * @return
     */
    protected static ChannelHandler wrapChannelHandler(URL url, ChannelHandler handler) {
        //为 url 增加一个 携带 address 的 线程名 属性
        url = ExecutorUtil.setThreadName(url, CLIENT_THREAD_POOL_NAME);
        //如果没有线程池 属性 就 设置 默认为 cache
        url = url.addParameterIfAbsent(Constants.THREADPOOL_KEY, Constants.DEFAULT_CLIENT_THREADPOOL);
        //将 handler 包装一层
        return ChannelHandlers.wrap(handler, url);
    }

    /**
     * 从 url 中 获取 重连参数
     * @param url
     * @return 0-false
     */
    private static int getReconnectParam(URL url) {
        int reconnect;
        //获取 对应的参数值
        String param = url.getParameter(Constants.RECONNECT_KEY);
        if (param == null || param.length() == 0 || "true".equalsIgnoreCase(param)) {
            //使用默认的 参数
            reconnect = Constants.DEFAULT_RECONNECT_PERIOD;
        } else if ("false".equalsIgnoreCase(param)) {
            reconnect = 0;
        } else {
            try {
                reconnect = Integer.parseInt(param);
            } catch (Exception e) {
                throw new IllegalArgumentException("reconnect param must be nonnegative integer or false/true. input is:" + param);
            }
            if (reconnect < 0) {
                throw new IllegalArgumentException("reconnect param must be nonnegative integer or false/true. input is:" + param);
            }
        }
        return reconnect;
    }

    /**
     * init reconnect thread
     * 初始化 一些连接参数
     */
    private synchronized void initConnectStatusCheckCommand() {
        //reconnect=false to close reconnect
        //从url 中解析出 重连参数 >0 使用 该参数 没设置 或者是 true 就是用默认值 如果 为false 代表不进行重连
        int reconnect = getReconnectParam(getUrl());
        //代表使用重连 且 future 对象还没有被 初始化 所有重连时 重新调用connect 不会再创建新的 定时任务了
        if (reconnect > 0 && (reconnectExecutorFuture == null || reconnectExecutorFuture.isCancelled())) {
            Runnable connectStatusCheckCommand = new Runnable() {
                @Override
                public void run() {
                    try {
                        //这个 定时任务就是进行重连的
                        if (!isConnected()) {
                            //未连接 就进行连接
                            connect();
                        } else {
                            lastConnectedTime = System.currentTimeMillis();
                        }
                    } catch (Throwable t) {
                        String errorMsg = "client reconnect to " + getUrl().getAddress() + " find error . url: " + getUrl();
                        // wait registry sync provider list
                        // 如果 距离最后的重连时间 超过了 终结时间
                        if (System.currentTimeMillis() - lastConnectedTime > shutdown_timeout) {
                            //这里打印一次 error 信息
                            if (!reconnect_error_log_flag.get()) {
                                reconnect_error_log_flag.set(true);
                                logger.error(errorMsg, t);
                                return;
                            }
                        }
                        //重连次数 满足 重连 日志打印次数 打印日志
                        if (reconnect_count.getAndIncrement() % reconnect_warning_period == 0) {
                            logger.warn(errorMsg, t);
                        }
                    }
                }
            };
            //使用定时任务线程池 进行重连
            reconnectExecutorFuture = reconnectExecutorService.scheduleWithFixedDelay(connectStatusCheckCommand, reconnect, reconnect, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 当销毁连接时 执行 关闭 重连线程池 和任务对象
     */
    private synchronized void destroyConnectStatusCheckCommand() {
        try {
            //如果 future 对象还没有完成 就关闭 这个future 是 jdk 的future
            if (reconnectExecutorFuture != null && !reconnectExecutorFuture.isDone()) {
                reconnectExecutorFuture.cancel(true);
                //从线程池 中移除所有的 future 对象同时 如果 线程池中没有任务就会终止
                reconnectExecutorService.purge();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    protected ExecutorService createExecutor() {
        return Executors.newCachedThreadPool(new NamedThreadFactory(CLIENT_THREAD_POOL_NAME + CLIENT_THREAD_POOL_ID.incrementAndGet() + "-" + getUrl().getAddress(), true));
    }

    public InetSocketAddress getConnectAddress() {
        return new InetSocketAddress(NetUtils.filterLocalHost(getUrl().getHost()), getUrl().getPort());
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return getUrl().toInetSocketAddress();
        }
        return channel.getRemoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        Channel channel = getChannel();
        if (channel == null) {
            return InetSocketAddress.createUnresolved(NetUtils.getLocalHost(), 0);
        }
        return channel.getLocalAddress();
    }

    @Override
    public boolean isConnected() {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.isConnected();
    }

    @Override
    public Object getAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return null;
        }
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        Channel channel = getChannel();
        if (channel == null) {
            return false;
        }
        return channel.hasAttribute(key);
    }

    /**
     * 发送请求
     * @param message
     * @param sent    already sent to socket?
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        if (send_reconnect && !isConnected()) {
            //尝试 设置重连任务
            connect();
        }
        Channel channel = getChannel();
        //TODO Can the value returned by getChannel() be null? need improvement.
        if (channel == null || !channel.isConnected()) {
            throw new RemotingException(this, "message can not send, because channel is closed . url:" + getUrl());
        }
        //委托给 channel发送消息
        channel.send(message, sent);
    }

    /**
     * 开始连接
     * @throws RemotingException
     */
    protected void connect() throws RemotingException {
        //上锁 因为有多个方法 会 进行connect 要保证只有一个线程进行连接
        connectLock.lock();
        try {
            //已连接 直接返回  就是判断 channel 属性是否存在 就是 服务提供者的 channel
            if (isConnected()) {
                return;
            }
            //设置 定时 重连任务
            initConnectStatusCheckCommand();
            //实际的 连接方法 交由子类实现
            doConnect();
            //连接失败 抛出异常
            if (!isConnected()) {
                throw new RemotingException(this, "Failed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                        + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                        + ", cause: Connect wait timeout: " + getConnectTimeout() + "ms.");
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Successed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                            + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                            + ", channel is " + this.getChannel());
                }
            }
            //设置 重连次数
            reconnect_count.set(0);
            //重置为 未打印过日志
            reconnect_error_log_flag.set(false);
        } catch (RemotingException e) {
            throw e;
        } catch (Throwable e) {
            throw new RemotingException(this, "Failed connect to server " + getRemoteAddress() + " from " + getClass().getSimpleName() + " "
                    + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion()
                    + ", cause: " + e.getMessage(), e);
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        connectLock.lock();
        try {
            //销毁连接时的状态检查
            destroyConnectStatusCheckCommand();
            try {
                //关闭 channel
                Channel channel = getChannel();
                if (channel != null) {
                    channel.close();
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
            try {
                //抽象方法 子类实现
                doDisConnect();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
            }
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 重连
     * @throws RemotingException
     */
    @Override
    public void reconnect() throws RemotingException {
        if (!isConnected()) {
            connectLock.lock();
            try {
                if (!isConnected()) {
                    disconnect();
                    connect();
                }
            } finally {
                connectLock.unlock();
            }
        }
    }

    /**
     * 立即关闭线程池 并执行其他的关闭工作
     */
    @Override
    public void close() {
        try {
            if (executor != null) {
                ExecutorUtil.shutdownNow(executor, 100);
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            super.close();
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            disconnect();
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            doClose();
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 延时 关闭线程池 并执行其他的关闭工作
     * @param timeout
     */
    @Override
    public void close(int timeout) {
        ExecutorUtil.gracefulShutdown(executor, timeout);
        close();
    }

    @Override
    public String toString() {
        return getClass().getName() + " [" + getLocalAddress() + " -> " + getRemoteAddress() + "]";
    }

    /**
     * Open client.
     *
     * @throws Throwable
     */
    protected abstract void doOpen() throws Throwable;

    /**
     * Close client.
     *
     * @throws Throwable
     */
    protected abstract void doClose() throws Throwable;

    /**
     * Connect to server.
     *
     * @throws Throwable
     */
    protected abstract void doConnect() throws Throwable;

    /**
     * disConnect to server.
     *
     * @throws Throwable
     */
    protected abstract void doDisConnect() throws Throwable;

    /**
     * Get the connected channel.
     *
     * @return channel
     */
    protected abstract Channel getChannel();

}
