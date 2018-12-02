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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Server;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Request;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableCollection;

/**
 * ExchangeServerImpl
 *
 * 为 server 增加了 心跳检测功能
 */
public class HeaderExchangeServer implements ExchangeServer {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 关联的 服务器对象 用来实现 server 接口的 功能
     */
    private final Server server;
    // heartbeat timeout (ms), default value is 0 , won't execute a heartbeat.
    /**
     * 也存在心跳检测 是针对 所有客户端的
     */
    private int heartbeat;
    /**
     * 心跳检测的 超时时间
     */
    private int heartbeatTimeout;
    /**
     * 是否被关闭
     */
    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 心跳检测 的 定时任务对象
     */
    private HashedWheelTimer heartbeatTimer;

    public HeaderExchangeServer(Server server) {
        if (server == null) {
            throw new IllegalArgumentException("server == null");
        }
        this.server = server;
        //默认 不开启心跳检测
        this.heartbeat = server.getUrl().getParameter(Constants.HEARTBEAT_KEY, 0);
        this.heartbeatTimeout = server.getUrl().getParameter(Constants.HEARTBEAT_TIMEOUT_KEY, heartbeat * 3);
        if (heartbeatTimeout < heartbeat * 2) {
            throw new IllegalStateException("heartbeatTimeout < heartbeatInterval * 2");
        }

        //开启心跳检测 相关定时任务
        startHeartbeatTimer();
    }

    public Server getServer() {
        return server;
    }

    @Override
    public boolean isClosed() {
        return server.isClosed();
    }

    /**
     * 判断当前服务器是否处于运行状态 只要有一个 channel是连接着的 就返回true
     * @return
     */
    private boolean isRunning() {
        Collection<Channel> channels = getChannels();
        for (Channel channel : channels) {

            /**
             *  If there are any client connections,
             *  our server should be running.
             */

            if (channel.isConnected()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        doClose();
        server.close();
    }

    /**
     * 关闭 服务器
     * @param timeout
     */
    @Override
    public void close(final int timeout) {
        startClose();
        if (timeout > 0) {
            final long max = (long) timeout;
            final long start = System.currentTimeMillis();
            //如果 url 中设置了只读事件通知 就通知 客户端 这样 当客户端准备发起请求时 判断到 服务器不再接受新的请求 通过isAvailable
            if (getUrl().getParameter(Constants.CHANNEL_SEND_READONLYEVENT_KEY, true)) {
                //触发只读事件
                sendChannelReadOnlyEvent();
            }
            //在指定的 时间内 如果服务器还在运行中
            while (HeaderExchangeServer.this.isRunning()
                    && System.currentTimeMillis() - start < max) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
        //真正的关闭逻辑 也即使关闭 定时器
        doClose();
        server.close(timeout);
    }

    /**
     * close 之前 要 先 做准备工作
     */
    @Override
    public void startClose() {
        //就是 修改 AbstractPeer 的 closing 标识 避免被重复关闭
        server.startClose();
    }

    /**
     * 触发 channel 的 只读事件
     */
    private void sendChannelReadOnlyEvent() {
        //创建请求对象
        Request request = new Request();
        //设置 触发事件为 只读
        request.setEvent(Request.READONLY_EVENT);
        //oneway 代表不需要 响应
        request.setTwoWay(false);
        request.setVersion(Version.getProtocolVersion());

        //获取 所有channel 并发送 只读 事件请求
        Collection<Channel> channels = getChannels();
        //这个返回的实际是 headerexchangechannel
        for (Channel channel : channels) {
            try {
                if (channel.isConnected()) {
                    channel.send(request, getUrl().getParameter(Constants.CHANNEL_READONLYEVENT_SENT_KEY, true));
                }
            } catch (RemotingException e) {
                logger.warn("send cannot write message error.", e);
            }
        }
    }

    /**
     * 除了 处理channel 外的其他逻辑
     */
    private void doClose() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        //停止心跳定时器
        stopHeartbeatTimer();
    }

    /**
     * 获取 所有管理的channel
     * @return
     */
    @Override
    public Collection<ExchangeChannel> getExchangeChannels() {
        Collection<ExchangeChannel> exchangeChannels = new ArrayList<ExchangeChannel>();
        //从服务器对象 获取channel 容器 这里维护了 所有连接上 服务器的 channel对象
        Collection<Channel> channels = server.getChannels();
        if (channels != null && !channels.isEmpty()) {
            for (Channel channel : channels) {
                //获取channel 绑定的 exchangchannel
                exchangeChannels.add(HeaderExchangeChannel.getOrAddChannel(channel));
            }
        }
        return exchangeChannels;
    }

    /**
     * 根据 指定地址获取 channel 并为 这个channel 绑定一个 HeaderExchangeChannel
     * @param remoteAddress
     * @return
     */
    @Override
    public ExchangeChannel getExchangeChannel(InetSocketAddress remoteAddress) {
        Channel channel = server.getChannel(remoteAddress);
        return HeaderExchangeChannel.getOrAddChannel(channel);
    }

    /**
     * 获取 所有管理的channel 就是从server 对象中获取 连接的 channel
     * @return
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Collection<Channel> getChannels() {
        return (Collection) getExchangeChannels();
    }


    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return getExchangeChannel(remoteAddress);
    }

    @Override
    public boolean isBound() {
        return server.isBound();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return server.getLocalAddress();
    }

    @Override
    public URL getUrl() {
        return server.getUrl();
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return server.getChannelHandler();
    }

    /**
     * 根据传入的 url 重置属性
     */
    @Override
    public void reset(URL url) {
        //保留父类的重置功能
        server.reset(url);
        try {
            //如果传入的参数中 包含心跳检测的 相关数据
            if (url.hasParameter(Constants.HEARTBEAT_KEY)
                    || url.hasParameter(Constants.HEARTBEAT_TIMEOUT_KEY)) {
                int h = url.getParameter(Constants.HEARTBEAT_KEY, heartbeat);
                int t = url.getParameter(Constants.HEARTBEAT_TIMEOUT_KEY, h * 3);
                if (t < h * 2) {
                    throw new IllegalStateException("heartbeatTimeout < heartbeatInterval * 2");
                }
                //使用该参数重新进行心跳任务
                if (h != heartbeat || t != heartbeatTimeout) {
                    heartbeat = h;
                    heartbeatTimeout = t;

                    stopHeartbeatTimer();
                    startHeartbeatTimer();
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void send(Object message) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(this.getLocalAddress(), null, "Failed to send message " + message
                    + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        //这里会发送给 server下的所有channel 这里会根据url 的信息 自动判断是否要等待结果返回
        server.send(message);
    }

    /**
     * send 代表是否要阻塞io 线程等待结果返回
     * @param message
     * @param sent    already sent to socket?
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        if (closed.get()) {
            throw new RemotingException(this.getLocalAddress(), null, "Failed to send message " + message
                    + ", cause: The server " + getLocalAddress() + " is closed!");
        }
        server.send(message, sent);
    }

    /**
     * Each interval cannot be less than 1000ms.
     * 计算滴答时间
     */
    private long calculateLeastDuration(int time) {
        if (time / Constants.HEARTBEAT_CHECK_TICK <= 0) {
            return Constants.LEAST_HEARTBEAT_DURATION;
        } else {
            return time / Constants.HEARTBEAT_CHECK_TICK;
        }
    }

    /**
     * 开启心跳检测相关定时任务
     */
    private void startHeartbeatTimer() {
        //计算滴答时间
        long tickDuration = calculateLeastDuration(heartbeat);
        //对于定时器 这个滴答时间参数是做什么的???
        heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("dubbo-server-heartbeat", true), tickDuration,
                TimeUnit.MILLISECONDS, Constants.TICKS_PER_WHEEL);

        //该 channel 提供者 的实现就是从 服务器中 获取所有 channel 对象
        AbstractTimerTask.ChannelProvider cp = () -> unmodifiableCollection(HeaderExchangeServer.this.getChannels());

        //创建 心跳检测 任务 和 重连任务 并开启定时任务
        long heartbeatTick = calculateLeastDuration(heartbeat);
        long heartbeatTimeoutTick = calculateLeastDuration(heartbeatTimeout);
        HeartbeatTimerTask heartBeatTimerTask = new HeartbeatTimerTask(cp, heartbeatTick, heartbeat);
        ReconnectTimerTask reconnectTimerTask = new ReconnectTimerTask(cp, heartbeatTimeoutTick, heartbeatTimeout);

        // init task and start timer.
        heartbeatTimer.newTimeout(heartBeatTimerTask, heartbeatTick, TimeUnit.MILLISECONDS);
        heartbeatTimer.newTimeout(reconnectTimerTask, heartbeatTimeoutTick, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止定时器 并删除引用
     */
    private void stopHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.stop();
            heartbeatTimer = null;
        }
    }

}
