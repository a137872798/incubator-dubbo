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
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ResponseFuture;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * DefaultMessageClient
 *
 * 协议头的 交换层 客户端抽象
 */
public class HeaderExchangeClient implements ExchangeClient {

    /**
     * 实际实现 client功能 的对象
     */
    private final Client client;
    /**
     * 交换层 channel 相比普通的 channel 到了request， response 模型的层面
     */
    private final ExchangeChannel channel;
    // heartbeat(ms), default value is 0 , won't execute a heartbeat.
    /**
     * 心跳检测 的时间 如果是0 就代表不需要
     */
    private int heartbeat;
    /**
     * 心跳检测的超时时间  只要在指定时间内 完成重连就可以
     */
    private int heartbeatTimeout;

    /**
     * 定时器对象
     */
    private HashedWheelTimer heartbeatTimer;

    public HeaderExchangeClient(Client client, boolean needHeartbeat) {
        if (client == null) {
            throw new IllegalArgumentException("client == null");
        }
        //根据 传入参数 初始化
        this.client = client;
        this.channel = new HeaderExchangeChannel(client);

        //获取 dubbo 相关属性
        String dubbo = client.getUrl().getParameter(Constants.DUBBO_VERSION_KEY);

        //如果 dubbo属性存在 且 为 以1.0 开头 就使用默认的  心跳时间
        this.heartbeat = client.getUrl().getParameter(Constants.HEARTBEAT_KEY, dubbo != null &&
                dubbo.startsWith("1.0.") ? Constants.DEFAULT_HEARTBEAT : 0);

        //默认心跳 检测时间 是 heartbeat 的 3倍
        this.heartbeatTimeout = client.getUrl().getParameter(Constants.HEARTBEAT_TIMEOUT_KEY, heartbeat * 3);
        //小于 2倍 就是异常情况
        if (heartbeatTimeout < heartbeat * 2) {
            throw new IllegalStateException("heartbeatTimeout < heartbeatInterval * 2");
        }

        //如果需要心跳检测
        if (needHeartbeat) {
            //计算 心跳检测的时间间隔
            long tickDuration = calculateLeastDuration(heartbeat);
            //初始化 定时器对象
            heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("dubbo-client-heartbeat", true), tickDuration,
                    TimeUnit.MILLISECONDS, Constants.TICKS_PER_WHEEL);
            //开始 心跳检测
            startHeartbeatTimer();
        }
    }

    @Override
    public ResponseFuture request(Object request) throws RemotingException {
        return channel.request(request);
    }

    @Override
    public URL getUrl() {
        return channel.getUrl();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    @Override
    public ResponseFuture request(Object request, int timeout) throws RemotingException {
        return channel.request(request, timeout);
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return channel.getExchangeHandler();
    }

    @Override
    public void send(Object message) throws RemotingException {
        channel.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        channel.send(message, sent);
    }

    @Override
    public boolean isClosed() {
        return channel.isClosed();
    }

    @Override
    public void close() {
        doClose();
        channel.close();
    }

    @Override
    public void close(int timeout) {
        // Mark the client into the closure process
        startClose();
        doClose();
        channel.close(timeout);
    }

    @Override
    public void startClose() {
        channel.startClose();
    }

    @Override
    public void reset(URL url) {
        client.reset(url);
    }

    /**
     * 增加指定参数后 开始 重置 以弃用
     * @param parameters
     */
    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        return channel.hasAttribute(key);
    }

    /**
     * 开始 心跳检测的 定时器
     */
    private void startHeartbeatTimer() {
        //通过传入 本 channel 初始化 channel 提供者 在定时任务中 调用处理逻辑
        AbstractTimerTask.ChannelProvider cp = () -> Collections.singletonList(HeaderExchangeClient.this);

        //计算 心跳 一次滴答的时间
        long heartbeatTick = calculateLeastDuration(heartbeat);
        //计算 心跳超时 的 滴答时间 只要在超时时间内完成重连就可以
        long heartbeatTimeoutTick = calculateLeastDuration(heartbeatTimeout);
        //前2个参数 代表第一次启动定时任务的 时间
        HeartbeatTimerTask heartBeatTimerTask = new HeartbeatTimerTask(cp, heartbeatTick, heartbeat);
        ReconnectTimerTask reconnectTimerTask = new ReconnectTimerTask(cp, heartbeatTimeoutTick, heartbeatTimeout);

        // init task and start timer.
        heartbeatTimer.newTimeout(heartBeatTimerTask, heartbeatTick, TimeUnit.MILLISECONDS);
        heartbeatTimer.newTimeout(reconnectTimerTask, heartbeatTimeoutTick, TimeUnit.MILLISECONDS);
    }

    /**
     * 停止定时器
     */
    private void stopHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.stop();
            heartbeatTimer = null;
        }
    }

    private void doClose() {
        stopHeartbeatTimer();
    }

    /**
     * Each interval cannot be less than 1000ms.
     * 计算 心跳检测的时间间隔
     */
    private long calculateLeastDuration(int time) {
        //时间 不满足 一次 滴答
        if (time / Constants.HEARTBEAT_CHECK_TICK <= 0) {
            //使用最小的时间间隔
            return Constants.LEAST_HEARTBEAT_DURATION;
        } else {
            //返回时间 / 滴答次数
            return time / Constants.HEARTBEAT_CHECK_TICK;
        }
    }

    @Override
    public String toString() {
        return "HeaderExchangeClient [channel=" + channel + "]";
    }
}
