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
package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.telnet.TelnetHandler;
import org.apache.dubbo.remoting.telnet.support.TelnetHandlerAdapter;
import org.apache.dubbo.remoting.transport.ChannelHandlerDispatcher;

import java.util.concurrent.CompletableFuture;

/**
 * ExchangeHandlerDispatcher
 *
 * 交换处理 的 请求调度对象
 */
public class ExchangeHandlerDispatcher implements ExchangeHandler {

    /**
     * 组合了一个 回复调度对象
     */
    private final ReplierDispatcher replierDispatcher;

    /**
     * 组合一个 针对 底层 channel的 调度对象
     */
    private final ChannelHandlerDispatcher handlerDispatcher;

    /**
     * telnet 请求处理器
     */
    private final TelnetHandler telnetHandler;

    /**
     * 创建该对象时 同时 初始化3个对象
     */
    public ExchangeHandlerDispatcher() {
        replierDispatcher = new ReplierDispatcher();
        handlerDispatcher = new ChannelHandlerDispatcher();
        telnetHandler = new TelnetHandlerAdapter();
    }

    public ExchangeHandlerDispatcher(Replier<?> replier) {
        replierDispatcher = new ReplierDispatcher(replier);
        handlerDispatcher = new ChannelHandlerDispatcher();
        telnetHandler = new TelnetHandlerAdapter();
    }

    public ExchangeHandlerDispatcher(ChannelHandler... handlers) {
        replierDispatcher = new ReplierDispatcher();
        handlerDispatcher = new ChannelHandlerDispatcher(handlers);
        telnetHandler = new TelnetHandlerAdapter();
    }

    public ExchangeHandlerDispatcher(Replier<?> replier, ChannelHandler... handlers) {
        replierDispatcher = new ReplierDispatcher(replier);
        handlerDispatcher = new ChannelHandlerDispatcher(handlers);
        telnetHandler = new TelnetHandlerAdapter();
    }

    public ExchangeHandlerDispatcher addChannelHandler(ChannelHandler handler) {
        handlerDispatcher.addChannelHandler(handler);
        return this;
    }

    public ExchangeHandlerDispatcher removeChannelHandler(ChannelHandler handler) {
        handlerDispatcher.removeChannelHandler(handler);
        return this;
    }

    public <T> ExchangeHandlerDispatcher addReplier(Class<T> type, Replier<T> replier) {
        replierDispatcher.addReplier(type, replier);
        return this;
    }

    public <T> ExchangeHandlerDispatcher removeReplier(Class<T> type) {
        replierDispatcher.removeReplier(type);
        return this;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<Object> reply(ExchangeChannel channel, Object request) throws RemotingException {
        //通过 request 的类型 获取到replierDispatcher 中对应的  replier对象 然后执行reply 方法 返回一个object对象
        //并设置到 completableFuture对象中 返回
        return CompletableFuture.completedFuture(((Replier) replierDispatcher).reply(channel, request));
    }

    /**
     * 遍历 handler 中所有channelHandler 对象并 触发 连接后事件
     * @param channel channel.
     */
    @Override
    public void connected(Channel channel) {
        handlerDispatcher.connected(channel);
    }

    /**
     * 将 调开连接 的channel 传入到 调配器 中每个 handler 对象执行相关方法
     * @param channel channel.
     */
    @Override
    public void disconnected(Channel channel) {
        handlerDispatcher.disconnected(channel);
    }

    @Override
    public void sent(Channel channel, Object message) {
        handlerDispatcher.sent(channel, message);
    }

    @Override
    public void received(Channel channel, Object message) {
        handlerDispatcher.received(channel, message);
    }

    @Override
    public void caught(Channel channel, Throwable exception) {
        handlerDispatcher.caught(channel, exception);
    }

    @Override
    public String telnet(Channel channel, String message) throws RemotingException {
        return telnetHandler.telnet(channel, message);
    }

}
