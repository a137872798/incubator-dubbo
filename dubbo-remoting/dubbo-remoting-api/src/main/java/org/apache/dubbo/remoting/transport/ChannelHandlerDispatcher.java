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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * ChannelListenerDispatcher
 * channelHandler 的 请求分发对象 应该是 循环调用 容器中每个对象的 相关方法
 */
public class ChannelHandlerDispatcher implements ChannelHandler {

    private static final Logger logger = LoggerFactory.getLogger(ChannelHandlerDispatcher.class);

    /**
     * 存放 handler的容器对象
     */
    private final Collection<ChannelHandler> channelHandlers = new CopyOnWriteArraySet<ChannelHandler>();

    public ChannelHandlerDispatcher() {
    }

    /**
     * 通过一组 handler 对象初始化 handler容器
     * @param handlers
     */
    public ChannelHandlerDispatcher(ChannelHandler... handlers) {
        this(handlers == null ? null : Arrays.asList(handlers));
    }

    /**
     * 通过一组 handler 对象初始化 handler容器
     * @param handlers
     */
    public ChannelHandlerDispatcher(Collection<ChannelHandler> handlers) {
        if (handlers != null && !handlers.isEmpty()) {
            this.channelHandlers.addAll(handlers);
        }
    }

    public Collection<ChannelHandler> getChannelHandlers() {
        return channelHandlers;
    }

    /**
     * 给容器 增加 handler 对象
     * @param handler
     * @return
     */
    public ChannelHandlerDispatcher addChannelHandler(ChannelHandler handler) {
        this.channelHandlers.add(handler);
        return this;
    }

    /**
     * 从容器中移除 handler 对象
     * @param handler
     * @return
     */
    public ChannelHandlerDispatcher removeChannelHandler(ChannelHandler handler) {
        this.channelHandlers.remove(handler);
        return this;
    }

    /**
     * 遍历容器对象 执行相关方法
     * @param channel channel.
     */
    @Override
    public void connected(Channel channel) {
        for (ChannelHandler listener : channelHandlers) {
            try {
                listener.connected(channel);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    /**
     * 注销连接
     * @param channel channel.
     */
    @Override
    public void disconnected(Channel channel) {
        for (ChannelHandler listener : channelHandlers) {
            try {
                listener.disconnected(channel);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    /**
     * 触发发送完后的回调事件
     * @param channel channel.
     * @param message message.
     */
    @Override
    public void sent(Channel channel, Object message) {
        for (ChannelHandler listener : channelHandlers) {
            try {
                listener.sent(channel, message);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    /**
     * 触发发送完后的回调事件
     * @param channel channel.
     * @param message message.
     */
    @Override
    public void received(Channel channel, Object message) {
        for (ChannelHandler listener : channelHandlers) {
            try {
                listener.received(channel, message);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    /**
     * 触发发送完后的回调事件
     * @param channel channel.
     * @param exception exception.
     */
    @Override
    public void caught(Channel channel, Throwable exception) {
        for (ChannelHandler listener : channelHandlers) {
            try {
                listener.caught(channel, exception);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        }
    }

}
