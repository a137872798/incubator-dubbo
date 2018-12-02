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
package org.apache.dubbo.remoting.transport.dispatcher;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Dispatcher;
import org.apache.dubbo.remoting.exchange.support.header.HeartbeatHandler;
import org.apache.dubbo.remoting.transport.MultiMessageHandler;

public class ChannelHandlers {

    /**
     * 单例
     */
    private static ChannelHandlers INSTANCE = new ChannelHandlers();

    protected ChannelHandlers() {
    }

    /**
     * 为 本 channelHandler 封装成一个新的 channelHandler
     * @param handler
     * @param url
     * @return
     */
    public static ChannelHandler wrap(ChannelHandler handler, URL url) {
        return ChannelHandlers.getInstance().wrapInternal(handler, url);
    }

    protected static ChannelHandlers getInstance() {
        return INSTANCE;
    }

    static void setTestingChannelHandlers(ChannelHandlers instance) {
        INSTANCE = instance;
    }

    /**
     * 包装 channelhandler
     * @param handler
     * @param url
     * @return
     */
    protected ChannelHandler wrapInternal(ChannelHandler handler, URL url) {
        //装饰器模式  创建一个 能将任务自动转发到线程池的对象   并被 心跳handler 和 复合消息 handler 装饰 同时具备这些功能 这些功能都是在 转发线程池之前完成的
        //当 调用handler 的 各个事件触发函数时 从外层开始 依次往内调用
        return new MultiMessageHandler(new HeartbeatHandler(ExtensionLoader.getExtensionLoader(Dispatcher.class)
                //这里首先根据url 的dispatcher 确认创建什么样的 分发处理器 默认使用all、 就是全部事件都使用线程池 装饰后 还是返回一个handler对象
                .getAdaptiveExtension().dispatch(handler, url)));
    }
}
