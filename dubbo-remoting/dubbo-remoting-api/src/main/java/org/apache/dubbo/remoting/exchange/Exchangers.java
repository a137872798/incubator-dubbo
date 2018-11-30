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
package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerDispatcher;
import org.apache.dubbo.remoting.exchange.support.Replier;
import org.apache.dubbo.remoting.transport.ChannelHandlerAdapter;

/**
 * Exchanger facade. (API, Static, ThreadSafe)
 *
 * exchange的门面类
 */
public class Exchangers {

    static {
        // check duplicate jar package
        // 检查有没有版本冲突
        Version.checkDuplicate(Exchangers.class);
    }

    private Exchangers() {
    }

    /**
     * 绑定 指定url 并返回 server对象
     * @param url
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(String url, Replier<?> replier) throws RemotingException {
        return bind(URL.valueOf(url), replier);
    }

    /**
     * 创建一个 适配器对象 继续委托调用bind 方法 适配器对象 实现 ChannelHandler 接口
     * @param url
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, Replier<?> replier) throws RemotingException {
        return bind(url, new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeServer bind(String url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return bind(URL.valueOf(url), handler, replier);
    }

    /**
     * 创建 exchangehandler 对象封装 replier 和 handler
     * @param url
     * @param handler
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return bind(url, new ExchangeHandlerDispatcher(replier, handler));
    }

    public static ExchangeServer bind(String url, ExchangeHandler handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 绑定的实际 调用逻辑
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        //给url 设置 编解码器  一般 已经被设置成 dubbo 了
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        //通过url 获取 exchanger 对象 进行绑定
        return getExchanger(url).bind(url, handler);
    }

    public static ExchangeClient connect(String url) throws RemotingException {
        return connect(URL.valueOf(url));
    }

    public static ExchangeClient connect(URL url) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), null);
    }

    public static ExchangeClient connect(String url, Replier<?> replier) throws RemotingException {
        return connect(URL.valueOf(url), new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeClient connect(URL url, Replier<?> replier) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), replier);
    }

    public static ExchangeClient connect(String url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return connect(URL.valueOf(url), handler, replier);
    }

    public static ExchangeClient connect(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return connect(url, new ExchangeHandlerDispatcher(replier, handler));
    }

    public static ExchangeClient connect(String url, ExchangeHandler handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    /**
     * exchanger 实现connect
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        //给 url 增加一个新的 键值对
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        //通过url 的 exchange 属性 获取特定的 拓展对象 调用connect 方法
        return getExchanger(url).connect(url, handler);
    }

    /**
     * 通过 url 获取 exchanger 的拓展对象
     * @param url
     * @return
     */
    public static Exchanger getExchanger(URL url) {
        //从参数中解析 exchanger 的实现类型
        String type = url.getParameter(Constants.EXCHANGER_KEY, Constants.DEFAULT_EXCHANGER);
        return getExchanger(type);
    }

    /**
     * 获取拓展对象
     * @param type
     * @return
     */
    public static Exchanger getExchanger(String type) {
        return ExtensionLoader.getExtensionLoader(Exchanger.class).getExtension(type);
    }

}