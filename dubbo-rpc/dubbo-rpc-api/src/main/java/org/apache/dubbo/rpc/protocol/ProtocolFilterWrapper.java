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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

import java.util.List;

/**
 * ListenerProtocol
 * 给Invoker 增加过滤链
 * 当通过SPI 初始化 协议对象时 同时 初始化了这些包装类  在SPI 导入 Protocol实现类时 将该类作为 cachedWrapperClasses 并 包装到生成的Protocol上
 */
public class ProtocolFilterWrapper implements Protocol {

    //协议对象
    private final Protocol protocol;

    public ProtocolFilterWrapper(Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol == null");
        }
        this.protocol = protocol;
    }

    /**
     * 构建一个 调用链对象
     * @param invoker invoker 原型对象
     * @param key serivce.filter
     * @param group provider/ consumer
     * @param <T>
     * @return
     */
    private static <T> Invoker<T> buildInvokerChain(final Invoker<T> invoker, String key, String group) {
        //传入的 invoker 对象作为 链尾
        Invoker<T> last = invoker;
        //通过 SPI 创建 Filter 对象  这里的 key: serivce.filter 去 url 中获取对应的值 返回一组 被拓展的 Filter对象
        List<Filter> filters = ExtensionLoader.getExtensionLoader(Filter.class).getActivateExtension(invoker.getUrl(), key, group);
        if (!filters.isEmpty()) {
            for (int i = filters.size() - 1; i >= 0; i--) {
                final Filter filter = filters.get(i);
                //从尾往前创建 每此调用下一个 元素 也就是 正序
                //通过 这个引用来 保存 临时 对象的 地址 链中每次下一个对象调的都是上一个临时保存的对象
                final Invoker<T> next = last;

                //新创建的对象 而invoke 还是 委托给 未被本次 包装的对象
                last = new Invoker<T>() {

                    @Override
                    public Class<T> getInterface() {
                        return invoker.getInterface();
                    }

                    @Override
                    public URL getUrl() {
                        return invoker.getUrl();
                    }

                    @Override
                    public boolean isAvailable() {
                        return invoker.isAvailable();
                    }

                    @Override
                    public Result invoke(Invocation invocation) throws RpcException {
                        //next 是 上次生成包装对象
                        return filter.invoke(next, invocation);
                    }

                    @Override
                    public void destroy() {
                        invoker.destroy();
                    }

                    @Override
                    public String toString() {
                        return invoker.toString();
                    }
                };
            }
        }
        //这个 last 相当于是一个临时对象 通过调用连依次向下调用
        return last;
    }

    @Override
    public int getDefaultPort() {
        return protocol.getDefaultPort();
    }

    /**
     * 获得服务提供者
     * @param invoker Service invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {

        //如果 协议是 registry 代表是 出口到 注册中心 就不走调用链 完成订阅or 发布后 再调用 还是会进入到下面
        if (Constants.REGISTRY_PROTOCOL.equals(invoker.getUrl().getProtocol())) {
            return protocol.export(invoker);
        }
        return protocol.export(buildInvokerChain(invoker, Constants.SERVICE_FILTER_KEY, Constants.PROVIDER));
    }

    /**
     * 引用 服务端提供的对象
     * @param type Service class
     * @param url  URL address for the remote service
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        //远程情况 也就是去注册中心 获取 当到注册中心 完成订阅or 发布后 再调用 还是会进入到下面
        if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
            return protocol.refer(type, url);
        }
        return buildInvokerChain(protocol.refer(type, url), Constants.REFERENCE_FILTER_KEY, Constants.CONSUMER);
    }

    @Override
    public void destroy() {
        protocol.destroy();
    }

}
