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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.registry.integration.RegistryDirectory;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @date 2017/11/23
 *
 * 服务提供者 和 消费者 的管理对象
 */
public class ProviderConsumerRegTable {
    /**
     * 一个 服务键 下的所有出口者对象  key 服务键  value 对应的注册中心 invoker 提供者url 的包装类
     * 因为 同一个key 可以发布到多个注册中心 所以value 是set 类型
     */
    public static ConcurrentHashMap<String, Set<ProviderInvokerWrapper>> providerInvokers = new ConcurrentHashMap<String, Set<ProviderInvokerWrapper>>();
    /**
     * 一个 服务键 下的所有订阅者
     */
    public static ConcurrentHashMap<String, Set<ConsumerInvokerWrapper>> consumerInvokers = new ConcurrentHashMap<String, Set<ConsumerInvokerWrapper>>();

    /**
     * 注册服务提供者
     * @param invoker 服务提供者 提供的 可执行对象
     * @param registryUrl 注册中心地址
     * @param providerUrl 服务提供者地址
     */
    public static void registerProvider(Invoker invoker, URL registryUrl, URL providerUrl) {
        //生成服务提供者包装类
        ProviderInvokerWrapper wrapperInvoker = new ProviderInvokerWrapper(invoker, registryUrl, providerUrl);
        //生成服务键
        String serviceUniqueName = providerUrl.getServiceKey();
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            providerInvokers.putIfAbsent(serviceUniqueName, new ConcurrentHashSet<ProviderInvokerWrapper>());
            invokers = providerInvokers.get(serviceUniqueName);
        }
        invokers.add(wrapperInvoker);
    }

    public static Set<ProviderInvokerWrapper> getProviderInvoker(String serviceUniqueName) {
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return Collections.emptySet();
        }
        return invokers;
    }

    public static ProviderInvokerWrapper getProviderWrapper(Invoker invoker) {
        URL providerUrl = invoker.getUrl();
        if (Constants.REGISTRY_PROTOCOL.equals(providerUrl.getProtocol())) {
            providerUrl = URL.valueOf(providerUrl.getParameterAndDecoded(Constants.EXPORT_KEY));
        }
        String serviceUniqueName = providerUrl.getServiceKey();
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return null;
        }

        for (ProviderInvokerWrapper providerWrapper : invokers) {
            Invoker providerInvoker = providerWrapper.getInvoker();
            if (providerInvoker == invoker) {
                return providerWrapper;
            }
        }

        return null;
    }

    /**
     * 在全局变量中 保存消费者 和 服务提供者(invoker)
     * @param invoker
     * @param registryUrl
     * @param consumerUrl
     * @param registryDirectory
     */
    public static void registerConsumer(Invoker invoker, URL registryUrl, URL consumerUrl, RegistryDirectory registryDirectory) {
        ConsumerInvokerWrapper wrapperInvoker = new ConsumerInvokerWrapper(invoker, registryUrl, consumerUrl, registryDirectory);
        String serviceUniqueName = consumerUrl.getServiceKey();
        Set<ConsumerInvokerWrapper> invokers = consumerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            consumerInvokers.putIfAbsent(serviceUniqueName, new ConcurrentHashSet<ConsumerInvokerWrapper>());
            invokers = consumerInvokers.get(serviceUniqueName);
        }
        invokers.add(wrapperInvoker);
    }

    public static Set<ConsumerInvokerWrapper> getConsumerInvoker(String serviceUniqueName) {
        Set<ConsumerInvokerWrapper> invokers = consumerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return Collections.emptySet();
        }
        return invokers;
    }

}
