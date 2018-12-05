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
package org.apache.dubbo.rpc.protocol.injvm;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;

import java.util.Map;

/**
 * InjvmInvoker
 * 本地 服务提供者 提供的 调用者对象 通过  AbstractInvoker 定义 invoke 的基本架构 然后重写子类核心的 doInvoke方法
 */
class InjvmInvoker<T> extends AbstractInvoker<T> {

    /**
     * 本 invoker 的 服务键
     */
    private final String key;

    /**
     * 本地 所有的 服务出口对象
     */
    private final Map<String, Exporter<?>> exporterMap;

    InjvmInvoker(Class<T> type, URL url, String key, Map<String, Exporter<?>> exporterMap) {
        super(type, url);
        this.key = key;
        this.exporterMap = exporterMap;
    }

    @Override
    public boolean isAvailable() {
        InjvmExporter<?> exporter = (InjvmExporter<?>) exporterMap.get(key);
        if (exporter == null) {
            return false;
        } else {
            return super.isAvailable();
        }
    }

    /**
     * invoker 的实际逻辑
     * @param invocation
     * @return
     * @throws Throwable
     */
    @Override
    public Result doInvoke(Invocation invocation) throws Throwable {
        //通过传入的 url 直接在容器中 寻找export 对象 export 是在服务提供者本地暴露时 添加到容器的
        Exporter<?> exporter = InjvmProtocol.getExporter(exporterMap, getUrl());
        if (exporter == null) {
            throw new RpcException("Service [" + key + "] not found.");
        }
        //远程地址 设定为 本地
        RpcContext.getContext().setRemoteAddress(NetUtils.LOCALHOST, 0);
        //通过 出口对象 包含的 invoker 属性执行实际 的 invoker方法
        return exporter.getInvoker().invoke(invocation);
    }
}
