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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericService;

import com.alibaba.dubbo.rpc.service.EchoService;

/**
 * AbstractProxyFactory
 *
 * 代理工厂的 骨架类
 */
public abstract class AbstractProxyFactory implements ProxyFactory {

    /**
     * 默认 非 泛化模式
     * @param invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    /**
     * 从 invoker 对象中 剥离出 ref对象
     * @param invoker
     * @param generic
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        //这里 是 抽取所有 代理类 方法的地方 通过这些方法 构建$Proxy 对象

        //生成 接口数组
        Class<?>[] interfaces = null;
        //获取 需要生成的 接口类型  在监控中心 会被设置成 MonitorService 为该对象增加2个实现的接口
        String config = invoker.getUrl().getParameter(Constants.INTERFACES);
        if (config != null && config.length() > 0) {
            //多个接口 用 "," 拼接
            String[] types = Constants.COMMA_SPLIT_PATTERN.split(config);
            if (types != null && types.length > 0) {
                interfaces = new Class<?>[types.length + 2];
                //多设置了 2个 接口 一个是 invoker本身接口 一个 是 echoService 接口
                //实现 回声接口是为了方便测试是否可用
                interfaces[0] = invoker.getInterface();
                interfaces[1] = EchoService.class;
                for (int i = 0; i < types.length; i++) {
                    // TODO can we load successfully for a different classloader?.
                    interfaces[i + 2] = ReflectUtils.forName(types[i]);
                }
            }
        }
        if (interfaces == null) {
            //默认 也会设置这 2个 接口
            interfaces = new Class<?>[]{invoker.getInterface(), EchoService.class};
        }

        //如果 不是 泛化接口 且  泛化标识为true
        if (!GenericService.class.isAssignableFrom(invoker.getInterface()) && generic) {
            int len = interfaces.length;
            Class<?>[] temp = interfaces;
            interfaces = new Class<?>[len + 1];
            System.arraycopy(temp, 0, interfaces, 0, len);
            //增加一个泛化接口  也就是将 普通实现类 变成 了  泛化类
            interfaces[len] = com.alibaba.dubbo.rpc.service.GenericService.class;
        }

        return getProxy(invoker, interfaces);
    }

    /**
     * 真正的 代理逻辑 由子类实现
     * @param invoker
     * @param types
     * @param <T>
     * @return
     */
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}
