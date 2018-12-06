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
package org.apache.dubbo.rpc.proxy.wrapper;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericService;

import java.lang.reflect.Constructor;

/**
 * StubProxyFactoryWrapper
 *
 * 存根工厂 包装类  在proxyFactory时 自动包装上去
 */
public class StubProxyFactoryWrapper implements ProxyFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StubProxyFactoryWrapper.class);

    /**
     * 代理工厂对象 在创建该对象时 已经通过 extension 进行属性注入了 这个对象 也是 自适应对象
     */
    private final ProxyFactory proxyFactory;

    /**
     * 协议对象 也是自适应
     */
    private Protocol protocol;

    public StubProxyFactoryWrapper(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 将 invoker 包装到 被代理过的对象中
     * @param invoker
     * @param generic
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        return proxyFactory.getProxy(invoker, generic);
    }

    /**
     * 在 代理工厂 本身 尝试 获取 代理对象时  判断是否是存根类
     * @param invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        //这里已经返回了  动态代理对象了
        T proxy = proxyFactory.getProxy(invoker);
        //没有实现泛化接口 一般都会进入
        if (GenericService.class != invoker.getInterface()) {
            //读取存根对象 全限定名  local 和 stub 是等价的 现在只使用 stub
            String stub = invoker.getUrl().getParameter(Constants.STUB_KEY, invoker.getUrl().getParameter(Constants.LOCAL_KEY   ));
            //存在 存根名时
            if (ConfigUtils.isNotEmpty(stub)) {
                //获取 invoker 对象的 接口  有存根的时候 会指定 存根类 对象 默认是 接口名后缀 增加 Stub or Local
                Class<?> serviceType = invoker.getInterface();
                //如果 stub:true or stub:default 也有可能上面返回的 时 local的 属性
                if (ConfigUtils.isDefault(stub)) {
                    if (invoker.getUrl().hasParameter(Constants.STUB_KEY)) {
                        //就变成了 intereface接口名Stub
                        stub = serviceType.getName() + "Stub";
                    } else {
                        stub = serviceType.getName() + "Local";
                    }
                }
                try {
                    //根据 全限定名 生成 存根类对象
                    Class<?> stubClass = ReflectUtils.forName(stub);
                    //如果该 存根类没有实现该接口 就是异常
                    if (!serviceType.isAssignableFrom(stubClass)) {
                        throw new IllegalStateException("The stub implementation class " + stubClass.getName() + " not implement interface " + serviceType.getName());
                    }
                    try {
                        //获取 存根类 的 构造函数 存根类 应该有一个 原始 ref 作为 参数 在调用方法时 加一些额外逻辑后 调用ref.XXX
                        Constructor<?> constructor = ReflectUtils.findConstructor(stubClass, serviceType);
                        //装饰成存根对象了 然后 proxy 还是动态代理对象
                        proxy = (T) constructor.newInstance(new Object[]{proxy});
                        //export stub service
                        URL url = invoker.getUrl();
                        //如果 存在存根事件???
                        if (url.getParameter(Constants.STUB_EVENT_KEY, Constants.DEFAULT_STUB_EVENT)) {
                            //设置 存根方法
                            url = url.addParameter(Constants.STUB_EVENT_METHODS_KEY, StringUtils.join(Wrapper.getWrapper(proxy.getClass()).getDeclaredMethodNames(), ","));
                            //修改为不是 服务类型
                            url = url.addParameter(Constants.IS_SERVER_KEY, Boolean.FALSE.toString());
                            try {
                                //暴露新的存根对象  这步感觉是不需要的 可能是为了测试能不能正常运行把
                                export(proxy, (Class) invoker.getInterface(), url);
                            } catch (Exception e) {
                                LOGGER.error("export a stub service error.", e);
                            }
                        }
                    } catch (NoSuchMethodException e) {
                        throw new IllegalStateException("No such constructor \"public " + stubClass.getSimpleName() + "(" + serviceType.getName() + ")\" in stub implementation class " + stubClass.getName(), e);
                    }
                } catch (Throwable t) {
                    LOGGER.error("Failed to create stub implementation class " + stub + " in consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", cause: " + t.getMessage(), t);
                    // ignore
                }
            }
        }
        return proxy;
    }

    @Override
    public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException {
        return proxyFactory.getInvoker(proxy, type, url);
    }

    /**
     * 把整个存根对象又作为动态代理对象 暴露
     * @param instance
     * @param type
     * @param url
     * @param <T>
     * @return
     */
    private <T> Exporter<T> export(T instance, Class<T> type, URL url) {
        return protocol.export(proxyFactory.getInvoker(instance, type, url));
    }

}
