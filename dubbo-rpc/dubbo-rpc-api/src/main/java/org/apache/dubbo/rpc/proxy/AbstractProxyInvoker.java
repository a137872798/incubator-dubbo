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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

/**
 * InvokerWrapper
 *
 * invoker 的包装类
 */
public abstract class AbstractProxyInvoker<T> implements Invoker<T> {
    Logger logger = LoggerFactory.getLogger(AbstractProxyInvoker.class);

    /**
     * 内部包含了 代理对象
     */
    private final T proxy;

    /**
     * 服务端接口类型
     */
    private final Class<T> type;

    /**
     * 服务提供者 的 url
     */
    private final URL url;

    public AbstractProxyInvoker(T proxy, Class<T> type, URL url) {
        if (proxy == null) {
            throw new IllegalArgumentException("proxy == null");
        }
        if (type == null) {
            throw new IllegalArgumentException("interface == null");
        }
        //代理类 如果没有实现该接口 抛出异常
        if (!type.isInstance(proxy)) {
            throw new IllegalArgumentException(proxy.getClass().getName() + " not implement interface " + type);
        }
        this.proxy = proxy;
        this.type = type;
        this.url = url;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
    }

    // TODO Unified to AsyncResult?

    /**
     * invoker 的 核心invoke 方法 doInvoke 由子类实现
     * 为什么要这样做 对外部统一 在外部看来始终是使用 invoker.invoke(Invocation) 进行远处调用 而不是 在代码中 每次给 ref对象传不同的参数
     * @param invocation 就是 request中解码获得的 包含了请求的方法名参数等
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        //从本地线程中获取上下文对象
        RpcContext rpcContext = RpcContext.getContext();
        try {
            //由子类实现  在這個方法裏 已经考虑了异步情况了 生成的 是一个CompletableFuture 对象
            Object obj = doInvoke(proxy, invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments());
            //如果返回值实现了  Compatable 接口 创建异步对象
            if (RpcUtils.isFutureReturnType(invocation)) {
                return new AsyncRpcResult((CompletableFuture<Object>) obj);
                //如果异步上下文 处于启动状态??? 这里看不懂
            } else if (rpcContext.isAsyncStarted()) { // ignore obj in case of RpcContext.startAsync()? always rely on user to write back.
                return new AsyncRpcResult(rpcContext.getAsyncContext().getInternalFuture());
            } else {
                //将动态代理的结果返回
                return new RpcResult(obj);
            }
            //这个 异常是 method.invoke 找不到 合适的异常时 抛出
        } catch (InvocationTargetException e) {
            // TODO async throw exception before async thread write back, should stop asyncContext
            //出现异常时 异步状态无法关闭 就打印日志
            if (rpcContext.isAsyncStarted() && !rpcContext.stopAsync()) {
                logger.error("Provider async started, but got an exception from the original method, cannot write the exception back to consumer because an async result may have returned the new thread.", e);
            }
            return new RpcResult(e.getTargetException());
        } catch (Throwable e) {
            throw new RpcException("Failed to invoke remote proxy method " + invocation.getMethodName() + " to " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    protected abstract Object doInvoke(T proxy, String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Throwable;

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? " " : getUrl().toString());
    }


}
