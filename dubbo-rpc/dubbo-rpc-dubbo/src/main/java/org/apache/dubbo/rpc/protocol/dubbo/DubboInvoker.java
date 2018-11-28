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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ResponseFuture;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.SimpleAsyncRpcResult;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DubboInvoker
 * dubbo 协议层的 invoker 对象
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    /**
     * 客户端对象 应该是 使用这个invoker 对象的所有 client
     */
    private final ExchangeClient[] clients;

    /**
     * 一个原子的计数器类
     */
    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    /**
     * 版本
     */
    private final String version;

    private final ReentrantLock destroyLock = new ReentrantLock();

    /**
     * invoker 容器
     */
    private final Set<Invoker<?>> invokers;

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients) {
        this(serviceType, url, clients, null);
    }

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients, Set<Invoker<?>> invokers) {
        //这些属性会设置到 父类的 attachment上
        super(serviceType, url, new String[]{Constants.INTERFACE_KEY, Constants.GROUP_KEY, Constants.TOKEN_KEY, Constants.TIMEOUT_KEY});
        this.clients = clients;
        // get version.
        this.version = url.getParameter(Constants.VERSION_KEY, "0.0.0");
        this.invokers = invokers;
    }

    /**
     * 实现父类的抽象调用方法
     * @param invocation  初始化完成并设置了一堆参数后
     * @return
     * @throws Throwable
     */
    @Override
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        //从该对象上获取方法名
        final String methodName = RpcUtils.getMethodName(invocation);
        //增加 路径和 版本信息
        inv.setAttachment(Constants.PATH_KEY, getUrl().getPath());
        inv.setAttachment(Constants.VERSION_KEY, version);

        ExchangeClient currentClient;
        //这个应该是 共享client
        if (clients.length == 1) {
            currentClient = clients[0];
        } else {
            //随机获取一个 client
            currentClient = clients[index.getAndIncrement() % clients.length];
        }
        try {
            //通过url 上的 属性标识判断当前是 同步还是异步
            boolean isAsync = RpcUtils.isAsync(getUrl(), invocation);
            //是泛型类 或者是 future 类 就 需要设置 异步future
            boolean isAsyncFuture = RpcUtils.isGeneratedFuture(inv) || RpcUtils.isFutureReturnType(inv);
            //是否 不需要返回 响应
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);
            int timeout = getUrl().getMethodParameter(methodName, Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
            if (isOneway) {
                //methodName + key 去 查找属性
                boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                //通过client 发送请求
                currentClient.send(inv, isSent);
                //学习了 netty 的 threadLocal  将 future 设置为null 因为 oneway 不需要返回结果就不需要future
                RpcContext.getContext().setFuture(null);
                //直接返回请求结果 没有结果和异常对象
                return new RpcResult();
            } else if (isAsync) {
                //异步 的 请求方式不同 返回一个future对象
                ResponseFuture future = currentClient.request(inv, timeout);
                // For compatibility
                // future 适配器对象
                FutureAdapter<Object> futureAdapter = new FutureAdapter<>(future);
                //设置结果对象  这里设置的 是适配器对象
                RpcContext.getContext().setFuture(futureAdapter);

                Result result;
                //如果是异步 结果
                if (isAsyncFuture) {
                    // register resultCallback, sometimes we need the asyn result being processed by the filter chain.
                    //创建一个 异步RPC 结果对象
                    result = new AsyncRpcResult(futureAdapter, futureAdapter.getResultFuture(), false);
                } else {
                    //创建一个简单对象 继承于上面那个类相比少了 recreate 方法
                    result = new SimpleAsyncRpcResult(futureAdapter, futureAdapter.getResultFuture(), false);
                }
                return result;
            } else {
                //设置 future为null  这里应该是 同步请求方式
                RpcContext.getContext().setFuture(null);
                //返回结果对象
                return (Result) currentClient.request(inv, timeout).get();
            }
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 是否可用
     * @return
     */
    @Override
    public boolean isAvailable() {
        //父类不可用
        if (!super.isAvailable()) {
            return false;
        }
        //只要有一个client 在连接中 且不是只读的就是true 当收到 服务端发起的 只读请求后 客户端就会修改状态为 只读
        for (ExchangeClient client : clients) {
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)) {
                //cannot write == not Available ?
                return true;
            }
        }
        return false;
    }

    /**
     * 销毁方法
     */
    @Override
    public void destroy() {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (super.isDestroyed()) {
            return;
        } else {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                //从 invoker 容器中移除该对象
                if (invokers != null) {
                    invokers.remove(this);
                }
                for (ExchangeClient client : clients) {
                    try {
                        //将下面的全部client 关闭
                        client.close(ConfigUtils.getServerShutdownTimeout());
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }

            } finally {
                destroyLock.unlock();
            }
        }
    }
}
