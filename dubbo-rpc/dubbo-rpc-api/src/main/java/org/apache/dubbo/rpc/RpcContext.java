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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadlocal.InternalThreadLocal;
import org.apache.dubbo.common.utils.NetUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Thread local context. (API, ThreadLocal, ThreadSafe)
 * <p>
 * Note: RpcContext is a temporary state holder. States in RpcContext changes every time when request is sent or received.
 * For example: A invokes B, then B invokes C. On service B, RpcContext saves invocation info from A to B before B
 * starts invoking C, and saves invocation info from B to C after B invokes C.
 *
 * RPC 上下文对象 是一个临时状态储存器 每当收到和发送请求时 状态都会发生改变 根据调用链的传递 调用信息也会被保存下去
 * 每次发起请求 rpc 上下文都会发生变化
 * @export
 * @see org.apache.dubbo.rpc.filter.ContextFilter
 */
public class RpcContext {

    /**
     * use internal thread local to improve performance
     * 创建一个本地线程变量 在里面设置一个 RPCContext对象
     */
    private static final InternalThreadLocal<RpcContext> LOCAL = new InternalThreadLocal<RpcContext>() {
        @Override
        protected RpcContext initialValue() {
            return new RpcContext();
        }
    };
    /**
     * 这个和上面的有什么区别
     */
    private static final InternalThreadLocal<RpcContext> SERVER_LOCAL = new InternalThreadLocal<RpcContext>() {
        @Override
        protected RpcContext initialValue() {
            return new RpcContext();
        }
    };

    /**
     * attachment 容器
     */
    private final Map<String, String> attachments = new HashMap<String, String>();
    private final Map<String, Object> values = new HashMap<String, Object>();

    /**
     * future对象
     */
    private Future<?> future;

    /**
     * 该线程 上下文中发起的所有 服务提供者的url
     */
    private List<URL> urls;

    /**
     * 消费者url
     */
    private URL url;

    private String methodName;

    private Class<?>[] parameterTypes;

    private Object[] arguments;

    private InetSocketAddress localAddress;

    private InetSocketAddress remoteAddress;
    /**
     * 使用List<URL>代替
     */
    @Deprecated
    private List<Invoker<?>> invokers;
    /**
     * 使用URL代替
     */
    @Deprecated
    private Invoker<?> invoker;
    /**
     * 使用methodName 等参数代替
     */
    @Deprecated
    private Invocation invocation;

    // now we don't use the 'values' map to hold these objects
    // we want these objects to be as generic as possible
    //确保可以在想获取的时机 随时获取 请求对象和响应对象
    private Object request;
    private Object response;
    /**
     * 异步上下文对象
     */
    private AsyncContext asyncContext;

    protected RpcContext() {
    }

    /**
     * get server side context.
     *
     * 获取服务端上下文对象
     * @return server context
     */
    public static RpcContext getServerContext() {
        return SERVER_LOCAL.get();
    }

    /**
     * 重置服务端上下文对象的内容
     * @param oldServerContext
     */
    public static void restoreServerContext(RpcContext oldServerContext) {
        SERVER_LOCAL.set(oldServerContext);
    }

    /**
     * remove server side context.
     *
     * @see org.apache.dubbo.rpc.filter.ContextFilter
     */
    public static void removeServerContext() {
        SERVER_LOCAL.remove();
    }

    /**
     * get context.
     *
     * @return context
     */
    public static RpcContext getContext() {
        return LOCAL.get();
    }

    /**
     * 重置本地上下文对象
     * @param oldContext
     */
    public static void restoreContext(RpcContext oldContext) {
        LOCAL.set(oldContext);
    }


    /**
     * 返回一个 当前上下文的 副本对象
     * @return
     */
    public RpcContext copyOf() {
        RpcContext copy = new RpcContext();
        copy.attachments.putAll(this.attachments);
        copy.values.putAll(this.values);
        copy.future = this.future;
        copy.urls = this.urls;
        copy.url = this.url;
        copy.methodName = this.methodName;
        copy.parameterTypes = this.parameterTypes;
        copy.arguments = this.arguments;
        copy.localAddress = this.localAddress;
        copy.remoteAddress = this.remoteAddress;
        copy.invokers = this.invokers;
        copy.invoker = this.invoker;
        copy.invocation = this.invocation;

        copy.request = this.request;
        copy.response = this.response;
        copy.asyncContext = this.asyncContext;

        return copy;
    }


    /**
     * remove context.
     *
     * 移除上下文对象
     * @see org.apache.dubbo.rpc.filter.ContextFilter
     */
    public static void removeContext() {
        LOCAL.remove();
    }

    /**
     * Get the request object of the underlying RPC protocol, e.g. HttpServletRequest
     *
     * @return null if the underlying protocol doesn't provide support for getting request
     */
    public Object getRequest() {
        return request;
    }

    public void setRequest(Object request) {
        this.request = request;
    }

    /**
     * Get the request object of the underlying RPC protocol, e.g. HttpServletRequest
     *
     * @return null if the underlying protocol doesn't provide support for getting request or the request is not of the specified type
     */
    @SuppressWarnings("unchecked")
    public <T> T getRequest(Class<T> clazz) {
        return (request != null && clazz.isAssignableFrom(request.getClass())) ? (T) request : null;
    }

    /**
     * Get the response object of the underlying RPC protocol, e.g. HttpServletResponse
     *
     * @return null if the underlying protocol doesn't provide support for getting response
     */
    public Object getResponse() {
        return response;
    }

    public void setResponse(Object response) {
        this.response = response;
    }

    /**
     * Get the response object of the underlying RPC protocol, e.g. HttpServletResponse
     *
     * @return null if the underlying protocol doesn't provide support for getting response or the response is not of the specified type
     */
    @SuppressWarnings("unchecked")
    public <T> T getResponse(Class<T> clazz) {
        return (response != null && clazz.isAssignableFrom(response.getClass())) ? (T) response : null;
    }

    /**
     * is provider side.
     *
     * 判断是提供者端还是消费者端
     * @return provider side.
     */
    public boolean isProviderSide() {
        return !isConsumerSide();
    }

    /**
     * is consumer side.
     *
     * 通过当前url 的 side属性 查看是哪段
     * @return consumer side.
     */
    public boolean isConsumerSide() {
        return getUrl().getParameter(Constants.SIDE_KEY, Constants.PROVIDER_SIDE).equals(Constants.CONSUMER_SIDE);
    }

    /**
     * get CompletableFuture.
     *
     * @param <T>
     * @return future
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> getCompletableFuture() {
        return (CompletableFuture<T>) future;
    }

    /**
     * get future.
     *
     * @param <T>
     * @return future
     */
    @SuppressWarnings("unchecked")
    public <T> Future<T> getFuture() {
        return (Future<T>) future;
    }

    /**
     * set future.
     *
     * @param future
     */
    public void setFuture(Future<?> future) {
        this.future = future;
    }

    public List<URL> getUrls() {
        return urls == null && url != null ? (List<URL>) Arrays.asList(url) : urls;
    }

    public void setUrls(List<URL> urls) {
        this.urls = urls;
    }

    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    /**
     * get method name.
     *
     * @return method name.
     */
    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * get parameter types.
     *
     * @serial
     */
    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    /**
     * get arguments.
     *
     * @return arguments.
     */
    public Object[] getArguments() {
        return arguments;
    }

    public void setArguments(Object[] arguments) {
        this.arguments = arguments;
    }

    /**
     * set local address.
     *
     * 通过 host和port 创建地址并设置到 本地地址中
     * @param host
     * @param port
     * @return context
     */
    public RpcContext setLocalAddress(String host, int port) {
        if (port < 0) {
            port = 0;
        }
        this.localAddress = InetSocketAddress.createUnresolved(host, port);
        return this;
    }

    /**
     * get local address.
     *
     * @return local address
     */
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    /**
     * set local address.
     *
     * @param address
     * @return context
     */
    public RpcContext setLocalAddress(InetSocketAddress address) {
        this.localAddress = address;
        return this;
    }

    public String getLocalAddressString() {
        return getLocalHost() + ":" + getLocalPort();
    }

    /**
     * get local host name.
     *
     * @return local host name
     */
    public String getLocalHostName() {
        String host = localAddress == null ? null : localAddress.getHostName();
        if (host == null || host.length() == 0) {
            //这里会设置本地地址
            return getLocalHost();
        }
        return host;
    }

    /**
     * set remote address.
     *
     * @param host
     * @param port
     * @return context
     */
    public RpcContext setRemoteAddress(String host, int port) {
        if (port < 0) {
            port = 0;
        }
        this.remoteAddress = InetSocketAddress.createUnresolved(host, port);
        return this;
    }

    /**
     * get remote address.
     *
     * @return remote address
     */
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * set remote address.
     *
     * @param address
     * @return context
     */
    public RpcContext setRemoteAddress(InetSocketAddress address) {
        this.remoteAddress = address;
        return this;
    }

    /**
     * get remote address string.
     *
     * @return remote address string.
     */
    public String getRemoteAddressString() {
        return getRemoteHost() + ":" + getRemotePort();
    }

    /**
     * get remote host name.
     *
     * @return remote host name
     */
    public String getRemoteHostName() {
        return remoteAddress == null ? null : remoteAddress.getHostName();
    }

    /**
     * get local host.
     *
     * 获取本地地址 没有就设置
     * @return local host
     */
    public String getLocalHost() {
        String host = localAddress == null ? null :
                localAddress.getAddress() == null ? localAddress.getHostName()
                        : NetUtils.filterLocalHost(localAddress.getAddress().getHostAddress());
        if (host == null || host.length() == 0) {
            //获取本地地址
            return NetUtils.getLocalHost();
        }
        return host;
    }

    /**
     * get local port.
     *
     * @return port
     */
    public int getLocalPort() {
        return localAddress == null ? 0 : localAddress.getPort();
    }

    /**
     * get remote host.
     *
     * @return remote host
     */
    public String getRemoteHost() {
        return remoteAddress == null ? null :
                remoteAddress.getAddress() == null ? remoteAddress.getHostName()
                        : NetUtils.filterLocalHost(remoteAddress.getAddress().getHostAddress());
    }

    /**
     * get remote port.
     *
     * @return remote port
     */
    public int getRemotePort() {
        return remoteAddress == null ? 0 : remoteAddress.getPort();
    }

    /**
     * get attachment.
     *
     * @param key
     * @return attachment
     */
    public String getAttachment(String key) {
        return attachments.get(key);
    }

    /**
     * set attachment.
     *
     * @param key
     * @param value
     * @return context
     */
    public RpcContext setAttachment(String key, String value) {
        if (value == null) {
            attachments.remove(key);
        } else {
            attachments.put(key, value);
        }
        return this;
    }

    /**
     * remove attachment.
     *
     * @param key
     * @return context
     */
    public RpcContext removeAttachment(String key) {
        attachments.remove(key);
        return this;
    }

    /**
     * get attachments.
     *
     * @return attachments
     */
    public Map<String, String> getAttachments() {
        return attachments;
    }

    /**
     * set attachments
     *
     * 先清除原先的attachment 再设置新的attachment
     * @param attachment
     * @return context
     */
    public RpcContext setAttachments(Map<String, String> attachment) {
        this.attachments.clear();
        if (attachment != null && attachment.size() > 0) {
            this.attachments.putAll(attachment);
        }
        return this;
    }

    public void clearAttachments() {
        this.attachments.clear();
    }

    /**
     * get values.
     *
     * @return values
     */
    public Map<String, Object> get() {
        return values;
    }

    /**
     * set value.
     *
     * @param key
     * @param value
     * @return context
     */
    public RpcContext set(String key, Object value) {
        if (value == null) {
            values.remove(key);
        } else {
            values.put(key, value);
        }
        return this;
    }

    /**
     * remove value.
     *
     * @param key
     * @return value
     */
    public RpcContext remove(String key) {
        values.remove(key);
        return this;
    }

    /**
     * get value.
     *
     * @param key
     * @return value
     */
    public Object get(String key) {
        return values.get(key);
    }

    /**
     * @deprecated Replace to isProviderSide()
     */
    @Deprecated
    public boolean isServerSide() {
        return isProviderSide();
    }

    /**
     * @deprecated Replace to isConsumerSide()
     */
    @Deprecated
    public boolean isClientSide() {
        return isConsumerSide();
    }

    /**
     * @deprecated Replace to getUrls()
     */
    @Deprecated
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Invoker<?>> getInvokers() {
        return invokers == null && invoker != null ? (List) Arrays.asList(invoker) : invokers;
    }

    public RpcContext setInvokers(List<Invoker<?>> invokers) {
        this.invokers = invokers;
        if (invokers != null && !invokers.isEmpty()) {
            List<URL> urls = new ArrayList<URL>(invokers.size());
            for (Invoker<?> invoker : invokers) {
                urls.add(invoker.getUrl());
            }
            setUrls(urls);
        }
        return this;
    }

    /**
     * @deprecated Replace to getUrl()
     */
    @Deprecated
    public Invoker<?> getInvoker() {
        return invoker;
    }

    public RpcContext setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
        if (invoker != null) {
            setUrl(invoker.getUrl());
        }
        return this;
    }

    /**
     * @deprecated Replace to getMethodName(), getParameterTypes(), getArguments()
     */
    @Deprecated
    public Invocation getInvocation() {
        return invocation;
    }

    public RpcContext setInvocation(Invocation invocation) {
        this.invocation = invocation;
        if (invocation != null) {
            setMethodName(invocation.getMethodName());
            setParameterTypes(invocation.getParameterTypes());
            setArguments(invocation.getArguments());
        }
        return this;
    }

    /**
     * Async invocation. Timeout will be handled even if <code>Future.get()</code> is not called.
     *
     * @param callable
     * @return get the return result from <code>future.get()</code>
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> asyncCall(Callable<T> callable) {
        try {
            try {
                //设置成异步状态
                setAttachment(Constants.ASYNC_KEY, Boolean.TRUE.toString());
                //执行 任务 并获取结果 一般返回的是一个future对象
                final T o = callable.call();
                //local invoke will return directly
                if (o != null) {
                    if (o instanceof CompletableFuture) {
                        //将future 对象返回
                        return (CompletableFuture<T>) o;
                    }
                    //将对象封装成 CompletableFuture
                    CompletableFuture.completedFuture(o);
                } else {
                    // The service has a normal sync method signature, should get future from RpcContext.
                }
            } catch (Exception e) {
                throw new RpcException(e);
            } finally {
                //结束后移除异步标识
                removeAttachment(Constants.ASYNC_KEY);
            }
        } catch (final RpcException e) {
            //捕获到异常时  返回一个 抛出异常的 future对象
            return new CompletableFuture<T>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public T get() throws InterruptedException, ExecutionException {
                    throw new ExecutionException(e.getCause());
                }

                @Override
                public T get(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException,
                        TimeoutException {
                    return get();
                }
            };
        }
        //没有结果的情况下从上下文对象中获取future
        return ((CompletableFuture<T>) getContext().getFuture());
    }

    /**
     * one way async call, send request only, and result is not required
     *
     * 这种参数 代表 是 oneway 不需要返回值
     * @param runnable
     */
    public void asyncCall(Runnable runnable) {
        try {
            //设置成不需要 返回值
            setAttachment(Constants.RETURN_KEY, Boolean.FALSE.toString());
            runnable.run();
        } catch (Throwable e) {
            // FIXME should put exception in future?
            throw new RpcException("oneway call error ." + e.getMessage(), e);
        } finally {
            removeAttachment(Constants.RETURN_KEY);
        }
    }

    /**
     * @return
     * @throws IllegalStateException
     */
    @SuppressWarnings("unchecked")
    public static AsyncContext startAsync() throws IllegalStateException {
        RpcContext currentContext = getContext();
        if (currentContext.asyncContext != null) {
            //启动上下文对象 并返回
            currentContext.asyncContext.start();
            return currentContext.asyncContext;
        } else {
            throw new IllegalStateException("This service does not support asynchronous operations, you should open async explicitly before use.");
        }
    }

    /**
     * 判断异步上下文对象是否启动
     * @return
     */
    public boolean isAsyncStarted() {
        if (this.asyncContext == null) {
            return false;
        }
        return asyncContext.isAsyncStarted();
    }

    public boolean stopAsync() {
        return asyncContext.stop();
    }

    /**
     * 保存异步上下文对象 便于在某个时机取出这个异步对象
     * @param asyncContext
     */
    public void setAsyncContext(AsyncContext asyncContext) {
        this.asyncContext = asyncContext;
    }

    public AsyncContext getAsyncContext() {
        return asyncContext;
    }

}
