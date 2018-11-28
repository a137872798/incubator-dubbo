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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.support.SerializableClassRegistry;
import org.apache.dubbo.common.serialize.support.SerializationOptimizer;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporter;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.apache.dubbo.rpc.AsyncContextImpl;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * dubbo protocol support.
 * dubbo 协议的 实现对象
 */
public class DubboProtocol extends AbstractProtocol {

    public static final String NAME = "dubbo";

    public static final int DEFAULT_PORT = 20880;
    private static final String IS_CALLBACK_SERVICE_INVOKE = "_isCallBackServiceInvoke";

    /**
     * 单例对象
     */
    private static DubboProtocol INSTANCE;

    /**
     * 通信服务器集合
     * key: 服务器地址 host:port
     */
    private final Map<String, ExchangeServer> serverMap = new ConcurrentHashMap<String, ExchangeServer>(); // <host:port,Exchanger>

    /**
     * key  服务键 value 包含引用计数的 client 对象
     */
    private final Map<String, ReferenceCountExchangeClient> referenceClientMap = new ConcurrentHashMap<String, ReferenceCountExchangeClient>(); // <host:port,Exchanger>
    /**
     * 是不是代表应该被销毁的client
     */
    private final ConcurrentMap<String, LazyConnectExchangeClient> ghostClientMap = new ConcurrentHashMap<String, LazyConnectExchangeClient>();
    private final ConcurrentMap<String, Object> locks = new ConcurrentHashMap<String, Object>();
    /**
     * 序列化优化器容器
     */
    private final Set<String> optimizers = new ConcurrentHashSet<String>();
    //consumer side export a stub service for dispatching event
    //servicekey-stubmethods
    private final ConcurrentMap<String, String> stubServiceMethodsMap = new ConcurrentHashMap<String, String>();

    /**
     * 处理请求对象
     */
    private ExchangeHandler requestHandler = new ExchangeHandlerAdapter() {

        /**
         * 回复
         * @param channel
         * @param message
         * @return
         * @throws RemotingException
         */
        @Override
        public CompletableFuture<Object> reply(ExchangeChannel channel, Object message) throws RemotingException {
            //如果 传入的数据是 invocation 类型
            if (message instanceof Invocation) {
                //转换
                Invocation inv = (Invocation) message;
                //获取 invoker 对象
                Invoker<?> invoker = getInvoker(channel, inv);
                // need to consider backward-compatibility if it's a callback
                //判断是否是回调 需要考虑向后兼容性
                if (Boolean.TRUE.toString().equals(inv.getAttachments().get(IS_CALLBACK_SERVICE_INVOKE))) {
                    //获取方法名
                    String methodsStr = invoker.getUrl().getParameters().get("methods");
                    boolean hasMethod = false;
                    if (methodsStr == null || !methodsStr.contains(",")) {
                        //如果 invoker 的 方法名和 获取到的一样 没有也是 true???
                        hasMethod = inv.getMethodName().equals(methodsStr);
                    } else {
                        //拆分 获取 方法数组
                        String[] methods = methodsStr.split(",");
                        for (String method : methods) {
                            //找到一个对应方法就是 hashMethod
                            if (inv.getMethodName().equals(method)) {
                                hasMethod = true;
                                break;
                            }
                        }
                    }
                    //没有找到 对应方法就抛出异常
                    if (!hasMethod) {
                        logger.warn(new IllegalStateException("The methodName " + inv.getMethodName()
                                + " not found in callback service interface ,invoke will be ignored."
                                + " please update the api interface. url is:"
                                + invoker.getUrl()) + " ,invocation is :" + inv);
                        return null;
                    }
                }
                //获取上下文对象
                RpcContext rpcContext = RpcContext.getContext();
                //判断是否是异步
                boolean supportServerAsync = invoker.getUrl().getMethodParameter(inv.getMethodName(), Constants.ASYNC_KEY, false);
                if (supportServerAsync) {
                    //创建异步对象并保存到上下文中
                    CompletableFuture<Object> future = new CompletableFuture<>();
                    rpcContext.setAsyncContext(new AsyncContextImpl(future));
                }
                //设置远程地址
                rpcContext.setRemoteAddress(channel.getRemoteAddress());
                Result result = invoker.invoke(inv);

                /**
                 * 如果是异步 结果
                 */
                if (result instanceof AsyncRpcResult) {
                    return ((AsyncRpcResult) result).getResultFuture().thenApply(r -> (Object) r);
                } else {
                    //计算结果
                    return CompletableFuture.completedFuture(result);
                }
            }
            throw new RemotingException(channel, "Unsupported request: "
                    + (message == null ? null : (message.getClass().getName() + ": " + message))
                    + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress());
        }

        /**
         * 接受到请求后
         * @param channel
         * @param message
         * @throws RemotingException
         */
        @Override
        public void received(Channel channel, Object message) throws RemotingException {
            if (message instanceof Invocation) {
                //是invocation 就直接处理
                reply((ExchangeChannel) channel, message);
            } else {
                //否则委托给父类
                super.received(channel, message);
            }
        }

        @Override
        public void connected(Channel channel) throws RemotingException {
            //传入 指定的key 来执行对应任务
            invoke(channel, Constants.ON_CONNECT_KEY);
        }

        @Override
        public void disconnected(Channel channel) throws RemotingException {
            if (logger.isInfoEnabled()) {
                logger.info("disconnected from " + channel.getRemoteAddress() + ",url:" + channel.getUrl());
            }
            invoke(channel, Constants.ON_DISCONNECT_KEY);
        }

        /**
         * 根据指定的key 执行不同的逻辑
         * @param channel
         * @param methodKey
         */
        private void invoke(Channel channel, String methodKey) {
            Invocation invocation = createInvocation(channel, channel.getUrl(), methodKey);
            if (invocation != null) {
                try {
                    received(channel, invocation);
                } catch (Throwable t) {
                    logger.warn("Failed to invoke event method " + invocation.getMethodName() + "(), cause: " + t.getMessage(), t);
                }
            }
        }

        /**
         * 创建invoker 对象
         * @param channel
         * @param url
         * @param methodKey
         * @return
         */
        private Invocation createInvocation(Channel channel, URL url, String methodKey) {
            //通过 对应配置获取属性 一般情况 connect 和disconnect 都是不设置的  这个应该是 连接或断开连接触发的 方法
            String method = url.getParameter(methodKey);
            if (method == null || method.length() == 0) {
                return null;
            }
            RpcInvocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
            invocation.setAttachment(Constants.PATH_KEY, url.getPath());
            invocation.setAttachment(Constants.GROUP_KEY, url.getParameter(Constants.GROUP_KEY));
            invocation.setAttachment(Constants.INTERFACE_KEY, url.getParameter(Constants.INTERFACE_KEY));
            invocation.setAttachment(Constants.VERSION_KEY, url.getParameter(Constants.VERSION_KEY));
            if (url.getParameter(Constants.STUB_EVENT_KEY, false)) {
                invocation.setAttachment(Constants.STUB_EVENT_KEY, Boolean.TRUE.toString());
            }
            return invocation;
        }
    };

    public DubboProtocol() {
        INSTANCE = this;
    }

    public static DubboProtocol getDubboProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(DubboProtocol.NAME); // load
        }
        return INSTANCE;
    }

    public Collection<ExchangeServer> getServers() {
        return Collections.unmodifiableCollection(serverMap.values());
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }

    Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    /**
     * 判断是否是客户端 方
     * @param channel
     * @return
     */
    private boolean isClientSide(Channel channel) {
        //获取远程地址
        InetSocketAddress address = channel.getRemoteAddress();
        //获取url 对象
        URL url = channel.getUrl();
        //如果 url 的 端口和 远程地址的 端口相同
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(channel.getUrl().getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    /**
     * 从 invocation 中获取invoker 对象
     * @param channel
     * @param inv
     * @return
     * @throws RemotingException
     */
    Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
        boolean isCallBackServiceInvoke = false;
        boolean isStubServiceInvoke = false;
        //获取端口和 地址
        int port = channel.getLocalAddress().getPort();
        String path = inv.getAttachments().get(Constants.PATH_KEY);
        // if it's callback service on client side
        //如果是 存根类型
        isStubServiceInvoke = Boolean.TRUE.toString().equals(inv.getAttachments().get(Constants.STUB_EVENT_KEY));
        if (isStubServiceInvoke) {
            //使用远程端口
            port = channel.getRemoteAddress().getPort();
        }
        //callback
        //判断是否是 回调
        isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;
        if (isCallBackServiceInvoke) {
            //获取回调路径
            path = inv.getAttachments().get(Constants.PATH_KEY) + "." + inv.getAttachments().get(Constants.CALLBACK_SERVICE_KEY);
            //设置 代表开启回调的 标识
            inv.getAttachments().put(IS_CALLBACK_SERVICE_INVOKE, Boolean.TRUE.toString());
        }
        //拼接传入的参数生成 服务键
        String serviceKey = serviceKey(port, path, inv.getAttachments().get(Constants.VERSION_KEY), inv.getAttachments().get(Constants.GROUP_KEY));

        //通过服务键获取 export 对象
        DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);

        if (exporter == null) {
            throw new RemotingException(channel, "Not found exported service: " + serviceKey + " in " + exporterMap.keySet() + ", may be version or group mismatch " + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress() + ", message:" + inv);
        }

        //从出口对象获取invoker
        return exporter.getInvoker();
    }

    public Collection<Invoker<?>> getInvokers() {
        return Collections.unmodifiableCollection(invokers);
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * dubbo 协议的 出口方法
     * @param invoker Service invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        //获取 invoker 对象的url 这个应该就是 注册中心 的 url
        URL url = invoker.getUrl();

        // export service.
        // 通过 url 生成对应的 服务键对象 也就是一个特殊的key
        String key = serviceKey(url);
        //创建 出口者对象 该对象是 单例模式 exporterMap 应该是只有一个
        DubboExporter<T> exporter = new DubboExporter<T>(invoker, key, exporterMap);
        //将服务键 和 出口对象保存到容器中  那么其他对象在 进行出口的时候一旦操作或者map 对象都会影响到 其他 exporter对象的 map
        exporterMap.put(key, exporter);

        //export an stub service for dispatching event
        //获取 一些 参数信息  这块跟 stub 挂钩先不管
        Boolean isStubSupportEvent = url.getParameter(Constants.STUB_EVENT_KEY, Constants.DEFAULT_STUB_EVENT);
        Boolean isCallbackservice = url.getParameter(Constants.IS_CALLBACK_SERVICE, false);

        //如果 支持 stub事件 且 不存在回调  什么意思???
        if (isStubSupportEvent && !isCallbackservice) {
            //从 参数中 获取 stub 的方法
            String stubServiceMethods = url.getParameter(Constants.STUB_EVENT_METHODS_KEY);
            if (stubServiceMethods == null || stubServiceMethods.length() == 0) {
                if (logger.isWarnEnabled()) {
                    logger.warn(new IllegalStateException("consumer [" + url.getParameter(Constants.INTERFACE_KEY) +
                            "], has set stubproxy support event ,but no stub methods founded."));
                }
            } else {
                //在 stub 方法容器中 增加对应键值对
                stubServiceMethodsMap.put(url.getServiceKey(), stubServiceMethods);
            }
        }

        //启动服务器
        openServer(url);

        //初始化序列化优化器
        optimizeSerialization(url);
        return exporter;
    }

    /**
     * 启动服务器  这里应该是 根据 注册中心的url 创建服务端对象
     * @param url
     */
    private void openServer(URL url) {
        // find server.
        String key = url.getAddress();
        //client can export a service which's only for server to invoke
        boolean isServer = url.getParameter(Constants.IS_SERVER_KEY, true);
        if (isServer) {
            ExchangeServer server = serverMap.get(key);
            if (server == null) {
                synchronized (this) {
                    server = serverMap.get(key);
                    if (server == null) {
                        //创建 新的服务器对象
                        serverMap.put(key, createServer(url));
                    }
                }
            } else {
                // server supports reset, use together with override
                // 将server 地址重置
                server.reset(url);
            }
        }
    }

    /**
     * 创建服务器对象
     * @param url
     * @return
     */
    private ExchangeServer createServer(URL url) {
        // send readonly event when server closes, it's enabled by default
        // 在关闭server 时 能够发送 read_only 事件
        url = url.addParameterIfAbsent(Constants.CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString());
        // enable heartbeat by default
        // 设置心跳检测的定时时间
        url = url.addParameterIfAbsent(Constants.HEARTBEAT_KEY, String.valueOf(Constants.DEFAULT_HEARTBEAT));
        // 获取服务器的 实现类型
        String str = url.getParameter(Constants.SERVER_KEY, Constants.DEFAULT_REMOTING_SERVER);

        //对应的 服务器实现类 没有找到
        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported server type: " + str + ", url: " + url);
        }

        //设置编码器 为 dubbo
        url = url.addParameter(Constants.CODEC_KEY, DubboCodec.NAME);
        ExchangeServer server;
        try {
            //绑定到指定地址生成服务器对象
            server = Exchangers.bind(url, requestHandler);
        } catch (RemotingException e) {
            throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
        }
        //从 url 中 获取  client 信息 并查询 是否有支持客户端的 通信框架实现 一般是没有设置的
        str = url.getParameter(Constants.CLIENT_KEY);
        if (str != null && str.length() > 0) {
            Set<String> supportedTypes = ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions();
            if (!supportedTypes.contains(str)) {
                throw new RpcException("Unsupported client type: " + str);
            }
        }
        return server;
    }

    /**
     * 初始化 序列化优化器
     * @param url
     * @throws RpcException
     */
    private void optimizeSerialization(URL url) throws RpcException {
        //获取 url 中的 optimize 属性
        String className = url.getParameter(Constants.OPTIMIZER_KEY, "");
        //如果 该优化器已经存在 获取没有 设置 就直接返回
        if (StringUtils.isEmpty(className) || optimizers.contains(className)) {
            return;
        }

        logger.info("Optimizing the serialization process for Kryo, FST, etc...");

        try {
            //通过当前线程类加载器加载 指定的 序列化优化器
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            //如果该类 没有实现 序列化优化器接口 抛出异常
            if (!SerializationOptimizer.class.isAssignableFrom(clazz)) {
                throw new RpcException("The serialization optimizer " + className + " isn't an instance of " + SerializationOptimizer.class.getName());
            }

            //创建 优化器实例
            SerializationOptimizer optimizer = (SerializationOptimizer) clazz.newInstance();

            //没有获取到 序列化类对象直接返回
            if (optimizer.getSerializableClasses() == null) {
                return;
            }

            //遍历每个 在 注册中心进行注册
            for (Class c : optimizer.getSerializableClasses()) {
                SerializableClassRegistry.registerClass(c);
            }

            //优化器 保存到 容器中
            optimizers.add(className);
        } catch (ClassNotFoundException e) {
            throw new RpcException("Cannot find the serialization optimizer class: " + className, e);
        } catch (InstantiationException e) {
            throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);
        } catch (IllegalAccessException e) {
            throw new RpcException("Cannot instantiate the serialization optimizer class: " + className, e);
        }
    }

    /**
     * dubbo 协议层的 引用 invoker 消费者 借助通信层 应该是会调用这个方法
     * @param serviceType
     * @param url  URL address for the remote service
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Invoker<T> refer(Class<T> serviceType, URL url) throws RpcException {
        //初始化 序列器对象
        optimizeSerialization(url);
        // create rpc invoker.
        //创建 invoker 对象
        DubboInvoker<T> invoker = new DubboInvoker<T>(serviceType, url, getClients(url), invokers);
        //全局容器中增加一个 发布的invoker对象
        invokers.add(invoker);
        return invoker;
    }

    /**
     * 通过url 获取 exchangeClient
     * @param url 这是服务提供者的 url
     * @return
     */
    private ExchangeClient[] getClients(URL url) {
        // whether to share connection
        // 是否 共享连接
        boolean service_share_connect = false;
        //获取 最大连接数 默认是0
        int connections = url.getParameter(Constants.CONNECTIONS_KEY, 0);
        // if not configured, connection is shared, otherwise, one connection for one service
        // 0 代表共享
        if (connections == 0) {
            service_share_connect = true;
            connections = 1;
        }

        //创建 对应数量的 client 对象
        ExchangeClient[] clients = new ExchangeClient[connections];
        for (int i = 0; i < clients.length; i++) {
            if (service_share_connect) {
                //获取 共享 url
                clients[i] = getSharedClient(url);
            } else {
                clients[i] = initClient(url);
            }
        }
        return clients;
    }

    /**
     * Get shared connection
     * 获取 共享连接
     */
    private ExchangeClient getSharedClient(URL url) {
        //获取 url 的地址  ip:port
        String key = url.getAddress();
        //获取client对象
        ReferenceCountExchangeClient client = referenceClientMap.get(key);
        //client 对象存在时
        if (client != null) {
            //增加 计数
            if (!client.isClosed()) {
                client.incrementAndGetCount();
                return client;
            } else {
                //不存在了就移除关联
                referenceClientMap.remove(key);
            }
        }

        //跟rocketMq 一样每个 key 针对一个 锁对象 保证每一个客户端是串行执行
        locks.putIfAbsent(key, new Object());
        synchronized (locks.get(key)) {
            if (referenceClientMap.containsKey(key)) {
                return referenceClientMap.get(key);
            }

            //通过url 初始化 client对象
            ExchangeClient exchangeClient = initClient(url);
            //封装成 计数client  将 ghostclient 传给 ReferenceCountExchangeClient
            client = new ReferenceCountExchangeClient(exchangeClient, ghostClientMap);
            referenceClientMap.put(key, client);
            ghostClientMap.remove(key);
            locks.remove(key);
            return client;
        }
    }

    /**
     * Create new connection
     * 初始化 client 对象
     */
    private ExchangeClient initClient(URL url) {

        // client type setting.
        // 获取 client 的 通信框架类型
        String str = url.getParameter(Constants.CLIENT_KEY, url.getParameter(Constants.SERVER_KEY, Constants.DEFAULT_REMOTING_CLIENT));

        //设置编码 器
        url = url.addParameter(Constants.CODEC_KEY, DubboCodec.NAME);
        // enable heartbeat by default
        //设置心跳检测时间
        url = url.addParameterIfAbsent(Constants.HEARTBEAT_KEY, String.valueOf(Constants.DEFAULT_HEARTBEAT));

        // BIO is not allowed since it has severe performance issue.
        //传入不存在 的 通信框架 会报错
        if (str != null && str.length() > 0 && !ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported client type: " + str + "," +
                    " supported client type is " + StringUtils.join(ExtensionLoader.getExtensionLoader(Transporter.class).getSupportedExtensions(), " "));
        }

        ExchangeClient client;
        try {
            // connection should be lazy
            // 获取 是否延迟
            if (url.getParameter(Constants.LAZY_CONNECT_KEY, false)) {
                //创建 延迟 client
                client = new LazyConnectExchangeClient(url, requestHandler);
            } else {
                //创建普通client 通过连接服务器
                client = Exchangers.connect(url, requestHandler);
            }
        } catch (RemotingException e) {
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }
        return client;
    }

    @Override
    public void destroy() {
        //销毁所有服务器
        for (String key : new ArrayList<String>(serverMap.keySet())) {
            ExchangeServer server = serverMap.remove(key);
            if (server != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo server: " + server.getLocalAddress());
                    }
                    //给与一定关闭时间 好处理已经接受到的 请求
                    server.close(ConfigUtils.getServerShutdownTimeout());
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }

        //关闭所有客户端
        for (String key : new ArrayList<String>(referenceClientMap.keySet())) {
            ExchangeClient client = referenceClientMap.remove(key);
            if (client != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
                    }
                    client.close(ConfigUtils.getServerShutdownTimeout());
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }

        //幽灵客户端也关闭
        for (String key : new ArrayList<String>(ghostClientMap.keySet())) {
            ExchangeClient client = ghostClientMap.remove(key);
            if (client != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
                    }
                    client.close(ConfigUtils.getServerShutdownTimeout());
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
        //清理存根容器
        stubServiceMethodsMap.clear();
        //继续 父类的销毁工作
        super.destroy();
    }
}
