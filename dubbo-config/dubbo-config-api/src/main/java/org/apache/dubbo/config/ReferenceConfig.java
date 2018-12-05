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
package org.apache.dubbo.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.config.AsyncFor;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.AvailableCluster;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;

/**
 * ReferenceConfig
 * 引用的 配置对象
 * @export
 */
public class ReferenceConfig<T> extends AbstractReferenceConfig {

    private static final long serialVersionUID = -5864351140409987595L;

    /**
     * 自适应的 Protocol 对象 根据 传入的 url 不同 动态调用方法
     */
    private static final Protocol refprotocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    /**
     * 自适应集群对象
     */
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    /**
     * 自适应的代理工厂对象
     */
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /**
     * 所有注册中心的 信息 （如果是直连情况 就是 提供者 url）
     */
    private final List<URL> urls = new ArrayList<URL>();
    // interface name
    private String interfaceName;
    private Class<?> interfaceClass;
    private Class<?> asyncInterfaceClass;
    // client type
    private String client;
    /**
     * 这个 是 直连地址 url
     */
    private String url;
    // method configs
    private List<MethodConfig> methods;
    // default config
    /**
     * 消费者配置
     */
    private ConsumerConfig consumer;
    private String protocol;
    /**
     * 代理后的对象引用 也就是保存服务提供者的引用
     */
    // interface proxy reference
    private transient volatile T ref;
    private transient volatile Invoker<?> invoker;
    private transient volatile boolean initialized;
    private transient volatile boolean destroyed;
    @SuppressWarnings("unused")
    private final Object finalizerGuardian = new Object() {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();

            if (!ReferenceConfig.this.destroyed) {
                logger.warn("ReferenceConfig(" + url + ") is not DESTROYED when FINALIZE");

                /* don't destroy for now
                try {
                    ReferenceConfig.this.destroy();
                } catch (Throwable t) {
                        logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url + ") in finalize method!", t);
                }
                */
            }
        }
    };

    public ReferenceConfig() {
    }

    /**
     * 通过给定的 注解 进行初始化 class 对象定位方法 reference 对象提供真实的属性 这个是子类通过spring 注解方式进行初始化时创建的 所以优先使用注解属性进行初始化
     * @param reference
     */
    public ReferenceConfig(Reference reference) {
        appendAnnotation(Reference.class, reference);
    }

    /**
     * 从url中返回第一个对象
     * @return
     */
    public URL toUrl() {
        return urls.isEmpty() ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    /**
     * 引用服务 (服务消费者) 就是通过这个方法获取 服务提供者的 实现类的
     * @return
     */
    public synchronized T get() {
        //如果 已经停止了
        if (destroyed) {
            throw new IllegalStateException("Already destroyed!");
        }
        //如果 没有 获取到的 实现对象 就 进行初始化
        if (ref == null) {
            //这里 初始化 的同时 会从 服务提供者那里获取ref 对象
            init();
        }
        return ref;
    }

    public synchronized void destroy() {
        if (ref == null) {
            return;
        }
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            invoker.destroy();
        } catch (Throwable t) {
            logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url + ").", t);
        }
        invoker = null;
        ref = null;
    }

    /**
     * 初始化 服务消费者 获取ref 对象
     */
    private void init() {
        //如果 已经初始化完成 就直接返回
        if (initialized) {
            return;
        }
        //设置 以初始化的标识
        initialized = true;
        //需要被 代理的 接口 没有设置就抛出异常
        if (interfaceName == null || interfaceName.length() == 0) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }
        //创建并加载 消费者 配置
        checkDefault();
        //为本对象 从 系统变量中 获取 属性
        appendProperties(this);
        //当 泛化模式 为null 消费者不为null
        if (getGeneric() == null && getConsumer() != null) {
            //从消费者配置中 获取泛化配置 并设置
            setGeneric(getConsumer().getGeneric());
        }
        //根据 泛化 信息判断是否是 泛化模式  为 true nativejava bean 代表是泛化模式
        if (ProtocolUtils.isGeneric(getGeneric())) {
            //将接口 变成了 泛化接口
            interfaceClass = GenericService.class;
        } else {
            try {
                //反射创建对应的接口对象  反射是可以创建接口对象的
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            //检查 该接口中是否 存在这些方法
            checkInterfaceAndMethods(interfaceClass, methods);
        }

        //获取 该 接口的 配置信息  这段逻辑代表的是 获取 直连服务提供者地址
        String resolve = System.getProperty(interfaceName);
        String resolveFile = null;
        //当获取不到配置时
        if (resolve == null || resolve.length() == 0) {
            //获取 文件
            resolveFile = System.getProperty("dubbo.resolve.file");
            if (resolveFile == null || resolveFile.length() == 0) {
                //尝试 从properties
                File userResolveFile = new File(new File(System.getProperty("user.home")), "dubbo-resolve.properties");
                if (userResolveFile.exists()) {
                    //创建成功后 获取绝对路径
                    resolveFile = userResolveFile.getAbsolutePath();
                }
            }
            //如果获取到了该文件
            if (resolveFile != null && resolveFile.length() > 0) {
                Properties properties = new Properties();
                FileInputStream fis = null;
                try {
                    //从文件中读取属性
                    fis = new FileInputStream(new File(resolveFile));
                    properties.load(fis);
                } catch (IOException e) {
                    throw new IllegalStateException("Unload " + resolveFile + ", cause: " + e.getMessage(), e);
                } finally {
                    try {
                        if (null != fis) {
                            fis.close();
                        }
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
                //从 配置中获取 resolve 信息
                resolve = properties.getProperty(interfaceName);
            }
        }
        //如果 直接获取到了 resolve
        if (resolve != null && resolve.length() > 0) {
            //将直连url 设置成 resolve
            url = resolve;
            if (logger.isWarnEnabled()) {
                if (resolveFile != null) {
                    logger.warn("Using default dubbo resolve file " + resolveFile + " replace " + interfaceName + "" + resolve + " to p2p invoke remote service.");
                } else {
                    logger.warn("Using -D" + interfaceName + "=" + resolve + " to p2p invoke remote service.");
                }
            }
        }
        if (consumer != null) {
            //从消费者中 获取对应配置
            if (application == null) {
                application = consumer.getApplication();
            }
            if (module == null) {
                module = consumer.getModule();
            }
            if (registries == null) {
                registries = consumer.getRegistries();
            }
            if (monitor == null) {
                monitor = consumer.getMonitor();
            }
        }
        if (module != null) {
            if (registries == null) {
                registries = module.getRegistries();
            }
            if (monitor == null) {
                monitor = module.getMonitor();
            }
        }
        if (application != null) {
            if (registries == null) {
                registries = application.getRegistries();
            }
            if (monitor == null) {
                monitor = application.getMonitor();
            }
        }
        //检查配置 并从环境变量中获取属性
        checkApplication();
        //检查 stub
        checkStub(interfaceClass);
        //这个  看不懂先不管 就是校验mock方法的
        checkMock(interfaceClass);
        Map<String, String> map = new HashMap<String, String>();
        //修改一些 interfaceClass 的信息
        resolveAsyncInterface(interfaceClass, map);

        //设置 时间戳 协议版本 和 side标识
        map.put(Constants.SIDE_KEY, Constants.CONSUMER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        //设置 pid
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        //如果 不是 泛化类型
        if (!isGeneric()) {
            //获取配置信息
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }

            //将 该接口 包装 增加几个 方法  还没看懂
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", Constants.ANY_VALUE);
            } else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        //设置 接口名
        map.put(Constants.INTERFACE_KEY, interfaceName);
        //从配置中获取相关属性设置到map中
        appendParameters(map, application);
        appendParameters(map, module);
        //为 设置的属性增加 default 前缀
        appendParameters(map, consumer, Constants.DEFAULT_KEY);
        appendParameters(map, this);
        Map<String, Object> attributes = null;
        if (methods != null && !methods.isEmpty()) {
            attributes = new HashMap<String, Object>();
            for (MethodConfig methodConfig : methods) {
                //从每个 方法配置中抽出  属性 第三个参数 是 前缀
                appendParameters(map, methodConfig, methodConfig.getName());
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    //跟服务提供者一样的  移除重试属性  设置重试次数属性
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
                //将 方法 与 调用时触发的 相关回调 保存到容器中 这里会涉及到调用链 在调用链中触发 设定的回调
                attributes.put(methodConfig.getName(), convertMethodConfig2AyncInfo(methodConfig));
            }
        }

        //从环境变量或 系统变量中 获取 订阅到 注册中心的 ip
        String hostToRegistry = ConfigUtils.getSystemProperty(Constants.DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry == null || hostToRegistry.length() == 0) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + Constants.DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }
        //保存端口信息
        map.put(Constants.REGISTER_IP_KEY, hostToRegistry);

        //通过配置 创建  代理对象
        ref = createProxy(map);

        //创建 服务消费者包装对象 并保存到全局容器中
        ConsumerModel consumerModel = new ConsumerModel(getUniqueServiceName(), ref, interfaceClass.getMethods(), attributes);
        ApplicationModel.initConsumerModel(getUniqueServiceName(), consumerModel);
    }

    /**
     * 生成代理对象
     */
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    private T createProxy(Map<String, String> map) {
        //创建新的 url 对象 使用临时协议 和本地ip
        URL tmpUrl = new URL("temp", "localhost", 0, map);
        final boolean isJvmRefer;
        //是否是 本地引用  本地服务在发布的 时候只是创建了一个injvmExport对象并保存在一个容器中
        if (isInjvm() == null) {
            //存在 url 代表是 直连模式
            if (url != null && url.length() > 0) { // if a url is specified, don't do local reference
                isJvmRefer = false;
            } else {
                // by default, reference local service if there is
                // 根据 传入的 url 判断是不是 本地引用
                isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = isInjvm();
        }

        //本地引用情况
        if (isJvmRefer) {
            //创建本地 url 使用本机 ip
            URL url = new URL(Constants.LOCAL_PROTOCOL, NetUtils.LOCALHOST, 0, interfaceClass.getName()).addParameters(map);
            //获取自适应拓展对象 通过url 的 协议 也就是 injvmProtocol 返回了invoker对象
            invoker = refprotocol.refer(interfaceClass, url);
            if (logger.isInfoEnabled()) {
                logger.info("Using injvm service " + interfaceClass.getName());
            }
        } else {
            //远程调用 当url 不为null的时候 代表是 直连
            if (url != null && url.length() > 0) { // user specified URL, could be peer-to-peer address, or register center's address.
                //通过 ; 拆分 url
                String[] us = Constants.SEMICOLON_SPLIT_PATTERN.split(url);
                if (us != null && us.length > 0) {
                    for (String u : us) {
                        //将字符串 转换成 url 对象
                        URL url = URL.valueOf(u);
                        if (url.getPath() == null || url.getPath().length() == 0) {
                            //设置path 为接口名
                            url = url.setPath(interfaceName);
                        }
                        //如果 直连 url 中 存在 registry 为协议的 对象 那么还是要 从 注册中心 获取 提供者
                        if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                            //给url 对象增加属性
                            urls.add(url.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                        } else {
                            //这里就是将 提供者url 中的 部分属性使用 传入的 map  以及去掉部分属性 比较繁琐等用到的时候再查看对应属性
                            urls.add(ClusterUtils.mergeUrl(url, map));
                        }
                    }
                }
                //代表从注册中心 发现服务提供者
            } else { // assemble URL from register center's configuration
                //获取所有的 注册中心地址
                List<URL> us = loadRegistries(false);
                if (us != null && !us.isEmpty()) {
                    for (URL u : us) {
                        //获取监控中心信息 如果监控中心 的 协议类型是 registry 代表从注册中心生成 监控中心
                        URL monitorUrl = loadMonitor(u);
                        if (monitorUrl != null) {
                            map.put(Constants.MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                        }
                        //将map 中属性 以 refer 作为key 保存 map中还保存了集群策略
                        urls.add(u.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                    }
                }
                if (urls.isEmpty()) {
                    throw new IllegalStateException("No such any registry to reference " + interfaceName + " on the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                }
            }

            //直连 或是从注册中心 发现
            if (urls.size() == 1) {
                //通过唯一的 注册中心 进行 引用 或者就是 直连 直接使用dubboProtocol 获取invoker
                invoker = refprotocol.refer(interfaceClass, urls.get(0));
            } else {
                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;
                for (URL url : urls) {
                    //将所有注册中心 返回的 对象都保存起来
                    invokers.add(refprotocol.refer(interfaceClass, url));
                    if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                        //这里 保存了 最后一个 协议是 registy的 url
                        registryURL = url; // use last registry url
                    }
                }
                //如果 有 从 注册中心返回的 可以构建成 集群模式 每次 从返回的所有invoker 中根据一定规则 获取 一个invoker
                if (registryURL != null) { // registry url is available
                    // use AvailableCluster only when register's cluster is available

                    //这下面根据 直连 和 注册中心生成不同的集群 现在 还体现不出来 之后再看 具体实现

                    //这里代表从所有注册中心 获取到的可用的单个invoker 对象 然后又用集群对象包装使用 available选择第一个可用的invoker对象
                    URL u = registryURL.addParameter(Constants.CLUSTER_KEY, AvailableCluster.NAME);
                    //这里使用第一个可用的 注册中心获取到的invoker 然后这个invoker 又是通过集群包裹的 invoker 根据集群容错策略
                    //返回一个合适的 invoker对象 如果是 group模式 那么 按组还有一层集群
                    invoker = cluster.join(new StaticDirectory(u, invokers));
                } else { // not a registry url
                    //没有url属性就获取 invoekr 中的 url 然后找不到关于集群的设定就使用默认的
                    invoker = cluster.join(new StaticDirectory(invokers));
                }
            }
        }

        //是否开启检查
        Boolean c = check;
        if (c == null && consumer != null) {
            //尝试从消费者 配置中查看是否需要检查
            c = consumer.isCheck();
        }
        if (c == null) {
            c = true; // default true
        }
        //发现invoker 不可用 初始化失败 并抛出异常 针对集群情况 任意一个 invoker 可用 就是可用
        if (c && !invoker.isAvailable()) {
            // make it possible for consumer to retry later if provider is temporarily unavailable
            initialized = false;
            throw new IllegalStateException("Failed to check the status of the service " + interfaceName + ". No provider available for the service " + (group == null ? "" : group + "/") + interfaceName + (version == null ? "" : ":" + version) + " from the url " + invoker.getUrl() + " to the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }
        if (logger.isInfoEnabled()) {
            logger.info("Refer dubbo service " + interfaceClass.getName() + " from url " + invoker.getUrl());
        }
        // create service proxy
        // 这里将invoker 对象变成了 代理对象
        return (T) proxyFactory.getProxy(invoker);
    }

    /**
     * 检查配置 如果消费者配置 未被初始化 就 创建 并 从系统变量中 加载对应属性
     */
    private void checkDefault() {
        if (consumer == null) {
            consumer = new ConsumerConfig();
        }
        appendProperties(consumer);
    }

    /**
     *
     * @param interfaceClass 需要被 服务提供者实现的接口
     * @param map
     */
    private void resolveAsyncInterface(Class<?> interfaceClass, Map<String, String> map) {
        //从接口类上获取 异步注解
        AsyncFor annotation = interfaceClass.getAnnotation(AsyncFor.class);
        if (annotation == null) {
            return;
        }
        //获取 原 接口
        Class<?> target = annotation.value();
        //如果 演接口 不是 该接口的 父类 就返回
        if (!target.isAssignableFrom(interfaceClass)) {
            return;
        }
        //将 异步接口 改成 本接口
        this.asyncInterfaceClass = interfaceClass;
        //本接口 变成 注解上的值
        this.interfaceClass = target;
        //修改 interfaceName的值  如果 id不存在 也一起修改
        setInterface(this.interfaceClass.getName());
        //将 接口 信息保存到容器中 这里保存的 还是 异步接口
        map.put(Constants.INTERFACES, interfaceClass.getName());
    }


    public Class<?> getInterfaceClass() {
        if (interfaceClass != null) {
            return interfaceClass;
        }
        if (isGeneric()
                || (getConsumer() != null && getConsumer().isGeneric())) {
            return GenericService.class;
        }
        try {
            if (interfaceName != null && interfaceName.length() > 0) {
                this.interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            }
        } catch (ClassNotFoundException t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
        return interfaceClass;
    }

    /**
     * @param interfaceClass
     * @see #setInterface(Class)
     * @deprecated
     */
    @Deprecated
    public void setInterfaceClass(Class<?> interfaceClass) {
        setInterface(interfaceClass);
    }

    public String getInterface() {
        return interfaceName;
    }

    public void setInterface(Class<?> interfaceClass) {
        if (interfaceClass != null && !interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        this.interfaceClass = interfaceClass;
        setInterface(interfaceClass == null ? null : interfaceClass.getName());
    }

    /**
     * 将接口名称 设置成给定的接口名
     * @param interfaceName
     */
    public void setInterface(String interfaceName) {
        this.interfaceName = interfaceName;
        if (id == null || id.length() == 0) {
            id = interfaceName;
        }
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        checkName("client", client);
        this.client = client;
    }

    @Parameter(excluded = true)
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<MethodConfig> getMethods() {
        return methods;
    }

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (List<MethodConfig>) methods;
    }

    public ConsumerConfig getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerConfig consumer) {
        this.consumer = consumer;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    // just for test
    Invoker<?> getInvoker() {
        return invoker;
    }

    @Parameter(excluded = true)
    public String getUniqueServiceName() {
        StringBuilder buf = new StringBuilder();
        if (group != null && group.length() > 0) {
            buf.append(group).append("/");
        }
        buf.append(interfaceName);
        if (version != null && version.length() > 0) {
            buf.append(":").append(version);
        }
        return buf.toString();
    }

}
