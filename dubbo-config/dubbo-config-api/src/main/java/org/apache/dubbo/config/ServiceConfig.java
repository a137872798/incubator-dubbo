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
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ClassHelper;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.utils.NetUtils.LOCALHOST;
import static org.apache.dubbo.common.utils.NetUtils.getAvailablePort;
import static org.apache.dubbo.common.utils.NetUtils.getLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidPort;

/**
 * ServiceConfig
 *
 * @export
 */
public class ServiceConfig<T> extends AbstractServiceConfig {

    private static final long serialVersionUID = 3033787999037024738L;

    /**
     * 通过 SPI 拓展机制 获取 自适应@Adaptive 对象 也就是 自适应对象 该对象在创建的时候不能确定 实际的实现方式
     * 在 调用对应自适应方法时 通过传入不同的 url 读取其中的属性 能够动态执行合适的逻辑
     */
    private static final Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    /**
     * 这个对象就是 创建 代理对象的 默认使用 javassist实现
     */
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /**
     * 保留了协议 名 与端口号的映射关系
     */
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    /**
     * 延迟暴露服务的 定时器 在进行暴露时 通过 判断delay 属性 决定是否 直接执行
     */
    private static final ScheduledExecutorService delayExportExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("DubboServiceDelayExporter", true));

    /**
     * 代表 以每种协议生成的 url 每个url 又会发布到全部的注册中心上
     */
    private final List<URL> urls = new ArrayList<URL>();

    /**
     * 服务配置的 出口对象 根据 scope 生成 可能是 remote + Local 也可能是单个 也可能是0
     */
    private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();
    // interface type
    private String interfaceName;

    /**
     * 实现的接口对象
     */
    private Class<?> interfaceClass;

    /**
     * reference to interface impl
     * 接口实现类
     */
    private T ref;
    // service name
    private String path;
    /**
     * method configuration
     * 出口的 方法配置
     */
    private List<MethodConfig> methods;
    private ProviderConfig provider;
    /**
     * 是否完成 发布
     */
    private transient volatile boolean exported;

    /**
     * 是否已经注销
     */
    private transient volatile boolean unexported;

    private volatile String generic;

    public ServiceConfig() {
    }

    public ServiceConfig(Service service) {
        appendAnnotation(Service.class, service);
    }

    /**
     * 将服务提供者配置 变成 协议配置
     * @param providers
     * @return
     */
    @Deprecated
    private static List<ProtocolConfig> convertProviderToProtocol(List<ProviderConfig> providers) {
        if (providers == null || providers.isEmpty()) {
            return null;
        }
        List<ProtocolConfig> protocols = new ArrayList<ProtocolConfig>(providers.size());
        for (ProviderConfig provider : providers) {
            protocols.add(convertProviderToProtocol(provider));
        }
        return protocols;
    }

    @Deprecated
    private static List<ProviderConfig> convertProtocolToProvider(List<ProtocolConfig> protocols) {
        if (protocols == null || protocols.isEmpty()) {
            return null;
        }
        List<ProviderConfig> providers = new ArrayList<ProviderConfig>(protocols.size());
        for (ProtocolConfig provider : protocols) {
            providers.add(convertProtocolToProvider(provider));
        }
        return providers;
    }

    /**
     * 从provider 中取出属性设置到 protocol中 因为
     * @param provider
     * @return
     */
    @Deprecated
    private static ProtocolConfig convertProviderToProtocol(ProviderConfig provider) {
        ProtocolConfig protocol = new ProtocolConfig();
        protocol.setName(provider.getProtocol().getName());
        protocol.setServer(provider.getServer());
        protocol.setClient(provider.getClient());
        protocol.setCodec(provider.getCodec());
        protocol.setHost(provider.getHost());
        protocol.setPort(provider.getPort());
        protocol.setPath(provider.getPath());
        protocol.setPayload(provider.getPayload());
        protocol.setThreads(provider.getThreads());
        protocol.setParameters(provider.getParameters());
        return protocol;
    }

    @Deprecated
    private static ProviderConfig convertProtocolToProvider(ProtocolConfig protocol) {
        ProviderConfig provider = new ProviderConfig();
        provider.setProtocol(protocol);
        provider.setServer(protocol.getServer());
        provider.setClient(protocol.getClient());
        provider.setCodec(protocol.getCodec());
        provider.setHost(protocol.getHost());
        provider.setPort(protocol.getPort());
        provider.setPath(protocol.getPath());
        provider.setPayload(protocol.getPayload());
        provider.setThreads(protocol.getThreads());
        provider.setParameters(protocol.getParameters());
        return provider;
    }

    /**
     * 根据不同协议名 获取端口号  如 dubbo
     */
    private static Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        if (RANDOM_PORT_MAP.containsKey(protocol)) {
            return RANDOM_PORT_MAP.get(protocol);
        }
        return Integer.MIN_VALUE;
    }

    private static void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
        }
    }

    public URL toUrl() {
        return urls.isEmpty() ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    @Parameter(excluded = true)
    public boolean isExported() {
        return exported;
    }

    @Parameter(excluded = true)
    public boolean isUnexported() {
        return unexported;
    }

    /**
     * 向注册中心暴露服务的 核心入口
     */
    public synchronized void export() {
        //如果 xml 中没有为 service 配置 provider 并且 provider 不是 default 这里就是null
        //也有可能 provider 没有配置
        if (provider != null) {
            if (export == null) {
                //如果 export 对象没有 就从provider 中获取
                export = provider.getExport();
            }
            if (delay == null) {
                delay = provider.getDelay();
            }
        }
        //这里是判断 该服务是否需要暴露的 标识 如果设置成 export = false 代表该服务不需要暴露
        if (export != null && !export) {
            return;
        }

        //当存在 延时时 通过定时器 触发 export 方法
        if (delay != null && delay > 0) {
            delayExportExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    doExport();
                }
            }, delay, TimeUnit.MILLISECONDS);
        } else {
            //为-1 代表不进行延时
            doExport();
        }
    }

    /**
     * 执行暴露逻辑
     */
    protected synchronized void doExport() {
        //如果 已经注销 不能进行暴露了 也就是一个服务 不能 反复暴露
        if (unexported) {
            throw new IllegalStateException("Already unexported!");
        }
        //如果 已经暴露成功 就不能发布到注册中心了
        if (exported) {
            return;
        }
        //修改 发布标识
        exported = true;
        //不存在 发布的 服务接口 抛出异常
        if (interfaceName == null || interfaceName.length() == 0) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }
        //检验配置信息  同时为provider 设置属性
        checkDefault();
        //尝试从providerConfig 中获取配置
        if (provider != null) {
            if (application == null) {
                application = provider.getApplication();
            }
            if (module == null) {
                module = provider.getModule();
            }
            if (registries == null) {
                registries = provider.getRegistries();
            }
            if (monitor == null) {
                monitor = provider.getMonitor();
            }
            if (protocols == null) {
                protocols = provider.getProtocols();
            }
        }
        //module 的配置会覆盖 provider的配置
        if (module != null) {
            if (registries == null) {
                registries = module.getRegistries();
            }
            if (monitor == null) {
                monitor = module.getMonitor();
            }
        }
        //应用级别配置 高于 组件级别
        if (application != null) {
            if (registries == null) {
                registries = application.getRegistries();
            }
            if (monitor == null) {
                monitor = application.getMonitor();
            }
        }
        //如果 提供的 服务对象 实现了 泛化接口 这个 服务实现对象一般是通过springBean 设置已经存在的类
        if (ref instanceof GenericService) {
            //将接口修改为泛化接口
            interfaceClass = GenericService.class;
            //设置 泛化标识为true
            if (StringUtils.isEmpty(generic)) {
                generic = Boolean.TRUE.toString();
            }
            //泛化 不用检查 methods 且InterfaceName 就不起作用了
        } else {
            try {

                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            //一般的接口类型 创建interface对象后 并检测 提供的方法是否能在该接口中找到
            checkInterfaceAndMethods(interfaceClass, methods);
            //检验 ref 是否实现该接口
            checkRef();
            //泛化标识设置为false  也就是在xml 中配置 该标识 意义不大
            generic = Boolean.FALSE.toString();
        }
        //代表是否 只进行本地暴露  这个功能 已经被弃用了 被 stub 替代
        if (local != null) {
            if ("true".equals(local)) {
                //修改接口名
                local = interfaceName + "Local";
            }
            Class<?> localClass;
            try {
                localClass = ClassHelper.forNameWithThreadContextClassLoader(local);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implementation class " + localClass.getName() + " not implement interface " + interfaceName);
            }
        }
        //存在 存根对象
        if (stub != null) {
            if ("true".equals(stub)) {
                //修改接口名
                stub = interfaceName + "Stub";
            }
            Class<?> stubClass;
            try {
                //返回对象 就是通过反射创建对象 数组 或原始类型 或一般对象
                stubClass = ClassHelper.forNameWithThreadContextClassLoader(stub);
                //对象不存在 或者不满足接口 都抛异常
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(stubClass)) {
                throw new IllegalStateException("The stub implementation class " + stubClass.getName() + " not implement interface " + interfaceName);
            }
        }
        //检查各个配置是否存在 不存在 就加载属性
        checkApplication();
        checkRegistry();
        checkProtocol();
        //为本config 再次 获取 属性
        appendProperties(this);
        //什么是 stub 以及 为什么需要一个 服务接口的构造函数
        checkStub(interfaceClass);
        checkMock(interfaceClass);
        //如果Service beanName 以interfaceName开头 path就会设置成 beanName 如果没有在这层就会设置成 接口名
        if (path == null || path.length() == 0) {
            path = interfaceName;
        }
        //上诉检查 和加载 对应 资源结束后 开始 执行出口逻辑
        doExportUrls();
        //生成一个 该服务提供者的 包装类
        ProviderModel providerModel = new ProviderModel(getUniqueServiceName(), ref, interfaceClass);
        //设置到全局容器中 代表 该应用下 发布的服务提供者
        ApplicationModel.initProviderModel(getUniqueServiceName(), providerModel);
    }

    private void checkRef() {
        // reference should not be null, and is the implementation of the given interface
        if (ref == null) {
            throw new IllegalStateException("ref not allow null!");
        }
        if (!interfaceClass.isInstance(ref)) {
            throw new IllegalStateException("The class "
                    + ref.getClass().getName() + " unimplemented interface "
                    + interfaceClass + "!");
        }
    }

    public synchronized void unexport() {
        if (!exported) {
            return;
        }
        if (unexported) {
            return;
        }
        if (!exporters.isEmpty()) {
            for (Exporter<?> exporter : exporters) {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn("unexpected err when unexport" + exporter, t);
                }
            }
            exporters.clear();
        }
        unexported = true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    /**
     * 出口服务  服务提供者暴露的对象都进行过动态代理  而 消费者获取到服务端的invoker对象都要 通过getProxy 获取代理对象
     */
    private void doExportUrls() {
        //加载注册中心的 信息 boolean 代表执行这个方法的是 服务提供者
        List<URL> registryURLs = loadRegistries(true);
        //遍历 协议 列表
        for (ProtocolConfig protocolConfig : protocols) {
            //根据 每种通信协议 对每个注册中心进行发布  注意如果 regisry 是 N/A 返回的是空列表
            //这里的 协议是指 dubbo injvm hessian 等  hessian 好像是 基于http请求 进行 调用的
            doExportUrlsFor1Protocol(protocolConfig, registryURLs);
        }
    }

    /**
     * 使用某个协议 对注册中心列表进行服务发布
     * @param protocolConfig
     * @param registryURLs
     */
    private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs) {
        //当协议名不存在时 默认使用dubbo 通信协议
        String name = protocolConfig.getName();
        if (name == null || name.length() == 0) {
            name = "dubbo";
        }

        Map<String, String> map = new HashMap<String, String>();
        //设置 版本 时间戳 提供者/消费者
        map.put(Constants.SIDE_KEY, Constants.PROVIDER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        //从中获取属性到 map中
        appendParameters(map, application);
        appendParameters(map, module);
        //为获取的 属性增加 default 前缀
        appendParameters(map, provider, Constants.DEFAULT_KEY);
        appendParameters(map, protocolConfig);
        appendParameters(map, this);
        //如果存在方法配置
        if (methods != null && !methods.isEmpty()) {
            for (MethodConfig method : methods) {
                //将其余配置抽取出来的共性 以方法级别 创建副本 并将 属性 增加该方法名作为前缀 代表该方法配置级别下的
                appendParameters(map, method, method.getName());
                //判断 该方法是否开启重试
                String retryKey = method.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    //如果 关闭重试
                    if ("false".equals(retryValue)) {
                        //将该方法级别的 重试次数 更正为0 这个重试次数 是从 service级别继承来的
                        map.put(method.getName() + ".retries", "0");
                    }
                }
                //获取 该 方法的 参数 配置信息
                List<ArgumentConfig> arguments = method.getArguments();
                if (arguments != null && !arguments.isEmpty()) {
                    //遍历参数配置
                    for (ArgumentConfig argument : arguments) {
                        // convert argument type
                        if (argument.getType() != null && argument.getType().length() > 0) {
                            //获取 接口类的 所有方法
                            Method[] methods = interfaceClass.getMethods();
                            // visit all methods
                            if (methods != null && methods.length > 0) {
                                for (int i = 0; i < methods.length; i++) {
                                    String methodName = methods[i].getName();
                                    // target the method, and get its signature
                                    // 接口方法对应到了 方法配置 对比需要的参数 是否能和 参数配置对上
                                    if (methodName.equals(method.getName())) {
                                        Class<?>[] argtypes = methods[i].getParameterTypes();
                                        // one callback in the method
                                        if (argument.getIndex() != -1) {
                                            if (argtypes[argument.getIndex()].getName().equals(argument.getType())) {
                                                //为map 增加 参数级别的配置
                                                appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                                            } else {
                                                throw new IllegalArgumentException("argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
                                            }
                                        } else {
                                            //参数 配置为-1的时候
                                            // multiple callbacks in the method
                                            for (int j = 0; j < argtypes.length; j++) {
                                                //遍历接口方法 的 每个 参数类型
                                                Class<?> argclazz = argtypes[j];
                                                //当 参数 类型 与 参数配置对上的时候
                                                if (argclazz.getName().equals(argument.getType())) {
                                                    appendParameters(map, argument, method.getName() + "." + j);
                                                    if (argument.getIndex() != -1 && argument.getIndex() != j) {
                                                        throw new IllegalArgumentException("argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            //没有规定类型 但是有下标时
                        } else if (argument.getIndex() != -1) {
                            appendParameters(map, argument, method.getName() + "." + argument.getIndex());
                        } else {
                            throw new IllegalArgumentException("argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
                        }

                    }
                }
            } // end of methods for
        }

        //判断是否指明了 泛化类型  这里应该在上面 被 覆盖成 true or false 了
        if (ProtocolUtils.isGeneric(generic)) {
            //为 true 时 会进入 设置 generic = true
            map.put(Constants.GENERIC_KEY, generic);
            //如果是 泛化实现 就将注册中心发布的方法 设置成*
            map.put(Constants.METHODS_KEY, Constants.ANY_VALUE);
        } else {
            //获取版本号的相关逻辑 暂时看不懂
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }

            //将接口生成包装类后 获取了 所有方法 这里 增加了3个 新的方法 一个是 setPropertyValue getPropertyValue invokeMethod
            //根据传入参数动态实现 这里怎么实现的 先不管 只知道 打算将返回的所有方法 都发布到了注册中心
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                //没有找到 方法为发布成* ???
                map.put(Constants.METHODS_KEY, Constants.ANY_VALUE);
            } else {
                //代表 要发布到注册中心的所有方法
                map.put(Constants.METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        //设置令牌 设置后 访问消费者就需要 提供对应的令牌
        if (!ConfigUtils.isEmpty(token)) {
            //是默认令牌的情况下
            if (ConfigUtils.isDefault(token)) {
                //生成随机令牌
                map.put(Constants.TOKEN_KEY, UUID.randomUUID().toString());
            } else {
                //否则 将 token内保存的值 直接 作为令牌
                map.put(Constants.TOKEN_KEY, token);
            }
        }
        //在发布的 时候 遍历每个 协议 并发布到所有注册中心 如果是 本地协议 也就是 不进行通信  injvm
        if (Constants.LOCAL_PROTOCOL.equals(protocolConfig.getName())) {
            // 就不注册到 注册中心了
            protocolConfig.setRegister(false);
            // 表示当监听到服务提供者发生变化时 不通知  现在设置是在哪里生效???
            map.put("notify", "false");
        }
        // export service
        // 获取 上下文对象路径
        String contextPath = protocolConfig.getContextpath();
        if ((contextPath == null || contextPath.length() == 0) && provider != null) {
            //没有就从 提供者上获取
            contextPath = provider.getContextpath();
        }

        //从协议配置中找到 本机的host 和 port  也就是说 对外发布的 ip port 由 协议配置决定
        String host = this.findConfigedHosts(protocolConfig, registryURLs, map);
        Integer port = this.findConfigedPorts(protocolConfig, name, map);

        //根据获取到的 创建URL 对象 这个URL 应该是本机的 这个 name 就是通信协议 从 Protocol 中获取 或者默认使用dubbo
        //获取到的 应该可以是 override 和 absent  有这么多种协议吗???
        //host 和 port 代表 对外暴露的 地址 以及暴露的 接口方法级别配置 方法参数级别配置
        URL url = new URL(name, host, port, (contextPath == null || contextPath.length() == 0 ? "" : contextPath + "/") + path, map);

        //如果存在以指定协议为key 的配置工厂 这个是实现动态配置的 可能传入的协议类型 就是 override
        if (ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                .hasExtension(url.getProtocol())) {
            //为 url 进行配置后返回 就是 补充 或 覆盖一些配置 这里应该是用不到的
            url = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class)
                    .getExtension(url.getProtocol()).getConfigurator(url).configure(url);
        }

        //获取scope 判断是 注册到远程还是本地 这个值是从其他配置中拿出来的
        String scope = url.getParameter(Constants.SCOPE_KEY);
        // don't export when none is configured
        //如果 是 none 代表不进行注册 scope 没有配置的情况 默认是 都进行暴露
        if (!Constants.SCOPE_NONE.equalsIgnoreCase(scope)) {

            // export to local if the config is not remote (export to remote only when config is remote)
            //只要不是 remote 就进行本地暴露
            if (!Constants.SCOPE_REMOTE.equalsIgnoreCase(scope)) {
                //本地暴露
                exportLocal(url);
            }
            // export to remote if the config is not local (export to local only when config is local)
            // 只要不是local 就进行远程暴露 也就是 如果没有设置 既不是 remote 也不是 local 就都会进行暴露
            if (!Constants.SCOPE_LOCAL.equalsIgnoreCase(scope)) {
                if (logger.isInfoEnabled()) {
                    logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
                }
                //遍历注册中心url 进行远程暴露
                if (registryURLs != null && !registryURLs.isEmpty()) {
                    //遍历注册中心的 URL 地址
                    for (URL registryURL : registryURLs) {
                        // 从注册中心中获取是否动态注册的信息
                        // "dynamic" ：服务是否动态注册，如果设为false，注册后将显示后disable状态，需人工启用，并且服务提供者停止时，也不会自动取消册，需人工禁用。
                        url = url.addParameterIfAbsent(Constants.DYNAMIC_KEY, registryURL.getParameter(Constants.DYNAMIC_KEY));
                        //获取监控中心 的url 没有获取到直接返回null 一般返回的都是同一个 监控中心配置上的信息
                        //如果获取不到 监控中心的 地址 且监控中心的协议是 registry类型 就需要从注册中心信息做转换
                        //当监控中心 协议为registry 就代表需要从 注册中心上获取地址
                        URL monitorUrl = loadMonitor(registryURL);
                        if (monitorUrl != null) {
                            //将监控中心的地址设置到 本地出口的url上  key: monitor value: 监控中心的 url
                            url = url.addParameterAndEncoded(Constants.MONITOR_KEY, monitorUrl.toFullString());
                        }
                        if (logger.isInfoEnabled()) {
                            logger.info("Register dubbo service " + interfaceClass.getName() + " url " + url + " to registry " + registryURL);
                        }

                        // 从本地url 中获取代理属性 这个功能还不知道怎么用
                        // For providers, this is used to enable custom proxy to generate invoker
                        String proxy = url.getParameter(Constants.PROXY_KEY);
                        if (StringUtils.isNotEmpty(proxy)) {
                            //如果存在 就 设置到 注册中心的 url中
                            registryURL = registryURL.addParameter(Constants.PROXY_KEY, proxy);
                        }

                        //这里为注册中心的 url 上又增加了 服务提供者的url (服务提供者上又携带了 监控中心的 url)
                        //getInvoker返回一个invoker 对象 调用invoke 方法时 使用动态类的 doInvoke
                        //这3个参数是 方便 javassist 进行动态代理的
                        Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, registryURL.addParameterAndEncoded(Constants.EXPORT_KEY, url.toFullString()));

                        //该对象封装了 invoker 和 serviceConfig  该对象还是 实现invoker接口
                        DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, this);

                        //这里的 protocol 是 SPI 机制拓展的 而不是 配置中的所有协议对象 这个对象应该会自适应成 RegistryProtocol 同时还设置了 过滤器和监听器
                        Exporter<?> exporter = protocol.export(wrapperInvoker);
                        //加入到 该服务提供者的出口对象中  每个 注册中心地址 对应一个 出口对象
                        exporters.add(exporter);
                    }
                } else {
                    //这样 服务提供者的url 中将不包含 监控中心 和 注册中心url 就是原始的提供者url
                    Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);
                    DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, this);

                    //这个协议是 根据 protocolConfig 中取出来的默认协议还是dubbo 代表不通过注册中心 直接发布服务 这个就是通过直连方式
                    //如果是 injvm 就直接是本地暴露了
                    Exporter<?> exporter = protocol.export(wrapperInvoker);
                    exporters.add(exporter);
                }
            }
        }
        //如果 scope 为 none 就会直接加入到这里
        this.urls.add(url);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    /**
     * 本地出口
     * 这个方法 是 服务提供者调用所有协议类型进行出口的
     * @param url 本机url【
     */
    private void exportLocal(URL url) {
        //当协议 不是 injvm 时会触发
        if (!Constants.LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
            //将本地 url 转换成字符串后 又转换成 对象 并重新设置了协议和 host port
            URL local = URL.valueOf(url.toFullString())
                    //又把协议设置成了 injvm
                    .setProtocol(Constants.LOCAL_PROTOCOL)
                    .setHost(LOCALHOST)
                    .setPort(0);

            //本地暴露使用的 是 injvmProtocol 同样会设置 filter链 和 listener 链  直接返回一个 InjvmExporter 对象

            //委托协议对象 进行出口 也就是 injvm 在本地还是要进行出口的
            //这里是基于 url 实现功能的 能够根据 协议和 方法名 动态实现 那么这里export 调用的应该是 injvmPorotocol
            Exporter<?> exporter = protocol.export(
                    //param1 服务提供者实现类 param2 接口类型 param3 本地url
                    proxyFactory.getInvoker(ref, (Class) interfaceClass, local));
            //获得 出口对象后 保存到容器中
            exporters.add(exporter);
            logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry");
        }
    }

    protected Class getServiceClass(T ref) {
        return ref.getClass();
    }

    /**
     * Register & bind IP address for service provider, can be configured separately.
     * Configuration priority: environment variables -> java system properties -> host property in config file ->
     * /etc/hosts -> default network address -> first available network address
     *
     * //就是获取指定的 host 根据不同优先级  系统变量 -> 配置文件->默认网卡端口 -> 第一个 连接返回的地址
     * @param protocolConfig
     * @param registryURLs
     * @param map
     * @return
     */
    private String findConfigedHosts(ProtocolConfig protocolConfig, List<URL> registryURLs, Map<String, String> map) {
        boolean anyhost = false;

        String hostToBind = getValueFromConfig(protocolConfig, Constants.DUBBO_IP_TO_BIND);
        if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
            throw new IllegalArgumentException("Specified invalid bind ip from property:" + Constants.DUBBO_IP_TO_BIND + ", value:" + hostToBind);
        }

        // if bind ip is not found in environment, keep looking up
        if (hostToBind == null || hostToBind.length() == 0) {
            hostToBind = protocolConfig.getHost();
            if (provider != null && (hostToBind == null || hostToBind.length() == 0)) {
                hostToBind = provider.getHost();
            }
            if (isInvalidLocalHost(hostToBind)) {
                anyhost = true;
                try {
                    hostToBind = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException e) {
                    logger.warn(e.getMessage(), e);
                }
                if (isInvalidLocalHost(hostToBind)) {
                    if (registryURLs != null && !registryURLs.isEmpty()) {
                        for (URL registryURL : registryURLs) {
                            if (Constants.MULTICAST.equalsIgnoreCase(registryURL.getParameter("registry"))) {
                                // skip multicast registry since we cannot connect to it via Socket
                                continue;
                            }
                            try {
                                Socket socket = new Socket();
                                try {
                                    SocketAddress addr = new InetSocketAddress(registryURL.getHost(), registryURL.getPort());
                                    socket.connect(addr, 1000);
                                    hostToBind = socket.getLocalAddress().getHostAddress();
                                    break;
                                } finally {
                                    try {
                                        socket.close();
                                    } catch (Throwable e) {
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn(e.getMessage(), e);
                            }
                        }
                    }
                    if (isInvalidLocalHost(hostToBind)) {
                        hostToBind = getLocalHost();
                    }
                }
            }
        }

        map.put(Constants.BIND_IP_KEY, hostToBind);

        // registry ip is not used for bind ip by default
        String hostToRegistry = getValueFromConfig(protocolConfig, Constants.DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry != null && hostToRegistry.length() > 0 && isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + Constants.DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        } else if (hostToRegistry == null || hostToRegistry.length() == 0) {
            // bind ip is used as registry ip by default
            hostToRegistry = hostToBind;
        }

        map.put(Constants.ANYHOST_KEY, String.valueOf(anyhost));

        return hostToRegistry;
    }

    /**
     * Register port and bind port for the provider, can be configured separately
     * Configuration priority: environment variable -> java system properties -> port property in protocol config file
     * -> protocol default port
     *
     * 获取默认端口 根据不同优先级
     * @param protocolConfig
     * @param name
     * @return
     */
    private Integer findConfigedPorts(ProtocolConfig protocolConfig, String name, Map<String, String> map) {
        Integer portToBind = null;

        // parse bind port from environment
        String port = getValueFromConfig(protocolConfig, Constants.DUBBO_PORT_TO_BIND);
        portToBind = parsePort(port);

        // if there's no bind port found from environment, keep looking up.
        if (portToBind == null) {
            //尝试 从protocolConfig 或者 providerConfigConfig 中获取端口号
            portToBind = protocolConfig.getPort();
            if (provider != null && (portToBind == null || portToBind == 0)) {
                portToBind = provider.getPort();
            }
            final int defaultPort = ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(name).getDefaultPort();
            if (portToBind == null || portToBind == 0) {
                portToBind = defaultPort;
            }
            if (portToBind == null || portToBind <= 0) {
                //这里返回的 默认就是 负数啊
                portToBind = getRandomPort(name);
                if (portToBind == null || portToBind < 0) {
                    //获取 可用的 端口 这里就是返回能 创建套接字的 第一个端口
                    portToBind = getAvailablePort(defaultPort);
                    putRandomPort(name, portToBind);
                }
                logger.warn("Use random available port(" + portToBind + ") for protocol " + name);
            }
        }

        // save bind port, used as url's key later
        // 记录已经使用的 所有 端口
        map.put(Constants.BIND_PORT_KEY, String.valueOf(portToBind));

        // registry port, not used as bind port by default
        //获取 绑定到 注册中心的 端口 一般 不会设置
        String portToRegistryStr = getValueFromConfig(protocolConfig, Constants.DUBBO_PORT_TO_REGISTRY);
        Integer portToRegistry = parsePort(portToRegistryStr);
        if (portToRegistry == null) {
            //将 发布到注册中心的 端口变成 绑定到本地的端口
            portToRegistry = portToBind;
        }

        return portToRegistry;
    }

    private Integer parsePort(String configPort) {
        Integer port = null;
        if (configPort != null && configPort.length() > 0) {
            try {
                Integer intPort = Integer.parseInt(configPort);
                if (isInvalidPort(intPort)) {
                    throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
                }
                port = intPort;
            } catch (Exception e) {
                throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
            }
        }
        return port;
    }

    private String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
        String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
        String port = ConfigUtils.getSystemProperty(protocolPrefix + key);
        if (port == null || port.length() == 0) {
            port = ConfigUtils.getSystemProperty(key);
        }
        return port;
    }

    /**
     * 检验配置信息
     */
    private void checkDefault() {
        if (provider == null) {
            //这里是2种情况
            // 1 是 xml 中没有配置 providerConfig
            // 2是没有指定provider 并且 providerConfig default = false
            provider = new ProviderConfig();
        }
        //刚创建的 providerConfig 是空的 需要获取属性
        appendProperties(provider);
    }

    /**
     * 检测 协议配置 信息
     */
    private void checkProtocol() {
        if ((protocols == null || protocols.isEmpty())
                && provider != null) {
            //从 provider 中 获取协议
            setProtocols(provider.getProtocols());
        }
        // backward compatibility
        if (protocols == null || protocols.isEmpty()) {
            setProtocol(new ProtocolConfig());
        }
        for (ProtocolConfig protocolConfig : protocols) {
            if (StringUtils.isEmpty(protocolConfig.getName())) {
                //name 默认使用dubbo
                protocolConfig.setName(Constants.DUBBO_VERSION_KEY);
            }
            appendProperties(protocolConfig);
        }
    }

    public Class<?> getInterfaceClass() {
        if (interfaceClass != null) {
            return interfaceClass;
        }
        if (ref instanceof GenericService) {
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
    public void setInterfaceClass(Class<?> interfaceClass) {
        setInterface(interfaceClass);
    }

    public String getInterface() {
        return interfaceName;
    }

    public void setInterface(String interfaceName) {
        this.interfaceName = interfaceName;
        if (id == null || id.length() == 0) {
            id = interfaceName;
        }
    }

    public void setInterface(Class<?> interfaceClass) {
        if (interfaceClass != null && !interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        this.interfaceClass = interfaceClass;
        setInterface(interfaceClass == null ? null : interfaceClass.getName());
    }

    public T getRef() {
        return ref;
    }

    public void setRef(T ref) {
        this.ref = ref;
    }

    @Parameter(excluded = true)
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        checkPathName(Constants.PATH_KEY, path);
        this.path = path;
    }

    public List<MethodConfig> getMethods() {
        return methods;
    }

    // ======== Deprecated ========

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (List<MethodConfig>) methods;
    }

    public ProviderConfig getProvider() {
        return provider;
    }

    public void setProvider(ProviderConfig provider) {
        this.provider = provider;
    }

    public String getGeneric() {
        return generic;
    }

    public void setGeneric(String generic) {
        if (StringUtils.isEmpty(generic)) {
            return;
        }
        if (ProtocolUtils.isGeneric(generic)) {
            this.generic = generic;
        } else {
            throw new IllegalArgumentException("Unsupported generic type " + generic);
        }
    }

    @Override
    public void setMock(Boolean mock) {
        throw new IllegalArgumentException("mock doesn't support on provider side");
    }

    @Override
    public void setMock(String mock) {
        throw new IllegalArgumentException("mock doesn't support on provider side");
    }

    public List<URL> getExportedUrls() {
        return urls;
    }

    /**
     * @deprecated Replace to getProtocols()
     */
    @Deprecated
    public List<ProviderConfig> getProviders() {
        return convertProtocolToProvider(protocols);
    }

    /**
     * @deprecated Replace to setProtocols()
     * 设置一组协议配置
     */
    @Deprecated
    public void setProviders(List<ProviderConfig> providers) {
        this.protocols = convertProviderToProtocol(providers);
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
