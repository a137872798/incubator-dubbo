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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.support.ProviderConsumerRegTable;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.dubbo.common.Constants.ACCEPT_FOREIGN_IP;
import static org.apache.dubbo.common.Constants.INTERFACES;
import static org.apache.dubbo.common.Constants.QOS_ENABLE;
import static org.apache.dubbo.common.Constants.QOS_PORT;
import static org.apache.dubbo.common.Constants.VALIDATION_KEY;

/**
 * RegistryProtocol
 * 注册中心 级别协议  就是在这层完成了 订阅 和 注册
 */
public class RegistryProtocol implements Protocol {

    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);

    /**
     * 单例
     */
    private static RegistryProtocol INSTANCE;
    /**
     * key 是 url value 是 关联的 监听器对象
     */
    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();
    //To solve the problem of RMI repeated exposure port conflicts, the services that have been exposed are no longer exposed.
    //providerurl <--> exporter
    /**
     * 解决 端口重复 出口的问题
     * key export url 的字符串格式 value 对应的export对象 该对象是由服务提供者url 生成的
     */
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();


    /**
     * 这个集群对象是要自己创建的
     */
    private Cluster cluster;

    /**
     * 协议对象  注册中心要 实现 协议相关的功能都是通过委托该对象
     */
    private Protocol protocol;

    /**
     * 注册中心工厂
     */
    private RegistryFactory registryFactory;

    /**
     * 代理工厂
     */
    private ProxyFactory proxyFactory;

    public RegistryProtocol() {
        INSTANCE = this;
    }

    /**
     * 获取 注册协议对象
     * @return
     */
    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            //通过指定名 加载 对应的 拓展类  这里没有做 赋值操作是 怎么做到的???
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }

    /**
     * Filter the parameters that do not need to be output in url(Starting with .)
     *
     * 从 url 中拦截部分不需要输出的属性
     * @param url
     * @return
     */
    private static String[] getFilteredKeys(URL url) {
        //从 url中 获取 属性集合
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                //筛选出 以 "." 为开头的 属性 代表这些是需要被过滤的
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[]{};
        }
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return 9090;
    }

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    /**
     * 通过 服务中心地址 获取到 服务中心并 将该 服务提供者 注册上去
     * @param registryUrl 服务中心地址
     * @param registedProviderUrl 服务提供者地址
     */
    public void register(URL registryUrl, URL registedProviderUrl) {
        Registry registry = registryFactory.getRegistry(registryUrl);
        //这里就是 将 服务提供者 发布到注册中心的过程 然后消费者 会从 注册中心订阅相关全量数据 并筛选需要的 invoker
        registry.register(registedProviderUrl);
    }

    /**
     * 服务提供者向 注册中心进行出口时调用的方法 返回一个 export对象
     * @param originInvoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        //export invoker
        //启动服务提供者 监听指定端口
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        //获取注册中心真正的 url 对象 (使用真正的 协议) 也就是将registry:// 变成 zookeeper or redis or dubbo 等
        //注意 这里的 注册中心的 url 可以是为0.0.0.0
        URL registryUrl = getRegistryUrl(originInvoker);

        //registry provider
        //根据注册中心协议类型获取对应的实现  这里可选的 有 dubbo redis zookeeper 等 注意 dubbo 是有自己的注册中心实现的
        final Registry registry = getRegistry(originInvoker);

        //将服务提供者 url 去除无关属性
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);

        //to judge to delay publish whether or not  判断 该 服务提供者是否 具有注册的 功能
        boolean register = registeredProviderUrl.getParameter("register", true);

        //这里就是将 注册中心地址 提供者地址 invoker 对象封装成一个 包装对象并保存在 全局容器中 应该是方便随时获取 或者查看 是否被重复发布等
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);

        //如果需要被注册
        if (register) {
            //针对指定注册中心 注册服务提供者
            register(registryUrl, registeredProviderUrl);
            //标记 服务注册表 为 已注册
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }

        // Subscribe the override data
        // FIXME When the provider subscribes, it will affect the scene : a certain JVM exposes the service and call the same service. Because the subscribed is cached key with the name of the service, it causes the subscription information to cover.
        //服务提供者 订阅注册中心 这样在注册中心发生变化时 会 提供提供者重新发布
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);
        // 使用该 url 创建 Override 监听器 这个监听器是针对 配置中心发生变化的
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        //为监听器管理容器 增加一个监听器对象
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);
        //为该注册中心 设置 该监听器对象
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
        //Ensure that a new exporter instance is returned every time export
        //返回一个可销毁的 出口者
        return new DestroyableExporter<T>(exporter, originInvoker, overrideSubscribeUrl, registeredProviderUrl);
    }

    /**
     * 启动服务提供者 监听指定端口 处理服务消费者请求
     * @param originInvoker
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {
        //从 invoker 对象的 url 中筛选出 export 的 url 这个信息里包含服务提供者信息 用来开启 并监听消费者请求
        //这里 只从url 中提取出 ip:port username password
        String key = getCacheKey(originInvoker);
        //尝试 获取 如果能获取到就不进行重复出口了 从当前实现来看 每个 协议 对应一个默认端口 也即是一个协议 对应一个 服务提供者地址
        //一个 协议下的 dubbo:service只能存在一个
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            //使用内置锁 来避免并发问题
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    //通过服务提供者 url 和 原始的 invoker对象 创建 delegate 对象
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));
                    //初始化 一个 出口服务的 包装对象 这个protocol 应该也是 自适应对象 这里一般就会转发到 dubboProtocol
                    //如果该协议是 injvm 还是会使用 injvmProtocol 不过多了一层通过注册中心 发现
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);
                    //设置到绑定对象中
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    /**
     * Reexport the invoker of the modified url
     *
     * 更换 export 对象
     * @param originInvoker
     * @param newInvokerUrl
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
        } else {
            //创建该对象时 将新的url 保存  这样就会使用新的url
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * Get an instance of registry based on the address of invoker
     *
     * 通过invoker 对象的 信息 获取 到注册中心的 URL
     * @param originInvoker
     * @return
     */
    private Registry getRegistry(final Invoker<?> originInvoker) {
        //从invoker 对象中获取 url 信息
        URL registryUrl = getRegistryUrl(originInvoker);

        //通过 注册中心的 url 获取对应的 注册中心对象
        return registryFactory.getRegistry(registryUrl);
    }

    /**
     * 获得 注册中心的URL
     * @param originInvoker
     * @return
     */
    private URL getRegistryUrl(Invoker<?> originInvoker) {

        //------------------- 在获取注册中心地址的 时候 有这样一段代码
        //遍历每个 资源对象 设置注册中心协议  以及在 registry中设置原本的 协议
        //例如：registry：zookeeper
        //url = url.addParameter(Constants.REGISTRY_KEY, url.getProtocol());
        //将协议修改成registry
        //例如：zookeeper：//  ----> registry://
        //url = url.setProtocol(Constants.REGISTRY_PROTOCOL);


        //首先从invoker 对象中获取 注册中心的 url
        URL registryUrl = originInvoker.getUrl();
        //如果 协议类型 是  registry  协议 就是 registry:// --> dubbo://
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            //从属性容器中获取 registry 对应的真正的 协议对象  没有就返回默认的dubbo 如果指定了 就可能是 redis or zookeeper
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            //替换掉原来的registry 协议 同时 移除掉属性中的 registry
            //那么 当 协议是 registry 时 就是告诉调用者 需要从属性中获取 真正的协议类型
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        return registryUrl;
    }


    /**
     * Return the url that is registered to the registry and filter the url parameter once
     *
     * 获取 注册到该 注册中心 的 url对象 也就是 服务提供者的 url
     * @param originInvoker
     * @return
     */
    private URL getRegisteredProviderUrl(final Invoker<?> originInvoker) {
        //获取 服务提供者的 url  在invoker 的 url 中 通过以 export 为key 保存了 服务提供者的 url信息
        URL providerUrl = getProviderUrl(originInvoker);
        //The address you see at the registry
        //移除掉一些不必要的属性
        //getFilteredKeys 返回的 是 需要被过滤的 属性  这里移除了 监控中心的 url
        return providerUrl.removeParameters(getFilteredKeys(providerUrl))
                .removeParameter(Constants.MONITOR_KEY)
                .removeParameter(Constants.BIND_IP_KEY)
                .removeParameter(Constants.BIND_PORT_KEY)
                .removeParameter(QOS_ENABLE)
                .removeParameter(QOS_PORT)
                .removeParameter(ACCEPT_FOREIGN_IP)
                .removeParameter(VALIDATION_KEY)
                .removeParameter(INTERFACES);
    }

    /**
     * 通过 服务提供者 url 获取
     * @param registedProviderUrl
     * @return
     */
    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL)
                //传入的 参数 每 2个 为一对  category:configurator  check:false
                .addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY,
                        Constants.CHECK_KEY, String.valueOf(false));
    }

    /**
     * Get the address of the providerUrl through the url of the invoker
     *
     * 通过 invoker 的url 获取到 服务提供者的 url
     * @param origininvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> origininvoker) {
        //以 export 作为 key 获取 数据
        String export = origininvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }

        //获取到 出口者的 url 这里面 应该还带着监控中心的url
        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * Get the key cached in bounds by invoker
     *
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        //从invoker 的url 中获取export 关联的url 这个url 中包含服务提供者信息
        URL providerUrl = getProviderUrl(originInvoker);

        //这里移除了 dynamic 和enabled 因为这2个属性 与 开启服务提供者无关
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    /**
     * 传入注册中心的 url 获取 对应的 服务提供者返回的invoker
     * @param type Service class 需要被代理的 接口类
     * @param url  URL address for the remote service
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        //获取 真正的 协议类型 默认是 dubbo 可能是 zookeeper
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        //从注册中心工厂中获取 对应的 注册中心类
        Registry registry = registryFactory.getRegistry(url);
        //如果需要的 类型是 RegistryService 类型 应该代表 消费者需要的是注册中心对象吧
        if (RegistryService.class.equals(type)) {
            //这里是将 注册中心 包装成 invoker 返回了
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // group="a,b" or group="*"
        //将 refer 属性解析成 map  这个是在消费者端 发起订阅时将所有参数 编码生成的
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        //获取 group 信息
        String group = qs.get(Constants.GROUP_KEY);
        //如果是多组  这里的逻辑要配置 RegistryDirectory#toMergeMethodInvokerMap 在方法中 将按组筛选的invoker 对象 封装成单个invoker 这样在调用RegistryDirectory#list
        //时返回的就是从外部看过去普通的一组invoker对象(实际每个invoker 又分别都代表一个组)
        if (group != null && group.length() > 0) {
            //如果group信息是 多组 或者 *
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                //这里针对多组情况 使用了mergeableCluster 进行外层包装 在调用invoke时 再把内部以组为单位的 invoker 分别通过集群获取实际invoker对象 并把结果合并
                //前提是调用的方法 必须设置了 merge 属性 否则 就跟普通集群一样
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }
        return doRefer(cluster, registry, type, url);
    }

    /**
     * 获取 可合并的集群的拓展对象
     * @return
     */
    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    /**
     * 获取 invoker 对象的实际逻辑
     * @param cluster 集群对象
     * @param registry 注册中心
     * @param type 接口类型
     * @param url 注册中心url
     * @param <T>
     * @return
     */
    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        //创建 基于注册中心 动态发现服务提供者 的 directory对象
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        //设置 注册中心 和 协议对象
        directory.setRegistry(registry);
        //注册中心 本身的 protocol 职能实现都是 通过委托给 protocol 对象 这个一般就是用户创建自适应对象 这个protocol 就是将 提供者的url 变成提供者的 invoker 的
        directory.setProtocol(protocol);
        // all attributes of REFER_KEY
        //获取 目录对象的url 的属性 这个url 是消费者 url 缩减部分数据后的
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());
        //创建 一个 消费者 url 对象 协议是 consumer 传入的 param 中 端口 使用 绑定到注册中心的 端口 也就是消费者端 连接 注册中心使用的端口
        //设置成消费者 这样 就不会在订阅失败时抛出异常了
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);
        //当消费者订阅的 接口 不是 * 时 且消费者 register 为true 应该是 代表该url 可以注册到注册中心
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            //将消费者自身 注册到了注册中心
            //相比上面 增加了 category = consumer  check = false 代表订阅了 消费者的变化
            registry.register(subscribeUrl.addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY,
                    Constants.CHECK_KEY, String.valueOf(false)));
        }
        //使用 目录对象 发起订阅 种类是 服务提供者，路由信息，配置信息 内部会委托到registry
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY,
                Constants.PROVIDERS_CATEGORY
                        + "," + Constants.CONFIGURATORS_CATEGORY
                        + "," + Constants.ROUTERS_CATEGORY));

        //使用这里获取到的 目录对象 创建invoker 对象 这里返回的invoker 对象是一个集群对象 在调用invoke方法时 会在集群中自动使用各种容错手段以及均衡负载
        Invoker invoker = cluster.join(directory);
        //给全局 表中 设置消费者 和 服务提供者(invoker) 的关联关系
        ProviderConsumerRegTable.registerConsumer(invoker, url, subscribeUrl, directory);
        return invoker;
    }

    @Override
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    /**
     * invoker 代理对象
     * @param <T>
     */
    public static class InvokerDelegete<T> extends InvokerWrapper<T> {

        /**
         * 这个 invoker 和 父类的 invoker是一个对象
         */
        private final Invoker<T> invoker;

        /**
         * @param invoker
         * @param url     invoker.getUrl return this value
         */
        public InvokerDelegete(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        public Invoker<T> getInvoker() {
            //如果 是delegate 就 返回 .invoker 应该是 这个对象不能直接获取url 所以要获取上层对象
            if (invoker instanceof InvokerDelegete) {
                return ((InvokerDelegete<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    /**
     * Reexport: the exporter destroy problem in protocol
     * 1.Ensure that the exporter returned by registryprotocol can be normal destroyed
     * 2.No need to re-register to the registry after notify
     * 3.The invoker passed by the export method , would better to be the invoker of exporter
     */
    private class OverrideListener implements NotifyListener {

        /**
         * 订阅的 url 对象
         */
        private final URL subscribeUrl;
        /**
         * 原始 invoker 对象
         */
        private final Invoker originInvoker;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        /**
         * 发生变化时 通知 给定的url 列表
         * @param urls The list of registered information , is always not empty, The meaning is the same as the return value of {@link org.apache.dubbo.registry.RegistryService#lookup(URL)}.
         */
        @Override
        public synchronized void notify(List<URL> urls) {
            logger.debug("original override urls: " + urls);
            //将 全量url 进行筛选 只返回 订阅者url 需要的url
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            // No matching results  如果匹配的url 为空 直接返回
            if (matchedUrls.isEmpty()) {
                return;
            }

            //将匹配上的url 转换成 配置列表
            List<Configurator> configurators = RegistryDirectory.toConfigurators(matchedUrls);

            final Invoker<?> invoker;
            //取出原始的 invoker 对象
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            //The origin invoker 将invoker 对象url 中的export url 转换成url对象返回
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            //将export url 去除 dynamic 和 enable 后又变成了字符串格式
            String key = getCacheKey(originInvoker);
            //获取绑定的 export对象
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            //The current, may have been merged many times
            //获取暴露者对象的 url 信息
            URL currentUrl = exporter.getInvoker().getUrl();
            //Merged with this configuration
            //将url 进行配置后返回
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            if (!currentUrl.equals(newUrl)) {
                //更换暴露的url 对象
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        /**
         * 匹配需要通知的 给 目标url 的 url数据
         * @param configuratorUrls 触发 该 notify 的是 配置中心的全量数据
         * @param currentSubscribe
         * @return
         */
        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            //遍历配置中心的 最新url
            for (URL url : configuratorUrls) {
                //获取 最新的 url对象
                URL overrideUrl = url;
                // Compatible with the old version
                //如果没有设置 catagory 就补充
                if (url.getParameter(Constants.CATEGORY_KEY) == null
                        && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                //判断2个url 是否匹配
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        //Merge the urls of configurators
        //将url 通过 配置对象配置后 返回
        private URL getConfigedInvokerUrl(List<Configurator> configurators, URL url) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
            return url;
        }
    }

    /**
     * exporter proxy, establish the corresponding relationship between the returned exporter and the exporter exported by the protocol, and can modify the relationship at the time of override.
     *
     * 利用装饰器模式 多封装了一层 在unexport 时 从bounds 中 移除关联关系
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private final Invoker<T> originInvoker;
        private Exporter<T> exporter;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }

    /**
     * 装饰器 对象
     * @param <T>
     */
    static private class DestroyableExporter<T> implements Exporter<T> {

        /**
         * 单线程池 执行销毁任务
         */
        public static final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        /**
         * 装饰的 出口者对象
         */
        private Exporter<T> exporter;
        /**
         * 出口者组合的invoker对象
         */
        private Invoker<T> originInvoker;

        /**
         * 这个参数 好像是 加工 提供者 url 得到的 不知道什么意思
         */
        private URL subscribeUrl;
        /**
         * 这个参数应该是服务提供者的url
         */
        private URL registerUrl;

        public DestroyableExporter(Exporter<T> exporter, Invoker<T> originInvoker, URL subscribeUrl, URL registerUrl) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
            this.subscribeUrl = subscribeUrl;
            this.registerUrl = registerUrl;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            //获取注册中心对象
            Registry registry = RegistryProtocol.INSTANCE.getRegistry(originInvoker);
            try {
                //注销服务提供者地址
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            try {
                //从 维护 url 和监听器的 容器中移除对应元素
                NotifyListener listener = RegistryProtocol.INSTANCE.overrideListeners.remove(subscribeUrl);
                //注销监听器
                registry.unsubscribe(subscribeUrl, listener);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }

            //将费时操作委托到线程池中
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int timeout = ConfigUtils.getServerShutdownTimeout();
                        if (timeout > 0) {
                            logger.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. Usually, this is called when you use dubbo API");
                            Thread.sleep(timeout);
                        }
                        //解除出口
                        exporter.unexport();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            });
        }
    }
}
