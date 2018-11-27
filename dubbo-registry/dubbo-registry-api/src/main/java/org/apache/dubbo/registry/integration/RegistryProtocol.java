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
 * 注册中心 协议
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
     */
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();
    private Cluster cluster;

    /**
     * 协议对象
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
        registry.register(registedProviderUrl);
    }

    /**
     * 服务提供者向 注册中心进行出口
     * @param originInvoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        //export invoker
        //将 该invoker 对象 保存到本地容器中 保证不重复出口
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        //获取注册中心真正的 url 对象 (使用真正的 协议)
        URL registryUrl = getRegistryUrl(originInvoker);

        //registry provider
        //通过 invoker 对象 获取到 注册中心 (方法 内部实际获取了 注册中心的url 再去 注册中心工厂获取对象)
        final Registry registry = getRegistry(originInvoker);

        //获取服务提供者 url 从invoker 的url 上 通过 export 的 key 获取到 服务提供者的 url 并过滤掉一部分无用属性
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);

        //to judge to delay publish whether or not
        boolean register = registeredProviderUrl.getParameter("register", true);

        //向本地注册表注册服务提供者 也就是生成缓存
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);

        //如果 register为true
        if (register) {
            //向服务中心提供自己
            register(registryUrl, registeredProviderUrl);
            //标记 服务注册表 为 已注册
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }

        // Subscribe the override data
        // FIXME When the provider subscribes, it will affect the scene : a certain JVM exposes the service and call the same service. Because the subscribed is cached key with the name of the service, it causes the subscription information to cover.
        // 为服务提供者 url 设置特殊参数
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);
        // 使用该 url 创建 Override 监听器
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
     * 在本地 为待出口的服务做标识 防止重复 出口
     * @param originInvoker
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {
        //先通过 invoker 对象 获取 服务提供者的url 并在 去除部分 属性后 转换成了 域名格式返回
        String key = getCacheKey(originInvoker);
        //尝试 获取 如果能获取到就不进行重复出口了
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            //使用内置锁 来避免并发问题
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    //再次 确保 对象没有被创建
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));
                    //初始化 一个 出口服务的 包装对象
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
            //从属性容器中获取 registry 对应的真正的 协议对象  没有就返回默认的dubbo
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
        //getFilteredKeys 返回的 是 需要被过滤的 属性
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

        //获取到 出口者的 url
        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * Get the key cached in bounds by invoker
     *
     * 查看 当前出口者 是否有对应的 引用
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        //通过 invoker 对象 获取到 providerUrl 就是 解析invoker 的 url 的export 字段
        URL providerUrl = getProviderUrl(originInvoker);

        //在 移除了 指定的 属性标识(key) 后 将 返回的新url 转换成了string
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
        //获取 真正的 协议类型
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        //从注册中心工厂中获取 对应的 注册中心类
        Registry registry = registryFactory.getRegistry(url);
        //如果需要的 类型是 RegistryService 类型 从代理工厂中获取 invoker 对象
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        // group="a,b" or group="*"
        //将 refer 属性解析成 map
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        //获取 group 信息
        String group = qs.get(Constants.GROUP_KEY);
        if (group != null && group.length() > 0) {
            //如果 是 多组 或者 *
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                //传入可合并的 集群对象 这里对应到 cluster 集群中的 分组
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
        //在发往 注册中心的 refer 时 会创建 注册中心 目录对象
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        //设置 注册中心 和 协议对象
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        // all attributes of REFER_KEY
        //获取 目录对象的url 的属性
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());
        //创建 一个 消费者 url 对象 协议是 consumer 传入的 param 中 移除 注册中心的 属性
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);
        //当接口 信息不是 *  这个属性是什么时候设置的???
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface())
                //且 url 包含registry
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            //向注册中心 注册该订阅信息
            registry.register(subscribeUrl.addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY,
                    Constants.CHECK_KEY, String.valueOf(false)));
        }
        //往 目录中 设置发布的 订阅信息 这里 替换了 category 默认订阅3种  这里就是委托到Regoistry的 subscribe
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY,
                Constants.PROVIDERS_CATEGORY
                        + "," + Constants.CONFIGURATORS_CATEGORY
                        + "," + Constants.ROUTERS_CATEGORY));

        //使用这里获取到的 目录对象 创建invoker 对象 这里通过集群调用 返回了一个合适的invoker 对象
        Invoker invoker = cluster.join(directory);
        //给全局 表中 设置消费者
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

        private final URL subscribeUrl;
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
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl);
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            // No matching results
            if (matchedUrls.isEmpty()) {
                return;
            }

            List<Configurator> configurators = RegistryDirectory.toConfigurators(matchedUrls);

            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            //The origin invoker
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            //The current, may have been merged many times
            URL currentUrl = exporter.getInvoker().getUrl();
            //Merged with this configuration
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            if (!currentUrl.equals(newUrl)) {
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                // Compatible with the old version
                if (url.getParameter(Constants.CATEGORY_KEY) == null
                        && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }

                // Check whether url is to be applied to the current service
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        //Merge the urls of configurators
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
