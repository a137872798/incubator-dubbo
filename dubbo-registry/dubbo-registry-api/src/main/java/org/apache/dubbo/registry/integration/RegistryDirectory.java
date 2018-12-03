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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RegistryDirectory
 *
 * 注册中心目录对象  本身实现了 监听器对象
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    /**
     * 获得自适应的 集群对象
     */
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    /**
     * 自适应 路由工厂
     */
    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    /**
     * 配置工厂
     */
    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();
    /**
     * 服务键  包含了 interface group 和version
     */
    private final String serviceKey; // Initialization at construction time, assertion not null
    /**
     * 服务类型  应该就是 服务接口类
     */
    private final Class<T> serviceType; // Initialization at construction time, assertion not null

    /**
     * 消费者 的 属性 因为一个消费者 对应一个 能自动发现服务提供者的 directory
     */
    private final Map<String, String> queryMap; // Initialization at construction time, assertion not null
    private final URL directoryUrl; // Initialization at construction time, assertion not null, and always assign non null value
    /**
     * 这个方法数组 不知道是 所有 相关的 服务提供者提供的全部方法还是 只是 消费者需要的方法
     */
    private final String[] serviceMethods;
    /**
     * 是否包含多个组
     */
    private final boolean multiGroup;

    //下面这2个 应该都是 由用户传入自适应实现
    /**
     * 连接到注册中心的 协议对象
     */
    private Protocol protocol; // Initialization at the time of injection, the assertion is not null
    /**
     * 注册中心对象
     */
    private Registry registry; // Initialization at the time of injection, the assertion is not null
    /**
     * 是否禁止访问
     */
    private volatile boolean forbidden = false;

    /**
     * 去除 部分信息后 只保留消费者信息的url
     */
    private volatile URL overrideDirectoryUrl; // Initialization at construction time, assertion not null, and always assign non null value

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     *
     * 配置列表
     */
    private volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<url, Invoker> cache service url to invoker mapping.
    /**
     * 服务提供者 url 已经对应的 invoker 对象
     */
    private volatile Map<String, Invoker<T>> urlInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<methodName, Invoker> cache service method to invokers mapping.
    /**
     * 以针对 的 方法为单位 提供该方法的所有 服务提供者
     */
    private volatile Map<String, List<Invoker<T>>> methodInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Set<invokerUrls> cache invokeUrls to invokers mapping.
    /**
     * 服务提供者 集合缓存
     */
    private volatile Set<URL> cachedInvokerUrls; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        //该容器存在的 对应url 的 refer = ***
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        //去除 部分信息后 只保留消费者信息的url
        this.overrideDirectoryUrl = this.directoryUrl = url.setPath(url.getServiceInterface()).clearParameters().addParameters(queryMap).removeParameter(Constants.MONITOR_KEY);
        //获取组信息
        String group = directoryUrl.getParameter(Constants.GROUP_KEY, "");
        //代表是 多个组 这时在RegistryProtocl#doRefer(getMergeableCluster(), registry, type, url); 时代表使用的集群对象是 merger对象
        this.multiGroup = group != null && ("*".equals(group) || group.contains(","));
        //获取方法信息 这个方法信息的 设置 对应 referenceconfig
        /*
         *    if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", Constants.ANY_VALUE);
            } else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
         */
        //如果 是 泛化 模式下 这个值会为null
        String methods = queryMap.get(Constants.METHODS_KEY);
        //拆分 提供的方法
        this.serviceMethods = methods == null ? null : Constants.COMMA_SPLIT_PATTERN.split(methods);
    }

    /**
     * Convert override urls to map for use when re-refer.
     * Send all rules every time, the urls will be reassembled and calculated
     *
     * @param urls Contract:
     *             </br>1.override://0.0.0.0/...( or override://ip:port...?anyhost=true)&para1=value1... means global rules (all of the providers take effect)
     *             </br>2.override://ip:port...?anyhost=false Special rules (only for a certain provider)
     *             </br>3.override:// rule is not supported... ,needs to be calculated by registry itself.
     *             </br>4.override://0.0.0.0/ without parameters means clearing the override
     *             将url 对象转换为Configurator对象 核心是通过ConfiguratorFactory 获取对应的配置对象
     * @return
     */
    public static List<Configurator> toConfigurators(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Collections.emptyList();
        }

        List<Configurator> configurators = new ArrayList<Configurator>(urls.size());
        for (URL url : urls) {
            //发现空协议 退出配置
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }
            Map<String, String> override = new HashMap<String, String>(url.getParameters());
            //The anyhost parameter of override may be added automatically, it can't change the judgement of changing url
            override.remove(Constants.ANYHOST_KEY);
            //获得一个 属性为空的 configurate 就 清空之前传入的
            if (override.size() == 0) {
                configurators.clear();
                continue;
            }
            //通过url 获取 配置对象 这里 根据 url 的 protocol 返回自适应对象
            configurators.add(configuratorFactory.getConfigurator(url));
        }
        Collections.sort(configurators);
        return configurators;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 订阅  这里传入的 url protocol 是 consumer category 是 provider route configuration  以及check 为 false
     * @param url
     */
    public void subscribe(URL url) {
        //设置消费者url
        setConsumerUrl(url);
        //对注册中心发起订阅
        registry.subscribe(url, this);
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // must be executed after unsubscribing
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    /**
     * 当注册中心发生变化时 触发的 方法(应该就是 订阅的 3个 文件(provider, route, configuration) 发生变化)
     * @param urls 已注册信息列表，总不为空，含义同{@link com.alibaba.dubbo.registry.RegistryService#lookup(URL)}的返回值。
     */
    @Override
    public synchronized void notify(List<URL> urls) {
        List<URL> invokerUrls = new ArrayList<URL>();
        List<URL> routerUrls = new ArrayList<URL>();
        List<URL> configuratorUrls = new ArrayList<URL>();
        for (URL url : urls) {
            //获取每个url对象
            String protocol = url.getProtocol();
            String category = url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            //根据 协议类型 设置到不同的列表中
            //这里 route 的协议类型是 route
            if (Constants.ROUTERS_CATEGORY.equals(category)
                    || Constants.ROUTE_PROTOCOL.equals(protocol)) {
                routerUrls.add(url);
                //configuration 的 协议类型是 override
            } else if (Constants.CONFIGURATORS_CATEGORY.equals(category)
                    || Constants.OVERRIDE_PROTOCOL.equals(protocol)) {
                configuratorUrls.add(url);
            } else if (Constants.PROVIDERS_CATEGORY.equals(category)) {
                invokerUrls.add(url);
            } else {
                logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
            }
        }
        // configurators
        if (configuratorUrls != null && !configuratorUrls.isEmpty()) {
            //将读取到的 关于配置中心的url 转换成对应的配置实体类
            this.configurators = toConfigurators(configuratorUrls);
        }
        // routers
        // 处理获取到的 路由 url 转换为 路由对象并对现有的 全部invoker 进行过滤
        if (routerUrls != null && !routerUrls.isEmpty()) {
            List<Router> routers = toRouters(routerUrls);
            if (routers != null) { // null - do nothing
                //设置路由信息
                setRouters(routers);
            }
        }
        //如果存在配置对象 尝试进行配置 用更新后的配置 修改 原来的url 信息
        List<Configurator> localConfigurators = this.configurators; // local reference
        // merge override parameters
        //directoryUrl 是原始的 url  每次收到notify 后 都会更新配置信息 这时 将url 通过配置中心配置后 就生成了新的 overrideUrl
        this.overrideDirectoryUrl = directoryUrl;
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                //链式配置 指定url
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
        //更新服务提供者 比如当配置属性发生变化 就要更新之前的 invoker
        refreshInvoker(invokerUrls);
    }

    /**
     * Convert the invokerURL list to the Invoker Map. The rules of the conversion are as follows:
     * 1.If URL has been converted to invoker, it is no longer re-referenced and obtained directly from the cache, and notice that any parameter changes in the URL will be re-referenced.
     * 2.If the incoming invoker list is not empty, it means that it is the latest invoker list
     * 3.If the list of incoming invokerUrl is empty, It means that the rule is only a override rule or a route rule, which needs to be re-contrasted to decide whether to re-reference.
     *
     * 处理服务提供者url
     *
     * 1.如果 url 已经被转换为 invoker ，则不在重新引用，直接从缓存中获取，注意如果 url 中任何一个参数变更也会重新引用
     * 2.如果传入的 invoker 列表不为空，则表示最新的 invoker 列表
     * 3.如果传入的 invokerUrl 列表是空，则表示只是下发的 override 规则或 route 规则，需要重新交叉对比，决定是否需要重新引用。
     * @param invokerUrls this parameter can't be null
     */
    // TODO: 2017/8/31 FIXME The thread pool should be used to refresh the address, otherwise the task may be accumulated.
    //将 invokerUrl 替换成 invoker 列表
    private void refreshInvoker(List<URL> invokerUrls) {
        //只存在 一个 服务提供者 并且 还是 空协议的情况下 代表没有可用invoker
        if (invokerUrls != null && invokerUrls.size() == 1 && invokerUrls.get(0) != null
                && Constants.EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            //禁止调用 因为没有invoker 对象
            this.forbidden = true; // Forbid to access
            //方法对应的 能调用的invoker 对象 也设置为null
            this.methodInvokerMap = null; // Set the method invoker map to null
            //将 <url, Invoker>的键值对 中invoker 销毁 也就是设置成avliable = false
            destroyAllInvokers(); // Close all invokers
        } else {
            this.forbidden = false; // Allow to access
            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap; // local reference
            //如果 服务提供者 没有发生 更新 就使用上次缓存的 服务提供者
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                //延迟初始化 or 重置对象 将本次 最新的 url 列表保存
                this.cachedInvokerUrls = new HashSet<URL>();
                //这里保存的 是上次获取的 provider 的 全部url
                this.cachedInvokerUrls.addAll(invokerUrls);//Cached invoker urls, convenient for comparison
            }
            //如果本次 和上次 都没有 provider 就直接返回
            if (invokerUrls.isEmpty()) {
                return;
            }
            //将 url 转换成 invoker 对象 就是通过url 中的协议对象 去访问服务提供者 并返回 invoker对象 这里 只会返回实现了 serviceType的 invoker对象
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);// Translate url list to Invoker map
            //针对方法级别的 服务提供者 并只返回了该消费者 可以调用的invoker 对象 根据情况可能针对 invoker 做了路由处理 这里还不懂
            Map<String, List<Invoker<T>>> newMethodInvokerMap = toMethodInvokers(newUrlInvokerMap); // Change method name to map Invoker Map
            // state change
            // If the calculation is wrong, it is not processed.
            // 指定的url 没有获取到 invoker对象 直接返回
            if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls.toString()));
                return;
            }
            //如果是多组 就 将同组的 进行整合 否则 直接返回原对象
            this.methodInvokerMap = multiGroup ? toMergeMethodInvokerMap(newMethodInvokerMap) : newMethodInvokerMap;
            //设置缓存对象
            this.urlInvokerMap = newUrlInvokerMap;
            try {
                //销毁旧的invoker对象 这里应该是找出 不同的invoker 然后关闭
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap); // Close the unused Invoker
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    /**
     * 针对多组 情况 根据组来进行合并
     * @param methodMap 以方法为 单位的 invoker对象集合
     * @return
     */
    private Map<String, List<Invoker<T>>> toMergeMethodInvokerMap(Map<String, List<Invoker<T>>> methodMap) {
        Map<String, List<Invoker<T>>> result = new HashMap<String, List<Invoker<T>>>();
        for (Map.Entry<String, List<Invoker<T>>> entry : methodMap.entrySet()) {
            //获得方法名
            String method = entry.getKey();
            //获得 invoker对象列表
            List<Invoker<T>> invokers = entry.getValue();
            //key 为 group value 为该组的 invoker 对象
            Map<String, List<Invoker<T>>> groupMap = new HashMap<String, List<Invoker<T>>>();
            for (Invoker<T> invoker : invokers) {
                //遍历每个 invoker对象 获取group 信息
                String group = invoker.getUrl().getParameter(Constants.GROUP_KEY, "");
                //已组为单位 将 invoker 对象分组保存
                List<Invoker<T>> groupInvokers = groupMap.get(group);
                if (groupInvokers == null) {
                    groupInvokers = new ArrayList<Invoker<T>>();
                    groupMap.put(group, groupInvokers);
                }
                //这样每个方法下 就有 按组分配的 map了
                groupInvokers.add(invoker);
            }
            //如果只有一个组 直接将所有元素存入  每个方法 都以组为单位 进行筛选
            if (groupMap.size() == 1) {
                result.put(method, groupMap.values().iterator().next());
                //出现多组时
            } else if (groupMap.size() > 1) {
                List<Invoker<T>> groupInvokers = new ArrayList<Invoker<T>>();
                //这里是遍历每个 组 下面的 所有invoker
                for (List<Invoker<T>> groupList : groupMap.values()) {
                    //以组为单位 每个 invoker 对象都属于某个组下 在invoke 的时候 通过 均衡负载找到 该组下的某个invoker对象
                    //这里使用staticDirectory 因为不是通过订阅机制实现的
                    groupInvokers.add(cluster.join(new StaticDirectory<T>(groupList)));
                }
                //当出现这种情况时 代表外层的 cluster对象是MergeableCluster 然后确定组后 是使用directory内部的 cluster对象 进行真正负载
                result.put(method, groupInvokers);
            } else {
                //默认保持原样
                result.put(method, invokers);
            }
        }
        return result;
    }

    /**
     * 将url 转换成 route 对象
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private List<Router> toRouters(List<URL> urls) {
        List<Router> routers = new ArrayList<Router>();
        if (urls == null || urls.isEmpty()) {
            return routers;
        }
        if (urls != null && !urls.isEmpty()) {
            for (URL url : urls) {
                //空协议 就跳过
                if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                    continue;
                }
                String routerType = url.getParameter(Constants.ROUTER_KEY);
                if (routerType != null && routerType.length() > 0) {
                    //这个应该是路由信息真正的协议
                    url = url.setProtocol(routerType);
                }
                try {
                    //通过路由工厂获取路由对象 将通过url 的 protocol 获取 自适应对象
                    Router router = routerFactory.getRouter(url);
                    if (!routers.contains(router)) {
                        routers.add(router);
                    }
                } catch (Throwable t) {
                    logger.error("convert router url to router error, url: " + url, t);
                }
            }
        }
        return routers;
    }

    /**
     * Turn urls into invokers, and if url has been refer, will not re-reference.
     *
     * 将url 对象转换成 invoker对象
     * @param urls
     * @return invokers
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        //创建invoker 容器
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<String, Invoker<T>>();
        if (urls == null || urls.isEmpty()) {
            //返回空容器 这里是 针对 没有任何invoker 对象传来 且之前也没有 invoker 的情况
            return newUrlInvokerMap;
        }
        //创建存放key 的容器
        Set<String> keys = new HashSet<String>();
        //这个是 消费者的可以接受的 协议类型 是在 dubbo:reference protocol 上的 属性 跟注册中心无关 在 dubbo:registry protocol 中可能出现zookeeper
        //这里不会出现 这里是 获取 url 的  refer 属性 生成的 queryMap  对应  urls.add(u.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
        String queryProtocols = this.queryMap.get(Constants.PROTOCOL_KEY);
        for (URL providerUrl : urls) {
            // If protocol is configured at the reference side, only the matching protocol is selected
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                //查看协议类型  一般就是 dubbo
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    //协议 是否支持url 的协议
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                //协议不支持
                if (!accept) {
                    continue;
                }
            }
            //进入到 这里时 每个 url 都代表一个 服务提供者 当出现 empty 时 代表该 服务提供者 不可用
            if (Constants.EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
            }
            //无法根据 该协议在SPI 中找到实现类
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() + " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost()
                        + ", supported protocol: " + ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }
            //将消费者的 部分 配置 覆盖到提供者 并调用configuration 链 (也就是这里 触发了 最新的 configuration 更新动作) 进一步 修改 url 这里 更新了overrideDirectoryUrl
            URL url = mergeUrl(providerUrl);

            //将url 转换成 key
            String key = url.toFullString(); // The parameter urls are sorted
            //已存在就跳过
            if (keys.contains(key)) { // Repeated url
                continue;
            }
            keys.add(key);
            // Cache key is url that does not merge with consumer side parameters, regardless of how the consumer combines parameters, if the server url changes, then refer again
            // 尝试在缓存中获取
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
            //查看是否存在指定invoker 对象 不存在就创建 这里并没有将结果保存到 urlInvokerMap 中 是在 外层方法设置的
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
            if (invoker == null) { // Not in the cache, refer again
                try {
                    boolean enabled = true;
                    //查看 该invoker 能否使用
                    if (url.hasParameter(Constants.DISABLED_KEY)) {
                        enabled = !url.getParameter(Constants.DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(Constants.ENABLED_KEY, true);
                    }
                    if (enabled) {
                        //通过 协议的refer获取invoker对象  默认是 dubbo 也就是通过远程通信 获取 invoker对象
                        //这里传入了接口类型 应该能保证 只有实现该接口的服务提供者会返回 这里就是创建 client 的地方
                        invoker = new InvokerDelegate<T>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) { // Put new invoker in cache
                    //添加元素
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
                newUrlInvokerMap.put(key, invoker);
            }
        }
        //帮助gc
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * Merge url parameters. the order is: override > -D >Consumer > Provider
     *
     * 将url 融合
     * @param providerUrl
     * @return
     */
    private URL mergeUrl(URL providerUrl) {
        //将 providerUrl 的部分属性 设置成queryMap的
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters

        //获取配置属性
        List<Configurator> localConfigurators = this.configurators; // local reference
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                //链式追加 配置
                providerUrl = configurator.configure(providerUrl);
            }
        }

        //将 check 修改为 false  这个标识 到底有什么用???
        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // Do not check whether the connection is successful or not, always create Invoker!

        // The combination of directoryUrl and override is at the end of notify, which can't be handled here
        //将 merge 后的 属性 设置到 overrideDirectoryUrl 中
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters()); // Merge the provider side parameters

        if ((providerUrl.getPath() == null || providerUrl.getPath().length() == 0)
                && "dubbo".equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            //这里是 对 1.0版本的兼容 先不管
            String path = directoryUrl.getParameter(Constants.INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        //返回融合后的providerUrl
        return providerUrl;
    }

    /**
     * 进行路由
     * @param invokers
     * @param method
     * @return
     */
    private List<Invoker<T>> route(List<Invoker<T>> invokers, String method) {
        //创建 rpcinvocation 对象 也就是上下文对象 记录了 当前 执行的 是哪个方法
        Invocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
        //获取路由对象
        List<Router> routers = getRouters();
        if (routers != null) {
            for (Router router : routers) {
                // If router's url not null and is not route by runtime,we filter invokers here 代表非运行时  route 可以在现在进行过滤
                if (router.getUrl() != null && !router.getUrl().getParameter(Constants.RUNTIME_KEY, false)) {
                    //每个路由对象都对invokers对象做处理
                    invokers = router.route(invokers, getConsumerUrl(), invocation);
                }
            }
        }
        return invokers;
    }

    /**
     * Transform the invokers list into a mapping relationship with a method
     *
     * 将接口级别的 url.tofullString  与 invoker的关联关系 转变成 方法级别的
     * @param invokersMap Invoker Map
     * @return Mapping relation between Invoker and method
     */
    private Map<String, List<Invoker<T>>> toMethodInvokers(Map<String, Invoker<T>> invokersMap) {
        //创建容器对象
        Map<String, List<Invoker<T>>> newMethodInvokerMap = new HashMap<String, List<Invoker<T>>>();
        // According to the methods classification declared by the provider URL, the methods is compatible with the registry to execute the filtered methods
        List<Invoker<T>> invokersList = new ArrayList<Invoker<T>>();
        //遍历获取每个invoker对象
        if (invokersMap != null && invokersMap.size() > 0) {
            for (Invoker<T> invoker : invokersMap.values()) {
                //获取方法属性 并拆分 看看 能实现 几个方法
                String parameter = invoker.getUrl().getParameter(Constants.METHODS_KEY);
                if (parameter != null && parameter.length() > 0) {
                    String[] methods = Constants.COMMA_SPLIT_PATTERN.split(parameter);
                    if (methods != null && methods.length > 0) {
                        for (String method : methods) {
                            //非* 方法
                            if (method != null && method.length() > 0
                                    && !Constants.ANY_VALUE.equals(method)) {
                                //查找容器中是否已经存在该方法
                                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                                if (methodInvokers == null) {
                                    //将方法名 和  invoker 对象的关联关系保存
                                    methodInvokers = new ArrayList<Invoker<T>>();
                                    newMethodInvokerMap.put(method, methodInvokers);
                                }
                                methodInvokers.add(invoker);
                            }
                        }
                    }
                }
                invokersList.add(invoker);
            }
        }

        //上面的代码完成 了  从提供者级别 到 方法级别的转换

        //这里保存了 所有的invoker对象 方法传入null 执行了一个 route调用链
        List<Invoker<T>> newInvokersList = route(invokersList, null);
        //为 * 设置所有的invoker对象
        newMethodInvokerMap.put(Constants.ANY_VALUE, newInvokersList);
        //遍历服务方法 这里针对serviceMethods 中的 invoker 对象 都进行了 路由处理
        if (serviceMethods != null && serviceMethods.length > 0) {
            for (String method : serviceMethods) {
                //如果找到了对应的invoker列表对象
                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                if (methodInvokers == null || methodInvokers.isEmpty()) {
                    methodInvokers = newInvokersList;
                }
                //就将 invoker对象 经过route 处理后(也就是 过滤掉一部分 invoker) 再设置会 map中
                newMethodInvokerMap.put(method, route(methodInvokers, method));
            }
        }
        // sort and unmodifiable
        for (String method : new HashSet<String>(newMethodInvokerMap.keySet())) {
            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
            //将 结果排序后 重新保存 并且设置成 不可修改
            Collections.sort(methodInvokers, InvokerComparator.getComparator());
            newMethodInvokerMap.put(method, Collections.unmodifiableList(methodInvokers));
        }
        //将结果也设置成 不可修改 也就是 视图对象
        return Collections.unmodifiableMap(newMethodInvokerMap);
    }

    /**
     * Close all invokers
     * 销毁所有的invoker 对象 当收到全量数据时 发现invoker 不存在了 就需要调用这个方法
     */
    private void destroyAllInvokers() {
        //获取 url 与 invoker 的 关联容器
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                try {
                    //遍历销毁每个invoker
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        methodInvokerMap = null;
    }

    /**
     * Check whether the invoker in the cache needs to be destroyed
     * If set attribute of url: refer.autodestroy=false, the invokers will only increase without decreasing,there may be a refer leak
     *
     * @param oldUrlInvokerMap
     * @param newUrlInvokerMap
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            //销毁全部
            destroyAllInvokers();
            return;
        }
        // check deleted invoker 代表需要被销毁的容器
        List<String> deleted = null;
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    //将 不存在的 旧的销毁
                    if (deleted == null) {
                        deleted = new ArrayList<String>();
                    }
                    deleted.add(entry.getKey());
                }
            }
        }

        if (deleted != null) {
            for (String url : deleted) {
                if (url != null) {
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] faild. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 上层中获取 invoker 列表对象的 核心逻辑
     * @param invocation
     * @return
     */
    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        //如果已经被禁止 直接抛出异常 这个 禁止就是当 invoker 都不可用的时候设定
        if (forbidden) {
            // 1. No service provider 2. Service providers are disabled
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                    "No provider available from registry " + getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " + NetUtils.getLocalHost()
                            + " use dubbo version " + Version.getVersion() + ", please check status of providers(disabled, not registered or in blacklist).");
        }
        List<Invoker<T>> invokers = null;
        //获取针对方法级别的 invoker 对象容器
        Map<String, List<Invoker<T>>> localMethodInvokerMap = this.methodInvokerMap; // local reference
        if (localMethodInvokerMap != null && localMethodInvokerMap.size() > 0) {
            //获取 调用的方法
            String methodName = RpcUtils.getMethodName(invocation);
            //获取该方法的参数
            Object[] args = RpcUtils.getArguments(invocation);
            //根据 第一个参数进行路由 这是一种特殊的写法
            //比如：动态的方法名本身就是接口中已经定义的
            //
            //举个例子吧接口定义了 method, method1,method2，
            // 如果我发起rpc调用method(1, 2, 3), 这个时候会去查找方法method1的invokers，
            // 如果我这个时候发起rpc method(2, 1, 3), 这个时候会去查找方法method2的invokers，
            // 然后调用invokers的method方法
            if (args != null && args.length > 0 && args[0] != null
                    && (args[0] instanceof String || args[0].getClass().isEnum())) {
                //当方法存入的时候好像做了 route的处理
                invokers = localMethodInvokerMap.get(methodName + "." + args[0]); // The routing can be enumerated according to the first parameter
            }
            if (invokers == null) {
                //通过方法名直接匹配
                invokers = localMethodInvokerMap.get(methodName);
            }
            if (invokers == null) {
                //通过星号进行匹配 也就是默认使用全部invoker
                invokers = localMethodInvokerMap.get(Constants.ANY_VALUE);
            }
            //使用最后一个方法的  invoker 列表
            if (invokers == null) {
                Iterator<List<Invoker<T>>> iterator = localMethodInvokerMap.values().iterator();
                if (iterator.hasNext()) {
                    invokers = iterator.next();
                }
            }
        }
        //返回方法匹配到的列表
        return invokers == null ? new ArrayList<Invoker<T>>(0) : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, List<Invoker<T>>> getMethodInvokerMap() {
        return methodInvokerMap;
    }

    /**
     * 通过invoker 的 url 进行比较
     */
    private static class InvokerComparator implements Comparator<Invoker<?>> {

        private static final InvokerComparator comparator = new InvokerComparator();

        private InvokerComparator() {
        }

        public static InvokerComparator getComparator() {
            return comparator;
        }

        @Override
        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }

    }

    /**
     * The delegate class, which is mainly used to store the URL address sent by the registry,and can be reassembled on the basis of providerURL queryMap overrideMap for re-refer.
     *
     * @param <T>
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {
        private URL providerUrl;

        /**
         * url 是 merge 后的 providerUrl 是 原始的 提供者url
         * @param invoker
         * @param url
         * @param providerUrl
         */
        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }
}
