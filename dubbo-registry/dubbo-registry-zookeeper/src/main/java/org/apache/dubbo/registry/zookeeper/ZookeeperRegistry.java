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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ZookeeperRegistry  该注册中心采用的 是树形目录结构
 * 注册中心 继承于 支持失败重试的注册中心
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    /**
     * zookeeper 的默认端口
     */
    private final static int DEFAULT_ZOOKEEPER_PORT = 2181;

    /**
     * 默认的 根节点
     */
    private final static String DEFAULT_ROOT = "dubbo";

    /**
     * 设置的 根节点
     */
    private final String root;

    /**
     * 接口集合
     */
    private final Set<String> anyServices = new ConcurrentHashSet<String>();

    /**
     * 监听器集合
     */
    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<URL, ConcurrentMap<NotifyListener, ChildListener>>();

    /**
     * 组合 了 zookeeper 对象 来实现 注册中心的 核心功能
     */
    private final ZookeeperClient zkClient;

    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        //如果 没有设置端口 抛出 异常 注意 在 dubbo 中url不等于 ip:port url 是属性的 载体
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        //获取 资源组
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        //为获取的 group 属性 拼接"/"
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        //将 根节点定位成group  默认是 dubbo
        this.root = group;
        //通过 封装的 transporter 连接到指定url 返回 zookeeper 客户端 client 对象也是被dubbo 封装过的
        //通过 组合的 方式从传入的url 中获取属性 初始化对象
        zkClient = zookeeperTransporter.connect(url);
        //给 对象设置监听器
        //zookeeper 原生的 客户端对象会被 创建监听器对象 当触发时 通过映射 状态枚举 以及 触发 dubbo创建的 适配 监听器 实现 功能兼容
        zkClient.addStateListener(new StateListener() {
            @Override
            public void stateChanged(int state) {
                if (state == RECONNECTED) {
                    try {
                        //重连 触发对应事件
                        recover();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    /**
     * 追加默认端口
     * @param address
     * @return
     */
    static String appendDefaultPort(String address) {
        //将 地址拆分出 ip 和 端口
        if (address != null && address.length() > 0) {
            int i = address.indexOf(':');
            if (i < 0) {
                //如果 只有 ip 就补上 默认的 端口号
                return address + ":" + DEFAULT_ZOOKEEPER_PORT;
                //如果端口号 是0 也补上默认的端口号
            } else if (Integer.parseInt(address.substring(i + 1)) == 0) {
                return address.substring(0, i + 1) + DEFAULT_ZOOKEEPER_PORT;
            }
        }
        //否则 直接返回地址对象
        return address;
    }

    /**
     * 判断 zookeeper 是否还是 正常运行
     * @return
     */
    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    /**
     * 销毁 客户端
     */
    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 委托进行注册
     * @param url
     */
    @Override
    protected void doRegister(URL url) {
        try {
            //传入 是否为 动态数据 如果是 就是 临时的 否就是要做持久化 保存在 zookeeper上
            zkClient.create(toUrlPath(url), url.getParameter(Constants.DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 委托进行注销
     * @param url
     */
    @Override
    protected void doUnregister(URL url) {
        try {
            //删除指定节点
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 执行订阅
     * @param url
     * @param listener
     */
    @Override
    protected void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            //获取 订阅的 目标接口   这里代表每个 订阅的 url 都需要一个 当节点发生变化时 回调的监听器
            //当调用顶层的 订阅时 根据下面所有的 服务接口 又会 进行单独的 订阅
            if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                //获取根目录地址
                String root = toRootPath();
                //获取 该url 下的 所有监听器对象
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                //当 该url 没有找到 对应的 监听器时
                if (listeners == null) {
                    zkListeners.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ChildListener>());
                    listeners = zkListeners.get(url);
                }
                //尝试 通过指定的监听器 获取 childList
                ChildListener zkListener = listeners.get(listener);
                if (zkListener == null) {
                    //设置 该监听器 关联的 ChildListener  触发的时机在哪里
                    listeners.putIfAbsent(listener, new ChildListener() {
                        /**
                         * 当子节点发生变化时
                         * @param parentPath 父节点
                         * @param currentChilds 同级所有子节点 应该就是 接口名
                         */
                        @Override
                        public void childChanged(String parentPath, List<String> currentChilds) {
                            //遍历所有子节点
                            for (String child : currentChilds) {
                                child = URL.decode(child);
                                //发现不包含子节点 就 增加新的 可订阅接口
                                if (!anyServices.contains(child)) {
                                    anyServices.add(child);
                                    //触发订阅事件
                                    //1.在 顶层父类 维护 订阅者的信息中增加这个新的url
                                    //2.触发本级的 doSubscribe

                                    //这里是 发起一个新的订阅操作 因为返回的 url 是一个新对象 同一层所以使用同一个监听对象
                                    //这个相当于是 自动创建订阅 信息
                                    subscribe(url.setPath(child).addParameters(Constants.INTERFACE_KEY, child,
                                            Constants.CHECK_KEY, String.valueOf(false)), listener);
                                }
                            }
                        }
                    });
                    zkListener = listeners.get(listener);
                }
                //上面是不存在 监听器的 情况 然后 添加的 监听器 在 感知到 节点发生变化时 就触发订阅事件

                //这里创建新节点  如果节点存在应该是不用操作了  订阅* 就是获取root下所有节点
                zkClient.create(root, false);
                //为子节点设置 监听器 以根节点为父节点 当子节点发生变化时 触发对应的监听器 也就是 自动发起订阅动作
                //返回子节点层的全部元素  也就是接口层
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (services != null && !services.isEmpty()) {
                    for (String service : services) {
                        service = URL.decode(service);
                        //覆盖 和 补充 接口
                        anyServices.add(service);
                        //为接口层设置 监听对象  首次 从根节点到 服务级节点 都是使用一个 listener
                        subscribe(url.setPath(service).addParameters(Constants.INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                //应该是 创建到 服务层级别 就是调用 订阅 定位到type级别就是 notify

                //代表针对 某个接口进行订阅  上面是 url.getParameter(interface) 是 *
                //当 新增了 某个节点时 也会触发下面的订阅
                List<URL> urls = new ArrayList<URL>();
                //从 url 中 获取到 category 并从zookeeper 的 树形目录中定位到 TYPE 层  针对 每一TYPE
                //一共4层 ROOT -> Service 接口层 -> TYPE 订阅类型层 -> 具体的 路径
                for (String path : toCategoriesPath(url)) {
                    //获取 针对 该 url 的监听器
                    ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                    if (listeners == null) {
                        zkListeners.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ChildListener>());
                        listeners = zkListeners.get(url);
                    }
                    ChildListener zkListener = listeners.get(listener);
                    //如果监听器不存在 这个 监听器 的 逻辑跟上面的不同
                    if (zkListener == null) {
                        listeners.putIfAbsent(listener, new ChildListener() {
                            @Override
                            public void childChanged(String parentPath, List<String> currentChilds) {
                                //触发 注册中心的 notify
                                ZookeeperRegistry.this.notify(url, listener, toUrlsWithEmpty(url, parentPath, currentChilds));
                            }
                        });
                        zkListener = listeners.get(listener);
                    }
                    //这里 直接就定位到 TYPE 层
                    zkClient.create(path, false);
                    //返回 具体的 url 级别的 children
                    List<String> children = zkClient.addChildListener(path, zkListener);
                    if (children != null) {
                        //代表需要通知的  url 也就是寻找 与 url 匹配的 children
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }
                //为 传入的url 通知 订阅到的 urls  通知只到 TYPE 和 URL 级别 订阅只到 Service
                notify(url, listener, urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 注销逻辑
     * @param url
     * @param listener
     */
    @Override
    protected void doUnsubscribe(URL url, NotifyListener listener) {
        //获取 该url 下的 监听器
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                //如果 订阅的是全部的 接口 就从 根目录开始移除  移除掉的  是 该url 相关的 zk对象 每一个url 都在 zookeeper 有一个对应的
                //监听器  这个 childListener 在 回调方法中 使用了 关联的 url对象
                //移除 都是 更下一级的监听器
                if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    //移除 TYPE级别的监听器
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    //获取到 category 级别的 路径
                    for (String path : toCategoriesPath(url)) {
                        //移除URL 级别的 监听器
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }
        }
    }

    /**
     * 通过 url 查找他的 下一级 url
     * @param url
     * @return
     */
    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<String>();
            //获取 TYPE 级别的 路径 获取该路径的 下一级路径
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            //返回匹配的 路径
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 获取根目录 为 例如: dubbo/
     * @return
     */
    private String toRootDir() {
        if (root.equals(Constants.PATH_SEPARATOR)) {
            return root;
        }
        return root + Constants.PATH_SEPARATOR;
    }

    /**
     * 获取根目录地址 在初始化的时候root 就会设置成正确的值
     * @return
     */
    private String toRootPath() {
        return root;
    }

    /**
     * 将url 设置成 zookeeper的路径  例如: dubbo/interfaceName
     * @param url
     * @return
     */
    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        //如果是 * 就是根路径  如果一开始 初始化 传入了 group 那这个就是根节点
        if (Constants.ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        //返回 dubbo/接口名
        return toRootDir() + URL.encode(name);
    }

    /**
     * 从url 中 获取到zookeeper 的多个节点
     * @param url
     * @return
     */
    private String[] toCategoriesPath(URL url) {
        String[] categories;
        //获取 订阅的 种类 如果是 * 代表针对 某个 或全部服务接口的全部类型订阅
        if (Constants.ANY_VALUE.equals(url.getParameter(Constants.CATEGORY_KEY))) {
            //创建 能够接受的 订阅类型 提供者 消费者 路由信息 和 配置信息
            categories = new String[]{Constants.PROVIDERS_CATEGORY, Constants.CONSUMERS_CATEGORY,
                    Constants.ROUTERS_CATEGORY, Constants.CONFIGURATORS_CATEGORY};
        } else {
            //没有设置的 话 默认是 订阅服务
            categories = url.getParameter(Constants.CATEGORY_KEY, new String[]{Constants.DEFAULT_CATEGORY});
        }
        //将 服务地址 增加 category 组合成 新 的地址
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + Constants.PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    /**
     * 将 url 转换成 zookeeper 的路径   category 是 一个目录级 例如: dubbo/接口名/category
     * @param url
     * @return
     */
    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    /**
     * 将 url 转换成 zookeeper 的路径 例如:dubbo/接口名/category/url
     * @param url
     * @return
     */
    private String toUrlPath(URL url) {
        return toCategoryPath(url) + Constants.PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    /**
     *
     * @param consumer 消费者 的url
     * @param providers 服务提供者的 具体路径
     * @return
     */
    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<URL>();
        if (providers != null && !providers.isEmpty()) {
            //遍历每个 服务提供者
            for (String provider : providers) {
                //针对 提供者进行解码
                provider = URL.decode(provider);
                //如果包含 ://
                if (provider.contains(Constants.PROTOCOL_SEPARATOR)) {
                    //解析成 url 对象
                    URL url = URL.valueOf(provider);
                    //如果 消费者 和 提供者 匹配
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    /**
     *
     * @param consumer 传入的 订阅对象
     * @param path type级别路径
     * @param providers type 更低一级的路径
     * @return
     */
    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        //返回 符合该消费者 的全部提供者
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        //当 没有 符合条件的 提供者时
        if (urls == null || urls.isEmpty()) {
            int i = path.lastIndexOf(Constants.PATH_SEPARATOR);
            //获取 最后一级 也就是 type
            String category = i < 0 ? path : path.substring(i + 1);
            //将消费者 设置成 empty 协议 修改 category 并返回
            URL empty = consumer.setProtocol(Constants.EMPTY_PROTOCOL).addParameter(Constants.CATEGORY_KEY, category);
            urls.add(empty);
        }
        return urls;
    }

}
