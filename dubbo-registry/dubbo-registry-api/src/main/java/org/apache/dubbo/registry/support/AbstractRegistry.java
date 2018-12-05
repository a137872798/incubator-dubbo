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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 *
 * 抽象注册中心
 */
public abstract class AbstractRegistry implements Registry {

    // URL address separator, used in file cache, service provider URL separation
    /**
     * URL 地址分隔符
     */
    private static final char URL_SEPARATOR = ' ';
    // URL address separated regular expression for parsing the service provider URL list in the file cache
    /**
     * 正则表达式
     */
    private static final String URL_SPLIT = "\\s+";
    // Log output
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    // Local disk cache, where the special key value.registies records the list of registry centers, and the others are the list of notified service providers
    /**
     * 注册中心 的 数据会暂时被保存在这个文件中
     */
    private final Properties properties = new Properties();
    // File cache timing writing
    /**
     * 后台用于 写文件的 线程池对象
     */
    private final ExecutorService registryCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));
    // Is it synchronized to save the file
    /**
     * 是否等待写入文件
     */
    private final boolean syncSaveFile;
    /**
     * 版本号
     */
    private final AtomicLong lastCacheChanged = new AtomicLong();
    /**
     * 注册中心上 保存的 所有url 包括 消费者 和 提供者
     */
    private final Set<URL> registered = new ConcurrentHashSet<URL>();
    /**
     * 针对 该url 的 监听器 集合 例如 当registryDirectory 订阅的时候 传入 监听器和 消费者url 这个url 记录了订阅类型
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();
    /**
     * key1 订阅者 url key2 订阅的种类 provider configurator route consumer  value key2对应的 所有url
     */
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<URL, Map<String, List<URL>>>();
    /**
     * 注册中心的地址
     */
    private URL registryUrl;
    /**
     * Local disk cache file
     * 本地文件
     */
    private File file;

    public AbstractRegistry(URL url) {
        //设置注册中心的 url
        setUrl(url);
        // Start file save timer  设置是否同步的 属性
        syncSaveFile = url.getParameter(Constants.REGISTRY_FILESAVE_SYNC_KEY, false);
        //获取文件名对象
        String filename = url.getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getParameter(Constants.APPLICATION_KEY) + "-" + url.getAddress() + ".cache");
        File file = null;
        //创建文件
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid registry store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
        }
        //设置文件对象
        this.file = file;
        //从文件中 将数据加载到 properties
        loadProperties();
        //通知 url 以及 backup 地址 backup 代表是 集群的备选地址  这里应该还没注册监听器
        notify(url.getBackupUrls());
    }

    /**
     * 判断urls 是否有效 无效就将url 设置成 空协议返回
     * @param url  符合 后面 urls 的 订阅者地址
     * @param urls 目标 url 列表
     * @return
     */
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        //如果 目标列表为空 就添加一个 协议为 empty的 url对象并 返回该列表
        if (urls == null || urls.isEmpty()) {
            List<URL> result = new ArrayList<URL>(1);
            result.add(url.setProtocol(Constants.EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    /**
     * 返回注册中心的url
     */
    @Override
    public URL getUrl() {
        return registryUrl;
    }

    /**
     * 设置 注册中心的 url
     * @param url
     */
    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public Set<URL> getRegistered() {
        return registered;
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return subscribed;
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return notified;
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }

    /**
     * 按照版本号 将数据保存
     * @param version
     */
    public void doSaveProperties(long version) {
        //如果 版本 已经过期就取消保存
        if (version < lastCacheChanged.get()) {
            return;
        }
        //目标文件不存在 就 返回
        if (file == null) {
            return;
        }
        // Save
        try {
            //创建 一个 锁文件
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {
                    //获取到锁才能 写入
                    FileLock lock = channel.tryLock();
                    if (lock == null) {
                        throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                    }
                    // Save
                    try {
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);
                        try {
                            properties.store(outputFile, "Dubbo Registry Cache");
                        } finally {
                            outputFile.close();
                        }
                    } finally {
                        lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                //可能是 上锁失败了就 用后台线程进行写入  并增加版本号
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry store file, cause: " + e.getMessage(), e);
        }
    }

    /**
     * 重新启动 注册中心时 从 文件中加载属性
     */
    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                //从文件中 读取属性
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * 获取会通知该url 的 所有 url (就是那4种类型的数据)
     * @param url
     * @return
     */
    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0 && key.equals(url.getServiceKey())
                    //key 必须 以某种 字符作为开头
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && value != null && value.length() > 0) {
                //去空又使用 \\s+ 来 拆分???
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<URL>();
                for (String u : arr) {
                    //将 url 转换成地址后保存  就当作是 普通的 读取value
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    /**
     * 获取 该url 订阅的4种类型 对应的 全部数据
     * @param url 查询条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @return
     */
    @Override
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<URL>();
        //key 是 订阅类型  list 是url集合
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        //存在 数据 立即返回
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            //这里 应该是 当数据发生变化时 再通知 返回 而不是立即返回
            final AtomicReference<List<URL>> reference = new AtomicReference<List<URL>>();
            NotifyListener listener = new NotifyListener() {
                //这里传入的 是全部的变化数据 既然进入这里代表数据为空  那也就是通知全部数据
                @Override
                public void notify(List<URL> urls) {
                    reference.set(urls);
                }
            };
            //为该消费者url 增加 这个监听器
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (urls != null && !urls.isEmpty()) {
                for (URL u : urls) {
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    /**
     * 注册  有很多 种 可以是 消费者 提供者 路由信息等
     * @param url 注册信息，不允许为空，如：dubbo://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     */
    @Override
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Register: " + url);
        }
        //给维护 url 的容器增加元素
        registered.add(url);
    }

    /**
     * 从容器中 移除 url
     * @param url 注册信息，不允许为空，如：dubbo://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     */
    @Override
    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unregister: " + url);
        }
        registered.remove(url);
    }

    /**
     * 为指定 url 添加 监听器 RegistryDirectory 也会被作为 监听器被添加
     * @param url      订阅条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }
        //获取 该 url 的 监听器 set 并添加 监听器
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners == null) {
            subscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
            listeners = subscribed.get(url);
        }
        listeners.add(listener);
    }

    /**
     * 取消该订阅者的 某个监听器 订阅者url 中 还包含了 订阅类型
     * @param url      订阅条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    /**
     * 恢复
     * @throws Exception
     */
    protected void recover() throws Exception {
        // register
        //获取 当前注册中心 登记的所有 消费者 提供者
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                //针对 每个 url 进行重新注册 就是重新加入容器 子类会重写这个方法 进行真正的注册
                register(url);
            }
        }
        // subscribe
        //重新订阅
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    //针对 url 下每个 监听者 进行 订阅
                    subscribe(url, listener);
                }
            }
        }
    }

    /**
     * 将新增的 url 通知到 指定url
     * @param urls
     */
    protected void notify(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return;
        }

        //获取了 订阅该地址的 其他地址
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();

            //因为 这批url 应该是针对同一个 url 进行通知的 所以第一个能对应就可以了
            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            //触发该订阅者携带的 每个 监听器对象
            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        //通知的 实际逻辑
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    /**
     * @param url 订阅者url
     * @param listener 通知订阅者需要触发的监听器
     * @param urls 新增的数据
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.isEmpty())
                && !Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }

        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        //遍历每个url 对象
        for (URL u : urls) {
            //匹配url 只有符合条件的才能 通知
            if (UrlUtils.isMatch(url, u)) {
                //获取 category 信息  照理说这里匹配上了 category 应该是相同的
                String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
                //根据 category 对 数据进行分类
                List<URL> categoryList = result.get(category);
                if (categoryList == null) {
                    categoryList = new ArrayList<URL>();
                    result.put(category, categoryList);
                }
                //针对协议种类进行保存
                categoryList.add(u);
            }
        }

        //到这里  result 已经按协议类型 存储了 url  该url 代表 待通知的url

        //代表没有 找到匹配的url 也就不用通知了
        if (result.size() == 0) {
            return;
        }
        //通过 url 定位到 会通知到 该url 的所有url 并且根据 category 分类
        Map<String, List<URL>> categoryNotified = notified.get(url);
        if (categoryNotified == null) {
            notified.putIfAbsent(url, new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = notified.get(url);
        }
        //遍历 匹配 该url 的所有 url信息 应该是要通知到 该url 上
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            //获取到的是全量数据  并替换掉原来的旧数据
            categoryNotified.put(category, categoryList);
            //保存 该 url 最新的 订阅者数据 里面也是用url 获取 notified 的数据
            saveProperties(url);
            //触发 目标 url 被 categoryList 通知后 的监听器对象  看来一次只会通知一种类型的数据
            listener.notify(categoryList);
        }
    }

    /**
     * 为给定url 增加属性
     * @param url
     */
    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();
            //获取 会通知 指定url 的 所有url 对象 并且以category分配好
            //这里每次写入的 都是 最新的 数据 并且是全部数据
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                //遍历 每个 category 下的 url
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        //全部写入到 buffer对象中
                        buf.append(u.toFullString());
                    }
                }
            }
            //以服务键作为 key 保存 订阅的 所有 url 信息
            properties.setProperty(url.getServiceKey(), buf.toString());
            //更新版本号
            long version = lastCacheChanged.incrementAndGet();
            //同步保存 就直接执行 否则 使用后台线程执行
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    /**
     * 销毁工作
     */
    @Override
    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        //获取 当前 的注册对象 进行注销
        Set<URL> destroyRegistered = new HashSet<URL>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<URL>(getRegistered())) {
                //如果是 dynamic 就进行注销 如果 dynamic 为false 应该是要在下线前将数据保存起来
                if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        //获取所有订阅对象 取消订阅
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

}
