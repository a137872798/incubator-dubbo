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
package org.apache.dubbo.common.extension;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassHelper;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * Load dubbo extensions
 * <ul>
 * <li>auto inject dependency extension </li>
 * <li>auto wrap extension in wrapper </li>
 * <li>default extension is an adaptive instance</li>
 * </ul>
 *
 * dubbo 用来拓展spi 的 核心类  一个拓展对象 对应一个 Loader 对象
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">Service Provider in Java 5</a>
 * @see org.apache.dubbo.common.extension.SPI
 * @see org.apache.dubbo.common.extension.Adaptive
 * @see org.apache.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);

    /**
     * 读取 spi 的路径
     */
    private static final String SERVICES_DIRECTORY = "META-INF/services/";

    /**
     * 放置配置文件的路径
     */
    private static final String DUBBO_DIRECTORY = "META-INF/dubbo/";

    /**
     * 放置配置文件的路径
     */
    private static final String DUBBO_INTERNAL_DIRECTORY = DUBBO_DIRECTORY + "internal/";

    /**
     * 拓展名的正则
     */
    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");

    /**
     * 拓展加载器的 集合
     * key: 拓展接口
     */
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<Class<?>, ExtensionLoader<?>>();

    /**
     * 拓展实现类集合
     * key: 拓展实现类
     * value: 拓展对象
     * 例如，key 为 Class<AccessLogFilter>
     * value 为 AccessLogFilter 对象
     */
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<Class<?>, Object>();

    // ==============================

    /**
     * 拓展接口
     */
    private final Class<?> type;

    /**
     * 对象工厂
     *
     * 用于调用 {@link #injectExtension(Object)} 方法，向拓展对象注入依赖属性。
     *
     * 例如，StubProxyFactoryWrapper 中有 `Protocol protocol` 属性。
     */
    private final ExtensionFactory objectFactory;

    /**
     * 拓展名和拓展类的映射
     */
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<Class<?>, String>();

    /**
     * 缓存的拓展实现类集合
     *
     * 不包含如下两种类型：
     * 1. 自适应拓展实现类。例如 AdaptiveExtensionFactory
     * 2. 带唯一参数为拓展接口的构造方法的实现类，或者说拓展 Wrapper 实现类。例如，ProtocolFilterWrapper 。
     *    拓展 Wrapper 实现类，会添加到 {@link #cachedWrapperClasses} 中
     */
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<Map<String, Class<?>>>();

    /**
     * 拓展名与 @Activate 的映射
     */
    private final Map<String, Object> cachedActivates = new ConcurrentHashMap<String, Object>();

    /**
     * 缓存的拓展对象集合
     */
    private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<String, Holder<Object>>();

    /**
     * 缓存的自适应(Adapter)拓展对象
     */
    private final Holder<Object> cachedAdaptiveInstance = new Holder<Object>();

    /**
     * 缓存的自适应拓展对象
     */
    private volatile Class<?> cachedAdaptiveClass = null;

    /**
     * 缓存的默认拓展名
     */
    private String cachedDefaultName;

    /**
     * 缓存的 当发生 createAdaptiveInstance 时使用的异常对象
     */
    private volatile Throwable createAdaptiveInstanceError;

    /**
     * 拓展的 Wrapper 实现集合 貌似是 链式调用
     */
    private Set<Class<?>> cachedWrapperClasses;

    /**
     * 拓展名 与 加载对应拓展类发生的 异常 映射
     */
    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<String, IllegalStateException>();

    /**
     *
     * @param type 被拓展的接口
     */
    private ExtensionLoader(Class<?> type) {
        this.type = type;
        //如果 拓展接口本身就是 拓展工厂就不设置 否则先获取拓展工厂的  SPI对象 信息
        objectFactory = (type == ExtensionFactory.class ? null : ExtensionLoader.getExtensionLoader(ExtensionFactory.class).getAdaptiveExtension());
    }

    /**
     * 是否 含有SPI 注解
     * @param type
     * @param <T>
     * @return
     */
    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    /**
     * 通过 class 获取 对应的拓展类 这个class 应该就是接口类
     * 每次调用顺序就是先通过拓展类 接口去 获取对应的 拓展类加载器实例对象然后加载 对应的文件 装载里面的 实现类
     * @param type
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        //传入的必须是接口
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not interface!");
        }
        //必须含有SPI的 接口 也就是说在 dubbo 中能够使用SPI的接口必须用SPI 注解标明
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type(" + type +
                    ") is not extension, because WITHOUT @" + SPI.class.getSimpleName() + " Annotation!");
        }

        //这个对象是静态对象也就是所有类 应该都是共享这个 容器的
        ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        if (loader == null) {
            //延迟设置  key 就是拓展类接口
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type));
            loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        }
        return loader;
    }

    /**
     * 获取类加载器
     * @return
     */
    private static ClassLoader findClassLoader() {
        return ClassHelper.getClassLoader(ExtensionLoader.class);
    }

    /**
     * 通过 拓展类对象获取对应的name
     * @param extensionInstance
     * @return
     */
    public String getExtensionName(T extensionInstance) {
        return getExtensionName(extensionInstance.getClass());
    }

    /**
     * 通过 拓展类对象获取对应的name
     * @param extensionClass
     * @return
     */
    public String getExtensionName(Class<?> extensionClass) {
        //先尝试去加载 当加载完成后 会在对应的容器中设置 相关拓展类信息
        getExtensionClasses();// load class
        //返回结果
        return cachedNames.get(extensionClass);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, key, null)}
     *
     * 获得 符合条件的 拓展对象数组
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, values, null)}
     *
     * @param url    url
     * @param values extension point names
     * @return extension list which are activated
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, url.getParameter(key).split(","), null)}
     *
     * 获得 符合条件的 拓展对象数组
     * @param url   url
     * @param key   url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
        //从url 中通过key 获取 value
        String value = url.getParameter(key);
        //将 value 通过 "," 拆分后 继续调用 返回一个List 对象
        return getActivateExtension(url, value == null || value.length() == 0 ? null : Constants.COMMA_SPLIT_PATTERN.split(value), group);
    }

    /**
     * Get activate extensions.
     *
     * 返回符合条件的List
     * @param url    url
     * @param values extension point names
     * @param group  group
     * @return extension list which are activated
     * @see org.apache.dubbo.common.extension.Activate
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        List<T> exts = new ArrayList<T>();
        //将values 数组转换成一个 List
        List<String> names = values == null ? new ArrayList<String>(0) : Arrays.asList(values);
        //如果 不包含 -default    -default 代表 移除所有默认配置
        if (!names.contains(Constants.REMOVE_VALUE_PREFIX + Constants.DEFAULT_KEY)) {
            //这里代表没有移除 任何 默认配置
            //加载
            getExtensionClasses();
            //循环 这个容器中存放了 每个拓展名和Activate注解对象的映射关系
            for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
                //获取 拓展名 和 Activate 注解对象
                String name = entry.getKey();
                Object activate = entry.getValue();

                String[] activateGroup, activateValue;

                if (activate instanceof Activate) {
                    //获取 自适应组 和 自适应值
                    activateGroup = ((Activate) activate).group();
                    activateValue = ((Activate) activate).value();
                    //兼容老版本
                } else if (activate instanceof com.alibaba.dubbo.common.extension.Activate) {
                    activateGroup = ((com.alibaba.dubbo.common.extension.Activate) activate).group();
                    activateValue = ((com.alibaba.dubbo.common.extension.Activate) activate).value();
                } else {
                    continue;
                }
                //判断 在 group[]中能否找到 group 如果group 为 null 就直接返回true
                if (isMatchGroup(group, activateGroup)) {
                    //根据拓展名 获取 拓展对象
                    T ext = getExtension(name);
                    //第一个 是代表不包含在自定义配置中  第二个代表 配置是否移除 第三个判断是否激活
                    if (!names.contains(name)
                            && !names.contains(Constants.REMOVE_VALUE_PREFIX + name)
                            && isActive(activateValue, url)) {
                        //加入到符合条件的 拓展对象list中
                        exts.add(ext);
                    }
                }
            }
            //使用指定的排序类 进行排序
            Collections.sort(exts, ActivateComparator.COMPARATOR);
        }
        List<T> usrs = new ArrayList<T>();
        for (int i = 0; i < names.size(); i++) {
            //遍历 获取每个 key
            String name = names.get(i);
            //未被 移除
            if (!name.startsWith(Constants.REMOVE_VALUE_PREFIX)
                    && !names.contains(Constants.REMOVE_VALUE_PREFIX + name)) {
                //如果 name 是 default
                if (Constants.DEFAULT_KEY.equals(name)) {
                    //如果 usrs 不为空 就将 元素 全部转义到 exts 并清空usrs
                    if (!usrs.isEmpty()) {
                        exts.addAll(0, usrs);
                        usrs.clear();
                    }
                } else {
                    //其余情况 获取拓展类对象并加入到 usrs
                    T ext = getExtension(name);
                    usrs.add(ext);
                }
            }
        }
        //将usrs 全部设置到 exts中
        if (!usrs.isEmpty()) {
            exts.addAll(usrs);
        }
        return exts;
    }

    /**
     * 校验是否在group中
     * @param group
     * @param groups
     * @return
     */
    private boolean isMatchGroup(String group, String[] groups) {
        if (group == null || group.length() == 0) {
            return true;
        }
        if (groups != null && groups.length > 0) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断是否激活
     * @param keys Activate.value 返回的 数组
     * @param url
     * @return
     */
    private boolean isActive(String[] keys, URL url) {
        //没有指定激活参数 就是无条件激活
        if (keys.length == 0) {
            return true;
        }
        for (String key : keys) {
            for (Map.Entry<String, String> entry : url.getParameters().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                //激活条件的 变量存在且有效时
                if ((k.equals(key) || k.endsWith("." + key))
                        && ConfigUtils.isNotEmpty(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get extension's instance. Return <code>null</code> if extension is not found or is not initialized. Pls. note
     * that this method will not trigger extension load.
     * <p>
     * In order to trigger extension load, call {@link #getExtension(String)} instead.
     *
     * 通过拓展名 从 容器中获取目标实例
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<Object>());
            holder = cachedInstances.get(name);
        }
        return (T) holder.get();
    }

    /**
     * Return the list of extensions which are already loaded.
     * <p>
     * Usually {@link #getSupportedExtensions()} should be called in order to get all extensions.
     *
     * 获取容器视图
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<String>(cachedInstances.keySet()));
    }

    /**
     * Find the extension with the given name. If the specified name is not found, then {@link IllegalStateException}
     * will be thrown.
     *
     * 根据拓展名 获取对应的拓展对象
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        //拓展名不存在返回错误信息
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Extension name == null");
        }
        //如果拓展名是 true 就返回默认的拓展对象
        if ("true".equals(name)) {
            return getDefaultExtension();
        }
        //从缓存实例容器中获取对象  没有就创建一个新的
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<Object>());
            holder = cachedInstances.get(name);
        }
        //从hold 对象中获取 实例
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    //获取不到就创建并保存到容器中
                    instance = createExtension(name);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * Return default extension, return <code>null</code> if it's not configured.
     * 获取默认的拓展对象 也就是SPI("***") 以 *** 作为拓展名
     */
    public T getDefaultExtension() {
        getExtensionClasses();
        if (null == cachedDefaultName || cachedDefaultName.length() == 0
                || "true".equals(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName);
    }

    /**
     * 是否存在某拓展名
     * @param name
     * @return
     */
    public boolean hasExtension(String name) {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("Extension name == null");
        }
        try {
            this.getExtensionClass(name);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * 返回 拓展缓存的 视图对象
     * @return
     */
    public Set<String> getSupportedExtensions() {
        Map<String, Class<?>> clazzes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<String>(clazzes.keySet()));
    }

    /**
     * Return default extension name, return <code>null</code> if not configured.
     * 返回 默认缓存名 如果没有 会先加载数据
     */
    public String getDefaultExtensionName() {
        getExtensionClasses();
        return cachedDefaultName;
    }

    /**
     * Register new extension via API
     *
     * 增加 指定 的 拓展类
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension with the same name has already been registered.
     */
    public void addExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " already existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
        } else {
            if (cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
        }
    }

    /**
     * Replace the existing extension via API
     *
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension to be placed doesn't exist
     * @deprecated not recommended any longer, and use only when test
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " not existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        } else {
            if (cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension not existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
        }
    }

    /**
     * 获取自适应 拓展类
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getAdaptiveExtension() {
        //从拓展类实例容器中 获取对象
        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            if (createAdaptiveInstanceError == null) {
                synchronized (cachedAdaptiveInstance) {
                    instance = cachedAdaptiveInstance.get();
                    if (instance == null) {
                        try {
                            //尝试 创建 自适应拓展类
                            instance = createAdaptiveExtension();
                            cachedAdaptiveInstance.set(instance);
                        } catch (Throwable t) {
                            createAdaptiveInstanceError = t;
                            throw new IllegalStateException("fail to create adaptive instance: " + t.toString(), t);
                        }
                    }
                }
            } else {
                throw new IllegalStateException("fail to create adaptive instance: " + createAdaptiveInstanceError.toString(), createAdaptiveInstanceError);
            }
        }

        return (T) instance;
    }

    /**
     * 从缓存容器中查找对应的异常对象
     * @param name
     * @return
     */
    private IllegalStateException findException(String name) {
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().contains(name.toLowerCase())) {
                return entry.getValue();
            }
        }
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);


        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (i == 1) {
                buf.append(", possible causes: ");
            }

            buf.append("\r\n(");
            buf.append(i++);
            buf.append(") ");
            buf.append(entry.getKey());
            buf.append(":\r\n");
            buf.append(StringUtils.toString(entry.getValue()));
        }
        return new IllegalStateException(buf.toString());
    }

    @SuppressWarnings("unchecked")
    /**
     * 根据拓展名 创建拓展对象 如果 该拓展类 有包装类 要返回被包装过的 对象
     */
    private T createExtension(String name) {
        //getExtensionClasses() 就已经加载了配置
        //从 拓展名 容器中获取 拓展类
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null) {
            //从拓展名 <-> 异常 中 寻找对应异常类
            throw findException(name);
        }
        try {
            //获取拓展类 对应的 实例对象 看来这个对象 是单例 在加载过程中 这个容器并没有被设置
            T instance = (T) EXTENSION_INSTANCES.get(clazz);
            if (instance == null) {
                //创建实例对象
                EXTENSION_INSTANCES.putIfAbsent(clazz, clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            }
            //注射 依赖的属性 也就是拓展类 即使初始化 可能有些成员变量还没有被设置
            injectExtension(instance);
            //包装对象看来是 一个 包装链 每个 拓展对象都要通过层层包装
            Set<Class<?>> wrapperClasses = cachedWrapperClasses;
            if (wrapperClasses != null && !wrapperClasses.isEmpty()) {
                for (Class<?> wrapperClass : wrapperClasses) {
                    //通过 该实例对象作为 构造函数参数 创建新对象 并注入属性后 再次作为新的 参数进行下次包装处理 也就是一个 满足 SPI 的 接口 如果有
                    //Wrapper 对象 就会被全部的 wrapper 包装 对外是 不可见的
                    //只有 包含本 type 作为 构造函数参数的类才会被加入到容器中
                    instance = injectExtension((T) wrapperClass.getConstructor(type).newInstance(instance));
                }
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " +
                    type + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }

    /**
     * 在目标实例的 setXXX 方法上 根据属性名 注入 指定的 拓展类
     * @param instance
     * @return
     */
    private T injectExtension(T instance) {
        try {
            // == null 代表type 是ExtensionloaderFactory
            if (objectFactory != null) {
                //获取setXXX方法
                for (Method method : instance.getClass().getMethods()) {
                    if (method.getName().startsWith("set")
                            && method.getParameterTypes().length == 1
                            && Modifier.isPublic(method.getModifiers())) {
                        /**
                         * Check {@link DisableInject} to see if we need auto injection for this property
                         */
                        //如果该方法有 DisableInject 代表这个属性不能注入
                        if (method.getAnnotation(DisableInject.class) != null) {
                            continue;
                        }
                        Class<?> pt = method.getParameterTypes()[0];
                        try {
                            //获取 属性名
                            String property = method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
                            //根据 类型和属性名获取属性对象
                            Object object = objectFactory.getExtension(pt, property);
                            if (object != null) {
                                //反射调用 setXXX 方法
                                method.invoke(instance, object);
                            }
                        } catch (Exception e) {
                            logger.error("fail to inject via method " + method.getName()
                                    + " of interface " + type.getName() + ": " + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return instance;
    }

    private Class<?> getExtensionClass(String name) {
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (name == null) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null) {
            throw new IllegalStateException("No such extension \"" + name + "\" for " + type.getName() + "!");
        }
        return clazz;
    }

    /**
     * 获取拓展类 实现数组
     * @return
     */
    private Map<String, Class<?>> getExtensionClasses() {
        //尝试从缓存中获取 拓展类数组
        Map<String, Class<?>> classes = cachedClasses.get();
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    //缓存还不存在 就去配置文件中加载
                    classes = loadExtensionClasses();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * 从配置文件中加载 对应的 拓展类
     * @return
     */
    // synchronized in getExtensionClasses
    private Map<String, Class<?>> loadExtensionClasses() {
        //从拓展的 接口上 获取 SPI 注解  SPI 的 value 是拓展名
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation != null) {
            String value = defaultAnnotation.value();
            //存在拓展名的情况
            if ((value = value.trim()).length() > 0) {
                //尝试是否能拆分
                String[] names = NAME_SEPARATOR.split(value);
                //SPI 注解 只应该保存一个拓展名
                if (names.length > 1) {
                    throw new IllegalStateException("more than 1 default extension name on extension " + type.getName()
                            + ": " + Arrays.toString(names));
                }
                if (names.length == 1) {
                    //设置成 默认的 拓展名 也就是SPI("") 里面的就是默认拓展名
                    cachedDefaultName = names[0];
                }
            }
        }

        Map<String, Class<?>> extensionClasses = new HashMap<String, Class<?>>();
        //从指定目录加载 SPI 文件 并保存到 缓存容器中
        loadDirectory(extensionClasses, DUBBO_INTERNAL_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, DUBBO_INTERNAL_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        loadDirectory(extensionClasses, DUBBO_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, DUBBO_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        //这2个是兼容jdk 的 spi
        loadDirectory(extensionClasses, SERVICES_DIRECTORY, type.getName());
        loadDirectory(extensionClasses, SERVICES_DIRECTORY, type.getName().replace("org.apache", "com.alibaba"));
        return extensionClasses;
    }

    /**
     * 从指定目录下 加载SPI 配置文件
     * @param extensionClasses
     * @param dir
     * @param type
     */
    private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type) {
        //获取完整的文件名  传入的 type 就是拓展类接口对象 type.getName  获取全限定名
        String fileName = dir + type;
        try {
            Enumeration<java.net.URL> urls;
            //获取类加载器
            ClassLoader classLoader = findClassLoader();
            if (classLoader != null) {
                //使用类加载器 从指定路径加载资源
                urls = classLoader.getResources(fileName);
            } else {
                urls = ClassLoader.getSystemResources(fileName);
            }
            //遍历资源
            if (urls != null) {
                while (urls.hasMoreElements()) {
                    java.net.URL resourceURL = urls.nextElement();
                    //从指定URL 中加载资源
                    loadResource(extensionClasses, classLoader, resourceURL);
                }
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " +
                    type + ", description file: " + fileName + ").", t);
        }
    }

    /**
     * 从指定路径加载资源并保存到 给定的容器中
     * @param extensionClasses
     * @param classLoader
     * @param resourceURL
     */
    private void loadResource(Map<String, Class<?>> extensionClasses, ClassLoader classLoader, java.net.URL resourceURL) {
        try {
            //定位到具体文件后创建输入流 读取所有的 拓展类
            BufferedReader reader = new BufferedReader(new InputStreamReader(resourceURL.openStream(), "utf-8"));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    final int ci = line.indexOf('#');
                    if (ci >= 0) {
                        //截断到#前的数据 也就是将 # 注释掉的 内容去掉
                        line = line.substring(0, ci);
                    }
                    line = line.trim();
                    if (line.length() > 0) {
                        try {
                            String name = null;
                            int i = line.indexOf('=');
                            if (i > 0) {
                                //name 作为key line 作为 实际引用的实现类全限定名
                                name = line.substring(0, i).trim();
                                line = line.substring(i + 1).trim();
                            }
                            if (line.length() > 0) {
                                //存在 全限定名 反射创建对应的类 读取 该类的 注解信息并保存到 各个容器中
                                //现在读取到的内容应该是这种格式: jcl=org.apache.dubbo.common.logger.jcl.JclLoggerAdapter
                                loadClass(extensionClasses, resourceURL, Class.forName(line, true, classLoader), name);
                            }
                        } catch (Throwable t) {
                            IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: " + type + ", class line: " + line + ") in " + resourceURL + ", cause: " + t.getMessage(), t);
                            //将 实现类 与 对应的加载异常保存到容器中
                            exceptions.put(line, e);
                        }
                    }
                }
            } finally {
                reader.close();
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " +
                    type + ", class file: " + resourceURL + ") in " + resourceURL, t);
        }
    }

    /**
     * 加载
     * @param extensionClasses
     * @param resourceURL
     * @param clazz
     * @param name
     * @throws NoSuchMethodException
     */
    private void loadClass(Map<String, Class<?>> extensionClasses, java.net.URL resourceURL, Class<?> clazz, String name) throws NoSuchMethodException {
        //通过全限定名 创建的 类 不满足 这个拓展接口
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Error when load extension class(interface: " +
                    type + ", class line: " + clazz.getName() + "), class "
                    + clazz.getName() + "is not subtype of interface.");
        }
        //一个拓展接口，有且仅有一个 Adaptive 拓展实现类
        //如果该 类 存在 Adaptive 注解
        if (clazz.isAnnotationPresent(Adaptive.class)) {
            //将 Adaptive 设置到这个引用上
            if (cachedAdaptiveClass == null) {
                cachedAdaptiveClass = clazz;
                //如果 已经存在Adaptive对象 并且 2个对象不同 抛出异常
            } else if (!cachedAdaptiveClass.equals(clazz)) {
                throw new IllegalStateException("More than 1 adaptive class found: "
                        + cachedAdaptiveClass.getClass().getName()
                        + ", " + clazz.getClass().getName());
            }
            //如果 该类 是Wrapper  就是含有一个 拓展类的 构造函数
        } else if (isWrapperClass(clazz)) {
            Set<Class<?>> wrappers = cachedWrapperClasses;
            if (wrappers == null) {
                cachedWrapperClasses = new ConcurrentHashSet<Class<?>>();
                wrappers = cachedWrapperClasses;
            }
            //在 包装类 容器中 增加一个
            wrappers.add(clazz);
        } else {
            //如果没有无参构造函数 这里应该会抛出异常到上一层方法 也就是 IllegalStateException
            clazz.getConstructor();
            //name 是 spi 的 key
            if (name == null || name.length() == 0) {
                //如果 name 不存在 从注解上获取信息
                //这里就是 兼容 jdk 的SPI 自动创建name
                name = findAnnotationName(clazz);
                if (name.length() == 0) {
                    throw new IllegalStateException("No such extension name for the class " + clazz.getName() + " in the config " + resourceURL);
                }
            }
            //尝试 拆分 key
            String[] names = NAME_SEPARATOR.split(name);
            if (names != null && names.length > 0) {
                Activate activate = clazz.getAnnotation(Activate.class);
                //这里都是保存第一个元素
                if (activate != null) {
                    //将 activate 跟key 的映射保存起来
                    cachedActivates.put(names[0], activate);
                } else {
                    // support com.alibaba.dubbo.common.extension.Activate
                    //兼容老版本
                    com.alibaba.dubbo.common.extension.Activate oldActivate = clazz.getAnnotation(com.alibaba.dubbo.common.extension.Activate.class);
                    if (oldActivate != null) {
                        cachedActivates.put(names[0], oldActivate);
                    }
                }
                //遍历拓展名
                for (String n : names) {
                    //将拓展类和名字的关系保存 这里也只会用到第一个name
                    if (!cachedNames.containsKey(clazz)) {
                        cachedNames.put(clazz, n);
                    }
                    //缓存到 extensionClass 这里 每个name 都被使用到
                    Class<?> c = extensionClasses.get(n);
                    if (c == null) {
                        extensionClasses.put(n, clazz);
                    } else if (c != clazz) {
                        throw new IllegalStateException("Duplicate extension " + type.getName() + " name " + n + " on " + c.getName() + " and " + clazz.getName());
                    }
                }
            }
        }
    }

    /**
     * 判断 该类使用是 包装类
     * 只要存在一个 拓展类接口类型的  构造函数就是
     * @param clazz
     * @return
     */
    private boolean isWrapperClass(Class<?> clazz) {
        try {
            clazz.getConstructor(type);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @SuppressWarnings("deprecation")
    /**
     * 从注解上获取信息
     */
    private String findAnnotationName(Class<?> clazz) {
        org.apache.dubbo.common.Extension extension = clazz.getAnnotation(org.apache.dubbo.common.Extension.class);
        if (extension == null) {
            String name = clazz.getSimpleName();
            if (name.endsWith(type.getSimpleName())) {
                name = name.substring(0, name.length() - type.getSimpleName().length());
            }
            return name.toLowerCase();
        }
        return extension.value();
    }

    /**
     * 创建自适应拓展类 针对的是实现本type 接口的 类 如果找不到 就会抛出异常
     * @return
     */
    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension() {
        try {
            //给创建完成的自适应拓展类 注入 setXXX 属性
            return injectExtension((T) getAdaptiveExtensionClass().newInstance());
        } catch (Exception e) {
            throw new IllegalStateException("Can not create adaptive extension " + type + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 创建自适应拓展对象
     * 有2种
     * 1：实现目标接口的 实现类 类上被@Adaptive 修饰
     * 2：待拓展接口本身 的某些方法 被 @Adaptive 修饰
     * @return
     */
    private Class<?> getAdaptiveExtensionClass() {
        getExtensionClasses();
        //返回 从扫描到的 文件中 带有Adaptive 注解的 类对象
        //这个是针对第一种情况  因为 Adaptive 既可以设置在 class 上 也可以设置在 方法上
        //在扫描时 只是设置了 类上的 Adaptive 注解
        if (cachedAdaptiveClass != null) {
            return cachedAdaptiveClass;
        }
        //针对 方法上的 Adaptive注解
        return cachedAdaptiveClass = createAdaptiveExtensionClass();
    }

    /**
     * 创建 自适应拓展类
     * @return
     */
    private Class<?> createAdaptiveExtensionClass() {
        //生成 待编译的 代码字符串
        String code = createAdaptiveExtensionClassCode();
        //获取类加载器
        ClassLoader classLoader = findClassLoader();
        //获取编译对象 也就是 从SPI 中加载对应的实现信息
        org.apache.dubbo.common.compiler.Compiler compiler = ExtensionLoader.getExtensionLoader(org.apache.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();
        //使用编译类 进行编译
        return compiler.compile(code, classLoader);
    }

    /**
     * 生成 创建自适应拓展类的 代码字符串
     * @return
     */
    private String createAdaptiveExtensionClassCode() {
        StringBuilder codeBuilder = new StringBuilder();
        //从目标接口中 寻找 @Adaptive 修饰的 方法
        Method[] methods = type.getMethods();
        boolean hasAdaptiveAnnotation = false;
        //寻找待 adaptive 的 方法
        for (Method m : methods) {
            if (m.isAnnotationPresent(Adaptive.class)) {
                hasAdaptiveAnnotation = true;
                break;
            }
        }
        // no need to generate adaptive class since there's no adaptive method found.
        // 找不到待拓展方法 抛出异常
        if (!hasAdaptiveAnnotation) {
            throw new IllegalStateException("No adaptive method on extension " + type.getName() + ", refuse to create the adaptive class!");
        }

        //这里相当于 动态生成了一个 XXX$Adaptive 的类对象
        codeBuilder.append("package ").append(type.getPackage().getName()).append(";");
        codeBuilder.append("\nimport ").append(ExtensionLoader.class.getName()).append(";");
        //代表该类实现了 XXX 接口
        codeBuilder.append("\npublic class ").append(type.getSimpleName()).append("$Adaptive").append(" implements ").append(type.getCanonicalName()).append(" {");

        //创建日志类 和 计数器
        codeBuilder.append("\nprivate static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);");
        codeBuilder.append("\nprivate java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);\n");

        //根据 type 需要被实现的 接口 创建 对应的 实现方法
        for (Method method : methods) {
            //获取对应的参数 返回值 异常类型
            Class<?> rt = method.getReturnType();
            Class<?>[] pts = method.getParameterTypes();
            Class<?>[] ets = method.getExceptionTypes();

            Adaptive adaptiveAnnotation = method.getAnnotation(Adaptive.class);
            //这里先创建 方法的内部逻辑 在下面 会创建 对应的 方法名 参数列表
            StringBuilder code = new StringBuilder(512);
            // 非 @Adaptive 注解，生成代码：生成的方法为直接抛出异常。因为，非自适应的接口不应该被调用。
            if (adaptiveAnnotation == null) {
                code.append("throw new UnsupportedOperationException(\"method ")
                        .append(method.toString()).append(" of interface ")
                        .append(type.getName()).append(" is not adaptive method!\");");
            } else {
                //存在 Adaptive 注解时 寻找URL
                int urlTypeIndex = -1;
                //获取 url 参数的 下标
                for (int i = 0; i < pts.length; ++i) {
                    if (pts[i].equals(URL.class)) {
                        urlTypeIndex = i;
                        break;
                    }
                }
                // found parameter in URL type
                if (urlTypeIndex != -1) {
                    // Null Point check
                    //检验参数是否存在
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"url == null\");",
                            urlTypeIndex);
                    code.append(s);

                    //这里 是 创建引用包含 url对象
                    //URL url = argindex    参数在 传入的 时候应该就是 argindex 的方式命名的
                    s = String.format("\n%s url = arg%d;", URL.class.getName(), urlTypeIndex);
                    code.append(s);
                }
                // did not find parameter in URL type
                // == -1 时  也就是没有找到 url 参数
                else {
                    String attribMethod = null;

                    // find URL getter method
                    LBL_PTS:
                    for (int i = 0; i < pts.length; ++i) {
                        //获取 参数的 每个 方法对象
                        Method[] ms = pts[i].getMethods();
                        for (Method m : ms) {
                            String name = m.getName();
                            //这里 参数 类型一般是接口类型 然后 该接口 又继承了其他接口就有更多方法了 在getMethod 中都能获取到
                            if ((name.startsWith("get") || name.length() > 3)
                                    && Modifier.isPublic(m.getModifiers())
                                    && !Modifier.isStatic(m.getModifiers())
                                    && m.getParameterTypes().length == 0
                                    && m.getReturnType() == URL.class) {
                                //如果从参数中找到了 返回 URL 对象的方法
                                //记录了 从第几个参数中找到的
                                urlTypeIndex = i;
                                attribMethod = name;
                                break LBL_PTS;
                            }
                        }
                    }
                    //在所有参数中 都没有找到 获取URL的方法
                    if (attribMethod == null) {
                        throw new IllegalStateException("fail to create adaptive class for interface " + type.getName()
                                + ": not found url parameter or url attribute in parameters of method " + method.getName());
                    }

                    // Null point check
                    //如果找到的那个参数为null 增加 非空校验 的code字符串
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"%s argument == null\");",
                            urlTypeIndex, pts[urlTypeIndex].getName());
                    code.append(s);
                    //如果 调用该方法 返回的 URL 对象为空 也是抛出异常
                    s = String.format("\nif (arg%d.%s() == null) throw new IllegalArgumentException(\"%s argument %s() == null\");",
                            urlTypeIndex, attribMethod, pts[urlTypeIndex].getName(), attribMethod);
                    code.append(s);

                    //在 字符串中追加 通过引用获取 url对象
                    s = String.format("%s url = arg%d.%s();", URL.class.getName(), urlTypeIndex, attribMethod);
                    code.append(s);
                }

                //获取该方法注解的 值  返回的是一个String[] 对应了多个key 按顺序 依次查找value 如果没有 就 返回默认的value
                String[] value = adaptiveAnnotation.value();
                // value is not set, use the value generated from class name as the key
                //将 type 按照 "." 拆分后作为 key[]
                if (value.length == 0) {
                    String splitName = StringUtils.camelToSplitName(type.getSimpleName(), ".");
                    value = new String[]{splitName};
                }

                boolean hasInvocation = false;
                //这一层是针对每个 方法 的 每个参数中是否存在 Invocation 也就是 只有存在这个参数的方法才会有影响
                for (int i = 0; i < pts.length; ++i) {
                    //如果参数类型中存在 Invocation
                    if (("org.apache.dubbo.rpc.Invocation").equals(pts[i].getName())) {
                        // Null Point check
                        // 针对该 Invocation参数 进行非空校验
                        String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"invocation == null\");", i);
                        code.append(s);
                        //Invocation 对象本身 有一个getMethodName 方法
                        s = String.format("\nString methodName = arg%d.getMethodName();", i);
                        code.append(s);
                        hasInvocation = true;
                        break;
                    }
                }

                //获取默认的 拓展名
                String defaultExtName = cachedDefaultName;
                String getNameCode = null;
                //遍历 Adaptive.value() 的 各个key
                //由 内层向外层 构建方法体 如果是 最后一个key 还要跟 默认的  拓展名关联 因为在最后一个key也没找到时 就要使用默认的拓展名
                for (int i = value.length - 1; i >= 0; --i) {
                    //如果是最后一个key
                    if (i == value.length - 1) {
                        if (null != defaultExtName) {
                            //最后一个 key 不是 protocol
                            if (!"protocol".equals(value[i])) {
                                //参数中 存在 Invocation
                                if (hasInvocation) {
                                    //methodName 是当参数是 Invocation 时 获取的方法名
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                } else {
                                    getNameCode = String.format("url.getParameter(\"%s\", \"%s\")", value[i], defaultExtName);
                                }
                            } else {
                                //是 protocol 就获取  url 对象是从 参数 或者 包含protocol 的 参数的其他对象中获取的
                                getNameCode = String.format("( url.getProtocol() == null ? \"%s\" : url.getProtocol() )", defaultExtName);
                            }
                        } else {
                            //不存在默认的 拓展名时
                            if (!"protocol".equals(value[i])) {
                                if (hasInvocation) {
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                } else {
                                    //这个方法不再 传入 默认方法名
                                    getNameCode = String.format("url.getParameter(\"%s\")", value[i]);
                                }
                            } else {
                                //这个方法不再 传入 默认方法名
                                getNameCode = "url.getProtocol()";
                            }
                        }
                        //i 不是最后一个参数的情况
                    } else {
                        if (!"protocol".equals(value[i])) {
                            if (hasInvocation) {
                                getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                            } else {
                                //这里好像是嵌套调用
                                getNameCode = String.format("url.getParameter(\"%s\", %s)", value[i], getNameCode);
                            }
                        } else {
                            //这里好像是嵌套调用
                            getNameCode = String.format("url.getProtocol() == null ? (%s) : url.getProtocol()", getNameCode);
                        }
                    }
                }
                //嵌套多层后 最终结果设置到了 extName 上
                code.append("\nString extName = ").append(getNameCode).append(";");
                // check extName == null?
                //如果拓展名为null 就抛出异常
                String s = String.format("\nif(extName == null) " +
                                "throw new IllegalStateException(\"Fail to get extension(%s) name from url(\" + url.toString() + \") use keys(%s)\");",
                        type.getName(), Arrays.toString(value));
                code.append(s);

                //获取拓展对象
                code.append(String.format("\n%s extension = null;\n try {\nextension = (%<s)%s.getExtensionLoader(%s.class).getExtension(extName);\n}catch(Exception e){\n",
                        type.getName(), ExtensionLoader.class.getSimpleName(), type.getName()));
                //如果计数器为1 就代表失败了 使用默认的 拓展名
                code.append(String.format("if (count.incrementAndGet() == 1) {\nlogger.warn(\"Failed to find extension named \" + extName + \" for type %s, will use default extension %s instead.\", e);\n}\n",
                        type.getName(), defaultExtName));
                //使用默认拓展名 获取拓展对象
                code.append(String.format("extension = (%s)%s.getExtensionLoader(%s.class).getExtension(\"%s\");\n}",
                        type.getName(), ExtensionLoader.class.getSimpleName(), type.getName(), defaultExtName));

                // return statement
                // 返回拓展对象
                if (!rt.equals(void.class)) {
                    code.append("\nreturn ");
                }

                //return extension.原方法名();
                s = String.format("extension.%s(", method.getName());
                code.append(s);
                for (int i = 0; i < pts.length; i++) {
                    if (i != 0) {
                        code.append(", ");
                    }
                    code.append("arg").append(i);
                }
                code.append(");");
            }

            //创建 方法名以及参数列表
            codeBuilder.append("\npublic ").append(rt.getCanonicalName()).append(" ").append(method.getName()).append("(");
            for (int i = 0; i < pts.length; i++) {
                if (i > 0) {
                    codeBuilder.append(", ");
                }
                codeBuilder.append(pts[i].getCanonicalName());
                codeBuilder.append(" ");
                codeBuilder.append("arg").append(i);
            }
            codeBuilder.append(")");
            if (ets.length > 0) {
                //创建抛出的 异常
                codeBuilder.append(" throws ");
                for (int i = 0; i < ets.length; i++) {
                    if (i > 0) {
                        codeBuilder.append(", ");
                    }
                    codeBuilder.append(ets[i].getCanonicalName());
                }
            }
            codeBuilder.append(" {");
            //补上方法的实际逻辑
            codeBuilder.append(code.toString());
            codeBuilder.append("\n}");
        }
        codeBuilder.append("\n}");
        if (logger.isDebugEnabled()) {
            logger.debug(codeBuilder.toString());
        }
        return codeBuilder.toString();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

}
