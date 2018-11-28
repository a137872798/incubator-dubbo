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
package org.apache.dubbo.config.spring;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.spring.extension.SpringExtensionFactory;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.dubbo.config.spring.util.BeanFactoryUtils.addApplicationListener;

/**
 * ServiceFactoryBean
 *
 * 该类实现了 ServiceConfig 实现了 自动像注册中心 进行服务发布
 * @export
 */
public class ServiceBean<T> extends ServiceConfig<T> implements InitializingBean, DisposableBean, ApplicationContextAware, ApplicationListener<ContextRefreshedEvent>, BeanNameAware {

    private static final long serialVersionUID = 213195494150089726L;

    private final transient Service service;

    /**
     * spring 的上下文对象 这里保存了上下文的 引用
     */
    private transient ApplicationContext applicationContext;

    private transient String beanName;

    private transient boolean supportedApplicationListener;

    public ServiceBean() {
        super();
        this.service = null;
    }

    /**
     * 这个应该是 通过spring注解创建bean 对象的时候设置的 使用了dubbo 的S二vice 注解
     */
    public ServiceBean(Service service) {
        super(service);
        this.service = service;
    }

    /**
     * 在创建该bean 的时候会顺宝设置到上下文中 方便随时获取该bean 实例对象 每个bean 对象都实现了 这个方法 所以获取上下文对象
     * 可以获取 任意bean 对象
     * 这里猜测是 每个 对象在 传入 解析xml 前都 实现了这个方法 然后 在某处的调用链中 依次调用每个对象的这个方法
     * 传入的上下文对象 会收到spring 生命周期的影响 因为这里是引用传递
     *
     * 这样通过创建 SpringExtensionFactory 对象可以随时从上下文对象中获取指定的 bean 对象
     * @param applicationContext
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        //为 spring 拓展工厂对象 传入了 上下文对象  并且该对象会被添加一个
        SpringExtensionFactory.addApplicationContext(applicationContext);
        //为上下文对象 传入本类作为监听器 当上下文对象发生变动 判断是否要进行 export方法
        supportedApplicationListener = addApplicationListener(applicationContext, this);
    }

    /**
     * 在 某个调用链中 设置该方法  应该是在 从xml 中生成BeanDefinition 后 根据 parse传入的对象是否实现了某些接口 并调用相应的方法
     * @param name
     */
    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    /**
     * Gets associated {@link Service}
     *
     * 这里 注解是怎么生成的 还不太懂 应该是跟 spring 的机制有关
     * @return associated {@link Service}
     */
    public Service getService() {
        return service;
    }

    /**
     * 容器发生变化时 触发 比如刷新
     * @param event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        //如果 为出口 也未注销 代表还没有启动吧
        if (!isExported() && !isUnexported()) {
            if (logger.isInfoEnabled()) {
                logger.info("The service ready on spring started. service: " + getInterface());
            }
            //开始调用 出口方法
            export();
        }
    }

    /**
     * 当该bean 对象生成后 执行
     * @throws Exception
     */
    @Override
    @SuppressWarnings({"unchecked", "deprecation"})
    public void afterPropertiesSet() throws Exception {
        //这里 spring 的加载顺序还不是很清楚 先揣测一下

        //该对象创建时 生成了ServiceConfig 对象 而里面的 provider 对象还没有设置
        if (getProvider() == null) {
            //尝试获取从xml 中 读取的 providerConfig 的所有属性
            Map<String, ProviderConfig> providerConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProviderConfig.class, false, false);
            //为null 代表 上下文对象 还没有设置 也就是ApplicationContextAware 还没触发回调
            if (providerConfigMap != null && providerConfigMap.size() > 0) {
                //这里是一级级获取  协议配置对象  应该是 要代表 准备开始 像注册中心发布 服务了
                //这里 需要 providerConfig 代表发布时的配置 ProtocolConfig 代表 使用什么协议进行远程调用
                Map<String, ProtocolConfig> protocolConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProtocolConfig.class, false, false);
                //协议配置没有完成 而 provider 已经读到属性
                if ((protocolConfigMap == null || protocolConfigMap.size() == 0)
                        && providerConfigMap.size() > 1) { // backward compatibility
                    //先提取 所有ProviderConfig 对象
                    List<ProviderConfig> providerConfigs = new ArrayList<ProviderConfig>();
                    for (ProviderConfig config : providerConfigMap.values()) {
                        if (config.isDefault() != null && config.isDefault()) {
                            //只添加默认配置???
                            providerConfigs.add(config);
                        }
                    }
                    if (!providerConfigs.isEmpty()) {
                        //将provider 添加到 serviceConfig 中 在 serviceConfig 中 会将 providerConfig 转换成ProtocolConfig
                        //存在 某种协议的 providerConfig 就一定有对应 的协议配置
                        setProviders(providerConfigs);
                    }
                } else {
                    //如果协议对象和 提供者对象都 初始化完成
                    ProviderConfig providerConfig = null;
                    for (ProviderConfig config : providerConfigMap.values()) {
                        if (config.isDefault() == null || config.isDefault()) {
                            if (providerConfig != null) {
                                throw new IllegalStateException("Duplicate provider configs: " + providerConfig + " and " + config);
                            }
                            //只允许设置一个配置 且这个配置 必须是默认配置 这里没有设置协议对象
                            providerConfig = config;
                        }
                    }
                    if (providerConfig != null) {
                        //将默认配置设置到 serviceConfig中
                        setProvider(providerConfig);
                    }
                }
            }
        }
        //如果 applicationConfig 还没有创建
        if (getApplication() == null
                && (getProvider() == null || getProvider().getApplication() == null)) {
            //尝试获取 applicationConfig
            Map<String, ApplicationConfig> applicationConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ApplicationConfig.class, false, false);
            if (applicationConfigMap != null && applicationConfigMap.size() > 0) {
                ApplicationConfig applicationConfig = null;
                for (ApplicationConfig config : applicationConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault()) {
                        if (applicationConfig != null) {
                            throw new IllegalStateException("Duplicate application configs: " + applicationConfig + " and " + config);
                        }
                        applicationConfig = config;
                    }
                }
                if (applicationConfig != null) {
                    setApplication(applicationConfig);
                }
            }
        }
        if (getModule() == null
                && (getProvider() == null || getProvider().getModule() == null)) {
            Map<String, ModuleConfig> moduleConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ModuleConfig.class, false, false);
            if (moduleConfigMap != null && moduleConfigMap.size() > 0) {
                ModuleConfig moduleConfig = null;
                for (ModuleConfig config : moduleConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault()) {
                        if (moduleConfig != null) {
                            throw new IllegalStateException("Duplicate module configs: " + moduleConfig + " and " + config);
                        }
                        moduleConfig = config;
                    }
                }
                if (moduleConfig != null) {
                    setModule(moduleConfig);
                }
            }
        }
        if ((getRegistries() == null || getRegistries().isEmpty())
                && (getProvider() == null || getProvider().getRegistries() == null || getProvider().getRegistries().isEmpty())
                && (getApplication() == null || getApplication().getRegistries() == null || getApplication().getRegistries().isEmpty())) {
            Map<String, RegistryConfig> registryConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, RegistryConfig.class, false, false);
            if (registryConfigMap != null && registryConfigMap.size() > 0) {
                List<RegistryConfig> registryConfigs = new ArrayList<RegistryConfig>();
                for (RegistryConfig config : registryConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault()) {
                        registryConfigs.add(config);
                    }
                }
                if (!registryConfigs.isEmpty()) {
                    super.setRegistries(registryConfigs);
                }
            }
        }
        if (getMonitor() == null
                && (getProvider() == null || getProvider().getMonitor() == null)
                && (getApplication() == null || getApplication().getMonitor() == null)) {
            Map<String, MonitorConfig> monitorConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, MonitorConfig.class, false, false);
            if (monitorConfigMap != null && monitorConfigMap.size() > 0) {
                MonitorConfig monitorConfig = null;
                for (MonitorConfig config : monitorConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault()) {
                        if (monitorConfig != null) {
                            throw new IllegalStateException("Duplicate monitor configs: " + monitorConfig + " and " + config);
                        }
                        monitorConfig = config;
                    }
                }
                if (monitorConfig != null) {
                    setMonitor(monitorConfig);
                }
            }
        }
        if ((getProtocols() == null || getProtocols().isEmpty())
                && (getProvider() == null || getProvider().getProtocols() == null || getProvider().getProtocols().isEmpty())) {
            Map<String, ProtocolConfig> protocolConfigMap = applicationContext == null ? null : BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, ProtocolConfig.class, false, false);
            if (protocolConfigMap != null && protocolConfigMap.size() > 0) {
                List<ProtocolConfig> protocolConfigs = new ArrayList<ProtocolConfig>();
                for (ProtocolConfig config : protocolConfigMap.values()) {
                    if (config.isDefault() == null || config.isDefault()) {
                        protocolConfigs.add(config);
                    }
                }
                if (!protocolConfigs.isEmpty()) {
                    super.setProtocols(protocolConfigs);
                }
            }
        }
        if (getPath() == null || getPath().length() == 0) {
            if (beanName != null && beanName.length() > 0
                    && getInterface() != null && getInterface().length() > 0
                    && beanName.startsWith(getInterface())) {
                setPath(beanName);
            }
        }
        if (!supportedApplicationListener) {
            export();
        }
    }

    /**
     * 在bean 被销毁时 触发 但是现在 服务终止是通过终结钩子的 所以就是 noop
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        // no need to call unexport() here, see
        // org.apache.dubbo.config.spring.extension.SpringExtensionFactory.ShutdownHookListener
    }

    /**
     * 跟 dubbox 相关的 先不管
     * @param ref
     * @return
     */
    // merged from dubbox
    @Override
    protected Class getServiceClass(T ref) {
        if (AopUtils.isAopProxy(ref)) {
            return AopUtils.getTargetClass(ref);
        }
        return super.getServiceClass(ref);
    }
}
