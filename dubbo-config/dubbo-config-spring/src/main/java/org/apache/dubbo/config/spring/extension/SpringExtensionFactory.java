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
package org.apache.dubbo.config.spring.extension;

import org.apache.dubbo.common.extension.ExtensionFactory;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.config.DubboShutdownHook;
import org.apache.dubbo.config.spring.util.BeanFactoryUtils;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

import java.util.Set;

/**
 * SpringExtensionFactory
 * 基于Spring 实现的 拓展工厂
 */
public class SpringExtensionFactory implements ExtensionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SpringExtensionFactory.class);

    /**
     * spring  上下文容器 保存了 关联的 所有 xml 启动的上下文对象
     */
    private static final Set<ApplicationContext> contexts = new ConcurrentHashSet<ApplicationContext>();
    /**
     * 应用监听器对象
     */
    private static final ApplicationListener shutdownHookListener = new ShutdownHookListener();

    public static void addApplicationContext(ApplicationContext context) {
        contexts.add(context);
        //为上下文对象添加终结钩子
        BeanFactoryUtils.addApplicationListener(context, shutdownHookListener);
    }

    public static void removeApplicationContext(ApplicationContext context) {
        contexts.remove(context);
    }

    public static Set<ApplicationContext> getContexts() {
        return contexts;
    }

    // currently for test purpose
    public static void clearContexts() {
        contexts.clear();
    }

    /**
     * 以spring 上下文的方式 获取 某个 bean对象
     *
     * 任意一个SpringExtensionFactory 对象都可以从全局上下文对象中 获取指定的bean对象
     * @param type object type.
     * @param name object name.
     * @param <T>
     * @return
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getExtension(Class<T> type, String name) {

        //SPI should be get from SpiExtensionFactory
        //代表该类只能通过SPI机制加载
        if (type.isInterface() && type.isAnnotationPresent(SPI.class)) {
            return null;
        }

        for (ApplicationContext context : contexts) {
            //遍历 如果存在 指定的 bean 就创建bean 对象 且 该实例实现该接口
            if (context.containsBean(name)) {
                //通过beanName 找到对应的bean 对象 如果是 给定类型的实例就直接返回
                Object bean = context.getBean(name);
                if (type.isInstance(bean)) {
                    return (T) bean;
                }
            }
        }

        //下面这段是 新版本加的----------------------------------------

        logger.warn("No spring extension (bean) named:" + name + ", try to find an extension (bean) of type " + type.getName());

        //Object类型就不用返回
        if (Object.class == type) {
            return null;
        }

        for (ApplicationContext context : contexts) {
            try {
                //依次从每个上下文中 获取bean 对象 抛出异常 捕获 不处理 直到获取到bean 对象
                return context.getBean(type);
            } catch (NoUniqueBeanDefinitionException multiBeanExe) {
                logger.warn("Find more than 1 spring extensions (beans) of type " + type.getName() + ", will stop auto injection. Please make sure you have specified the concrete parameter type and there's only one extension of that type.");
            } catch (NoSuchBeanDefinitionException noBeanExe) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Error when get spring extension(bean) for type:" + type.getName(), noBeanExe);
                }
            }
        }

        logger.warn("No spring extension (bean) named:" + name + ", type:" + type.getName() + " found, stop get bean.");

        //没有找到 bean 对象
        return null;
    }

    /**
     * 应用监听器对象 这里触发条件是 当 应用关闭时  即 spring停止时
     */
    private static class ShutdownHookListener implements ApplicationListener {
        @Override
        public void onApplicationEvent(ApplicationEvent event) {
            if (event instanceof ContextClosedEvent) {
                // we call it anyway since dubbo shutdown hook make sure its destroyAll() is re-entrant.
                // pls. note we should not remove dubbo shutdown hook when spring framework is present, this is because
                // its shutdown hook may not be installed.
                //调用 dubbo 的终结钩子 开始 关闭程序
                DubboShutdownHook shutdownHook = DubboShutdownHook.getDubboShutdownHook();
                shutdownHook.destroyAll();
            }
        }
    }
}
