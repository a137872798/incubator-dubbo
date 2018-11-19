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
     * spring  上下文容器
     */
    private static final Set<ApplicationContext> contexts = new ConcurrentHashSet<ApplicationContext>();
    private static final ApplicationListener shutdownHookListener = new ShutdownHookListener();

    public static void addApplicationContext(ApplicationContext context) {
        contexts.add(context);
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

    @Override
    @SuppressWarnings("unchecked")
    /**
     * 获取拓展对象
     */
    public <T> T getExtension(Class<T> type, String name) {

        //SPI should be get from SpiExtensionFactory
        //实现SPI 的 待拓展对象不应该有SPRINGExtension 实现
        if (type.isInterface() && type.isAnnotationPresent(SPI.class)) {
            return null;
        }

        for (ApplicationContext context : contexts) {
            //遍历 如果存在 指定的 bean 就创建bean 对象 且 该实例实现该接口
            if (context.containsBean(name)) {
                Object bean = context.getBean(name);
                if (type.isInstance(bean)) {
                    return (T) bean;
                }
            }
        }

        //下面这段是 新版本加的----------------------------------------

        logger.warn("No spring extension (bean) named:" + name + ", try to find an extension (bean) of type " + type.getName());

        //如果 是 Object 类型 就不返回
        if (Object.class == type) {
            return null;
        }

        for (ApplicationContext context : contexts) {
            try {
                //这里 不管是否 实现该接口 都返回
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

        return null;
    }

    private static class ShutdownHookListener implements ApplicationListener {
        @Override
        public void onApplicationEvent(ApplicationEvent event) {
            if (event instanceof ContextClosedEvent) {
                // we call it anyway since dubbo shutdown hook make sure its destroyAll() is re-entrant.
                // pls. note we should not remove dubbo shutdown hook when spring framework is present, this is because
                // its shutdown hook may not be installed.
                DubboShutdownHook shutdownHook = DubboShutdownHook.getDubboShutdownHook();
                shutdownHook.destroyAll();
            }
        }
    }
}
