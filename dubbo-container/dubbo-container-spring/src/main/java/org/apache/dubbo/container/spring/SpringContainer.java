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
package org.apache.dubbo.container.spring;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.container.Container;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * SpringContainer. (SPI, Singleton, ThreadSafe)
 */
public class SpringContainer implements Container {

    /**
     * spring 的配置文件信息
     */
    public static final String SPRING_CONFIG = "dubbo.spring.config";
    /**
     * spring 默认配置地址
     */
    public static final String DEFAULT_SPRING_CONFIG = "classpath*:META-INF/spring/*.xml";
    private static final Logger logger = LoggerFactory.getLogger(SpringContainer.class);

    /**
     * spring的上下文对象 静态对象 全局唯一
     */
    static ClassPathXmlApplicationContext context;

    public static ClassPathXmlApplicationContext getContext() {
        return context;
    }

    @Override
    public void start() {
        //获取spring配置文件的地址
        //【高】JVM 启动参数：-Ddubbo.spring.config=自定义 XML 路径 。
        //【低】Dubbo Properties 配置文件：dubbo.spring.config=自定义 XML 路径 。
        String configPath = ConfigUtils.getProperty(SPRING_CONFIG);
        if (configPath == null || configPath.length() == 0) {
            //一般是 无法从 dubbo默认配置中获取spring配置地址信息的 就使用 默认的 spring 地址
            configPath = DEFAULT_SPRING_CONFIG;
        }
        //通过指定路径获取了 上下文对象 并启动
        context = new ClassPathXmlApplicationContext(configPath.split("[,\\s]+"));
        context.start();
    }

    @Override
    public void stop() {
        try {
            //如果上下文 对象存在就关闭
            if (context != null) {
                context.stop();
                context.close();
                context = null;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

}
