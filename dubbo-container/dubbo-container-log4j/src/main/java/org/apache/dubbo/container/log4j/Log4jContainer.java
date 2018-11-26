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
package org.apache.dubbo.container.log4j;

import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.container.Container;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.util.Enumeration;
import java.util.Properties;

/**
 * Log4jContainer. (SPI, Singleton, ThreadSafe)
 */
public class Log4jContainer implements Container {

    /**
     * 日志文件路径
     */
    public static final String LOG4J_FILE = "dubbo.log4j.file";

    /**
     * 日志级别配置
     */
    public static final String LOG4J_LEVEL = "dubbo.log4j.level";

    /**
     * 日志子目录
     */
    public static final String LOG4J_SUBDIRECTORY = "dubbo.log4j.subdirectory";

    /**
     * 默认日志级别
     */
    public static final String DEFAULT_LOG4J_LEVEL = "ERROR";

    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        //获取log4j 的 路径
        String file = ConfigUtils.getProperty(LOG4J_FILE);
        if (file != null && file.length() > 0) {
            //获取日志级别
            String level = ConfigUtils.getProperty(LOG4J_LEVEL);
            if (level == null || level.length() == 0) {
                level = DEFAULT_LOG4J_LEVEL;
            }
            //创建properties 对象
            Properties properties = new Properties();
            properties.setProperty("log4j.rootLogger", level + ",application");
            properties.setProperty("log4j.appender.application", "org.apache.log4j.DailyRollingFileAppender");
            properties.setProperty("log4j.appender.application.File", file);
            properties.setProperty("log4j.appender.application.Append", "true");
            properties.setProperty("log4j.appender.application.DatePattern", "'.'yyyy-MM-dd");
            properties.setProperty("log4j.appender.application.layout", "org.apache.log4j.PatternLayout");
            properties.setProperty("log4j.appender.application.layout.ConversionPattern", "%d [%t] %-5p %C{6} (%F:%L) - %m%n");
            //给 配置类设置 配置文件
            PropertyConfigurator.configure(properties);
        }
        //获取子目录地址
        String subdirectory = ConfigUtils.getProperty(LOG4J_SUBDIRECTORY);
        if (subdirectory != null && subdirectory.length() > 0) {
            //获取当前所有日志
            Enumeration<org.apache.log4j.Logger> ls = LogManager.getCurrentLoggers();
            while (ls.hasMoreElements()) {
                org.apache.log4j.Logger l = ls.nextElement();
                if (l != null) {
                    //获取每个 appender 对象
                    Enumeration<Appender> as = l.getAllAppenders();
                    while (as.hasMoreElements()) {
                        Appender a = as.nextElement();
                        //如果是文件appender 类型
                        if (a instanceof FileAppender) {
                            FileAppender fa = (FileAppender) a;
                            //获取目标文件
                            String f = fa.getFile();
                            if (f != null && f.length() > 0) {
                                int i = f.replace('\\', '/').lastIndexOf('/');
                                String path;
                                if (i == -1) {
                                    path = subdirectory;
                                } else {
                                    path = f.substring(0, i);
                                    //该路径需要以 子目录 结尾
                                    if (!path.endsWith(subdirectory)) {
                                        path = path + "/" + subdirectory;
                                    }
                                    f = f.substring(i + 1);
                                }
                                //更新文件路径
                                fa.setFile(path + "/" + f);
                                //使配置生效
                                fa.activateOptions();
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void stop() {
    }

}
