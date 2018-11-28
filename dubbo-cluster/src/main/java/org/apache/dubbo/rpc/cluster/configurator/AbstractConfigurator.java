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
package org.apache.dubbo.rpc.cluster.configurator;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.cluster.Configurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * AbstractOverrideConfigurator
 *
 */
public abstract class AbstractConfigurator implements Configurator {

    /**
     * 配置的 相关url 对象
     */
    private final URL configuratorUrl;

    /**
     * 设置url 对象
     * @param url
     */
    public AbstractConfigurator(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("configurator url == null");
        }
        this.configuratorUrl = url;
    }

    @Override
    public URL getUrl() {
        return configuratorUrl;
    }

    /**
     * 为给定的 url 增加配置
     * @param url - old rovider url.
     * @return
     */
    @Override
    public URL configure(URL url) {
        //如果 该配置规则 端口不存在 就直接返回 host应该代表 该 配置中心是针对 哪个host的 url 进行配置的
        if (configuratorUrl.getHost() == null || url == null || url.getHost() == null) {
            return url;
        }
        // If override url has port, means it is a provider address. We want to control a specific provider with this override url, it may take effect on the specific provider instance or on consumers holding this provider instance.
        // 如果带有端口 就代表针对指定服务提供者
        if (configuratorUrl.getPort() != 0) {
            if (url.getPort() == configuratorUrl.getPort()) {
                //如果匹配就会进行配置 否则 返回原对象
                return configureIfMatch(url.getHost(), url);
            }
        } else {// override url don't have a port, means the ip override url specify is a consumer address or 0.0.0.0
            // 1.If it is a consumer ip address, the intention is to control a specific consumer instance, it must takes effect at the consumer side, any provider received this override url should ignore;
            // 2.If the ip is 0.0.0.0, this override url can be used on consumer, and also can be used on provider
            // 当没有端口时 代表针对的配置对象是消费者端 也要进行匹配
            if (url.getParameter(Constants.SIDE_KEY, Constants.PROVIDER).equals(Constants.CONSUMER)) {
                //getLocalHost 是消费者注册到 注册中心的地址
                return configureIfMatch(NetUtils.getLocalHost(), url);// NetUtils.getLocalHost is the ip address consumer registered to registry.
            //或者是 任意服务提供者端口
            } else if (url.getParameter(Constants.SIDE_KEY, Constants.CONSUMER).equals(Constants.PROVIDER)) {
                //那么这里就传入全部 提供者 那么现在还没有针对 某个 服务提供者 进行配置的功能
                return configureIfMatch(Constants.ANYHOST_VALUE, url);// take effect on all providers, so address must be 0.0.0.0, otherwise it won't flow to this if branch
            }
        }
        return url;
    }

    /**
     * 检测配置是否匹配
     * @param host 是需要被配置的 url 的host
     * @param url
     * @return
     */
    private URL configureIfMatch(String host, URL url) {
        //如果配置中心的 host 是0.0.0.0 就代表是针对 任何url的 或者 端口相匹配
        //如果传入的host 是0.0.0.0 而 配置的url 不是 也不会进行处理 也就是 配置中心是针对某一个host的
        if (Constants.ANYHOST_VALUE.equals(configuratorUrl.getHost()) || host.equals(configuratorUrl.getHost())) {
            //判断针对的应用返回
            String configApplication = configuratorUrl.getParameter(Constants.APPLICATION_KEY,
                    configuratorUrl.getUsername());
            String currentApplication = url.getParameter(Constants.APPLICATION_KEY, url.getUsername());
            //如果 应用名没设置 或者 是全应用 或者 匹配 都可以进行配置
            if (configApplication == null || Constants.ANY_VALUE.equals(configApplication)
                    || configApplication.equals(currentApplication)) {
                //设置一些属性到 set中
                Set<String> conditionKeys = new HashSet<String>();
                conditionKeys.add(Constants.CATEGORY_KEY);
                conditionKeys.add(Constants.CHECK_KEY);
                conditionKeys.add(Constants.DYNAMIC_KEY);
                conditionKeys.add(Constants.ENABLED_KEY);
                //获取 成员变量的url 中全部的属性
                for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    //~ 好像是代表要取消的 属性  出现 application 和 side 时 要加入到 set容器中 这些都是需要匹配 的条件
                    if (key.startsWith("~") || Constants.APPLICATION_KEY.equals(key) || Constants.SIDE_KEY.equals(key)) {
                        conditionKeys.add(key);
                        //这里代表没有匹配上 就直接返回原url 代表 不进行配置操作
                        if (value != null && !Constants.ANY_VALUE.equals(value)
                                && !value.equals(url.getParameter(key.startsWith("~") ? key.substring(1) : key))) {
                            return url;
                        }
                    }
                }
                //全部匹配时 当 成员 url 移除部分属性后 进行配置 就是将 成员变量中独有的属性补充到 传入的url 中
                return doConfigure(url, configuratorUrl.removeParameters(conditionKeys));
            }
        }
        //一开始端口没匹配 也是直接返回
        return url;
    }

    /**
     * Sort by host, priority
     * 1. the url with a specific host ip should have higher priority than 0.0.0.0
     * 2. if two url has the same host, compare by priority value；
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(Configurator o) {
        if (o == null) {
            return -1;
        }

        int ipCompare = getUrl().getHost().compareTo(o.getUrl().getHost());
        if (ipCompare == 0) {//host is the same, sort by priority
            int i = getUrl().getParameter(Constants.PRIORITY_KEY, 0),
                    j = o.getUrl().getParameter(Constants.PRIORITY_KEY, 0);
            return Integer.compare(i, j);
        } else {
            return ipCompare;
        }


    }

    protected abstract URL doConfigure(URL currentUrl, URL configUrl);

}
