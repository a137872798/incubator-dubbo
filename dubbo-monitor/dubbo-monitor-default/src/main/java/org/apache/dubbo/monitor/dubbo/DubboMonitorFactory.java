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
package org.apache.dubbo.monitor.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.monitor.support.AbstractMonitorFactory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;

/**
 * DefaultMonitorFactory
 * duboo 的 监控中心 工厂对象
 */
public class DubboMonitorFactory extends AbstractMonitorFactory {

    //这2个 属性 会自动变成 extension 的 自适应对象

    private Protocol protocol;

    private ProxyFactory proxyFactory;

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    /**
     * 创建 监控中心对象
     * @param url
     * @return
     */
    @Override
    protected Monitor createMonitor(URL url) {
        url = url.setProtocol(url.getParameter(Constants.PROTOCOL_KEY, "dubbo"));
        if (url.getPath() == null || url.getPath().length() == 0) {
            //设置path 属性
            url = url.setPath(MonitorService.class.getName());
        }
        String filter = url.getParameter(Constants.REFERENCE_FILTER_KEY);
        if (filter == null || filter.length() == 0) {
            filter = "";
        } else {
            filter = filter + ",";
        }
        //默认使用 安全失败的 集群 拦截器中去除掉monitor 避免被重复添加
        url = url.addParameters(Constants.CLUSTER_KEY, "failsafe", Constants.CHECK_KEY, String.valueOf(false),
                Constants.REFERENCE_FILTER_KEY, filter + "-monitor");
        //这里通过 dubboProtocol 的形式 连接到 监控中心地址 并返回一个 dubboInvoker对象  这个url 就是监控中心地址
        Invoker<MonitorService> monitorInvoker = protocol.refer(MonitorService.class, url);
        //将 连接 返回结果的远程调用对象代理成 普通的 ref对象  当调用对应方法时 就会在内部 发起RPC 请求
        MonitorService monitorService = proxyFactory.getProxy(monitorInvoker);
        //用代理对象创建监控中心
        return new DubboMonitor(monitorInvoker, monitorService);
    }

}