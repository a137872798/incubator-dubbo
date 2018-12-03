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
package org.apache.dubbo.monitor.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorFactory;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MonitorFilter. (SPI, Singleton, ThreadSafe)
 * 监控中心  同时针对 消费者 和 提供者生效
 */
@Activate(group = {Constants.PROVIDER, Constants.CONSUMER})
public class MonitorFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(MonitorFilter.class);

    /**
     * 记录某方法的当前调用次数
     */
    private final ConcurrentMap<String, AtomicInteger> concurrents = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * 监控中心工厂
     */
    private MonitorFactory monitorFactory;

    public void setMonitorFactory(MonitorFactory monitorFactory) {
        this.monitorFactory = monitorFactory;
    }

    /**
     * 当方法进行调用的 时候 拦截 并触发相应逻辑
     * intercepting invocation
     * @param invoker    service 调用链的 上个元素
     * @param invocation invocation.
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //如果需要进行监控
        if (invoker.getUrl().hasParameter(Constants.MONITOR_KEY)) {
            RpcContext context = RpcContext.getContext(); // provider must fetch context before invoke() gets called
            String remoteHost = context.getRemoteHost();
            long start = System.currentTimeMillis(); // record start timestamp
            //增加当前调用次数
            getConcurrent(invoker, invocation).incrementAndGet(); // count up
            try {
                //调用结束后
                Result result = invoker.invoke(invocation); // proceed invocation chain
                //收集调用信息
                collect(invoker, invocation, result, remoteHost, start, false);
                return result;
            } catch (RpcException e) {
                //收集失败信息
                collect(invoker, invocation, null, remoteHost, start, true);
                throw e;
            } finally {
                //将当前调用次数减少
                getConcurrent(invoker, invocation).decrementAndGet(); // count down
            }
        } else {
            //代表 这层不对该invoker 对象进行过滤
            return invoker.invoke(invocation);
        }
    }

    /**
     * collect info
     * 收集信息
     */
    private void collect(Invoker<?> invoker, Invocation invocation, Result result, String remoteHost, long start, boolean error) {
        try {
            // ---- service statistics ----
            //获取 调用总时长
            long elapsed = System.currentTimeMillis() - start; // invocation cost
            //获取并发调用次数
            int concurrent = getConcurrent(invoker, invocation).get(); // current concurrent count
            //获取该 invoker 相关信息 作为记录的key
            String application = invoker.getUrl().getParameter(Constants.APPLICATION_KEY);
            String service = invoker.getInterface().getName(); // service name
            String method = RpcUtils.getMethodName(invocation); // method name
            String group = invoker.getUrl().getParameter(Constants.GROUP_KEY);
            String version = invoker.getUrl().getParameter(Constants.VERSION_KEY);
            //获取 监控中心url  当存在监控中心的情况 在url 中是包含 监控中心url的
            URL url = invoker.getUrl().getUrlParameter(Constants.MONITOR_KEY);
            //通过 url 获取监控中心对象
            Monitor monitor = monitorFactory.getMonitor(url);
            //不存在监控中心 直接返回 这里有可能 正在创建监控中心
            if (monitor == null) {
                return;
            }
            int localPort;
            String remoteKey;
            String remoteValue;
            //区分是消费者 还是 提供者
            if (Constants.CONSUMER_SIDE.equals(invoker.getUrl().getParameter(Constants.SIDE_KEY))) {
                // ---- for service consumer ----
                localPort = 0;
                remoteKey = MonitorService.PROVIDER;
                remoteValue = invoker.getUrl().getAddress();
            } else {
                // ---- for service provider ----
                localPort = invoker.getUrl().getPort();
                remoteKey = MonitorService.CONSUMER;
                remoteValue = remoteHost;
            }
            String input = "", output = "";
            //获取 本次请求的 数据大小
            if (invocation.getAttachment(Constants.INPUT_KEY) != null) {
                input = invocation.getAttachment(Constants.INPUT_KEY);
            }
            //获取 本次返回的 数据大小
            if (result != null && result.getAttachment(Constants.OUTPUT_KEY) != null) {
                output = result.getAttachment(Constants.OUTPUT_KEY);
            }
            //收集信息  创建一个  以 count 为 协议的 url 作为 监控中心保存的数据
            monitor.collect(new URL(Constants.COUNT_PROTOCOL,
                    NetUtils.getLocalHost(), localPort,
                    service + "/" + method,
                    MonitorService.APPLICATION, application,
                    MonitorService.INTERFACE, service,
                    MonitorService.METHOD, method,
                    remoteKey, remoteValue,
                    error ? MonitorService.FAILURE : MonitorService.SUCCESS, "1",
                    MonitorService.ELAPSED, String.valueOf(elapsed),
                    MonitorService.CONCURRENT, String.valueOf(concurrent),
                    Constants.INPUT_KEY, input,
                    Constants.OUTPUT_KEY, output,
                    Constants.GROUP_KEY, group,
                    Constants.VERSION_KEY, version));
        } catch (Throwable t) {
            logger.error("Failed to monitor count service " + invoker.getUrl() + ", cause: " + t.getMessage(), t);
        }
    }

    /**
     * 获取 当前的 服务 的并发调用次数
     * @param invoker
     * @param invocation
     * @return
     */
    // concurrent counter
    private AtomicInteger getConcurrent(Invoker<?> invoker, Invocation invocation) {
        //将接口名 和方法名 拼接成 key 获取调用次数
        String key = invoker.getInterface().getName() + "." + invocation.getMethodName();
        AtomicInteger concurrent = concurrents.get(key);
        if (concurrent == null) {
            concurrents.putIfAbsent(key, new AtomicInteger());
            concurrent = concurrents.get(key);
        }
        return concurrent;
    }

}
