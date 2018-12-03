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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DubboMonitor
 * 监控中心实现
 */
public class DubboMonitor implements Monitor {

    private static final Logger logger = LoggerFactory.getLogger(DubboMonitor.class);

    private static final int LENGTH = 10;

    /**
     * 定时器
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3, new NamedThreadFactory("DubboMonitorSendTimer", true));

    /**
     * 定时任务结果
     */
    private final ScheduledFuture<?> sendFuture;

    /**
     * invoker 中包含 的就是监控中心对象
     */
    private final Invoker<MonitorService> monitorInvoker;

    /**
     * 监控服务对象  这是调用getProxy 生成的 还不知道 动态代理是怎么处理的 这个对象才是进行远程调用的对象
     */
    private final MonitorService monitorService;

    /**
     * 汇报的间隔时间
     */
    private final long monitorInterval;

    /**
     * 统计信息  这个long 数组中每个元素 都对应一个 数据
     */
    private final ConcurrentMap<Statistics, AtomicReference<long[]>> statisticsMap = new ConcurrentHashMap<Statistics, AtomicReference<long[]>>();

    public DubboMonitor(Invoker<MonitorService> monitorInvoker, MonitorService monitorService) {
        this.monitorInvoker = monitorInvoker;
        this.monitorService = monitorService;
        this.monitorInterval = monitorInvoker.getUrl().getPositiveParameter("interval", 60000);
        // collect timer for collecting statistics data
        sendFuture = scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                // collect data
                try {
                    //定期往监控中心发送信息
                    send();
                } catch (Throwable t) {
                    logger.error("Unexpected error occur at send statistic, cause: " + t.getMessage(), t);
                }
            }
        }, monitorInterval, monitorInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * 定时 发送数据
     */
    public void send() {
        logger.debug("Send statistics to monitor " + getUrl());
        String timestamp = String.valueOf(System.currentTimeMillis());
        for (Map.Entry<Statistics, AtomicReference<long[]>> entry : statisticsMap.entrySet()) {
            // get statistics data
            // 获取当前的数据
            Statistics statistics = entry.getKey();
            AtomicReference<long[]> reference = entry.getValue();
            long[] numbers = reference.get();
            long success = numbers[0];
            long failure = numbers[1];
            long input = numbers[2];
            long output = numbers[3];
            long elapsed = numbers[4];
            long concurrent = numbers[5];
            long maxInput = numbers[6];
            long maxOutput = numbers[7];
            long maxElapsed = numbers[8];
            long maxConcurrent = numbers[9];
            String version = getUrl().getParameter(Constants.DEFAULT_PROTOCOL);

            // send statistics data  生成统计信息数据
            URL url = statistics.getUrl()
                    .addParameters(MonitorService.TIMESTAMP, timestamp,
                            MonitorService.SUCCESS, String.valueOf(success),
                            MonitorService.FAILURE, String.valueOf(failure),
                            MonitorService.INPUT, String.valueOf(input),
                            MonitorService.OUTPUT, String.valueOf(output),
                            MonitorService.ELAPSED, String.valueOf(elapsed),
                            MonitorService.CONCURRENT, String.valueOf(concurrent),
                            MonitorService.MAX_INPUT, String.valueOf(maxInput),
                            MonitorService.MAX_OUTPUT, String.valueOf(maxOutput),
                            MonitorService.MAX_ELAPSED, String.valueOf(maxElapsed),
                            MonitorService.MAX_CONCURRENT, String.valueOf(maxConcurrent),
                            Constants.DEFAULT_PROTOCOL, version
                    );
            //这个url 包含了最新的 统计信息
            monitorService.collect(url);

            // reset
            long[] current;
            long[] update = new long[LENGTH];
            do {
                //更新统计信息
                current = reference.get();
                if (current == null) {
                    update[0] = 0;
                    update[1] = 0;
                    update[2] = 0;
                    update[3] = 0;
                    update[4] = 0;
                    update[5] = 0;
                } else {
                    //发送过的 统计信息 就 清除 如果出现并发 那么 - 之后 还剩下 最新增加的统计信息
                    update[0] = current[0] - success;
                    update[1] = current[1] - failure;
                    update[2] = current[2] - input;
                    update[3] = current[3] - output;
                    update[4] = current[4] - elapsed;
                    update[5] = current[5] - concurrent;
                }
            } while (!reference.compareAndSet(current, update));
        }
    }

    /**
     * 更新本地数据 没有更新到 监控中心 只有调用 MonitorService 的 collect 才会发起请求
     * @param url
     */
    @Override
    public void collect(URL url) {
        //解析出 监控拦截器 获取到 的数据
        // data to collect from url
        int success = url.getParameter(MonitorService.SUCCESS, 0);
        int failure = url.getParameter(MonitorService.FAILURE, 0);
        int input = url.getParameter(MonitorService.INPUT, 0);
        int output = url.getParameter(MonitorService.OUTPUT, 0);
        int elapsed = url.getParameter(MonitorService.ELAPSED, 0);
        int concurrent = url.getParameter(MonitorService.CONCURRENT, 0);
        // init atomic reference
        // 生成对应的 key
        Statistics statistics = new Statistics(url);
        // 获取该统计对象的 记录
        AtomicReference<long[]> reference = statisticsMap.get(statistics);
        if (reference == null) {
            statisticsMap.putIfAbsent(statistics, new AtomicReference<long[]>());
            reference = statisticsMap.get(statistics);
        }
        // use CompareAndSet to sum
        // 将数据增加上去
        long[] current;
        long[] update = new long[LENGTH];
        do {
            current = reference.get();
            if (current == null) {
                update[0] = success;
                update[1] = failure;
                update[2] = input;
                update[3] = output;
                update[4] = elapsed;
                update[5] = concurrent;
                update[6] = input;
                update[7] = output;
                update[8] = elapsed;
                update[9] = concurrent;
            } else {
                //当前数据存在的情况下 增加数据
                update[0] = current[0] + success;
                update[1] = current[1] + failure;
                update[2] = current[2] + input;
                update[3] = current[3] + output;
                update[4] = current[4] + elapsed;
                update[5] = (current[5] + concurrent) / 2;
                update[6] = current[6] > input ? current[6] : input;
                update[7] = current[7] > output ? current[7] : output;
                update[8] = current[8] > elapsed ? current[8] : elapsed;
                update[9] = current[9] > concurrent ? current[9] : concurrent;
            }
        } while (!reference.compareAndSet(current, update));
    }

    //委托给动态代理类实现

    @Override
    public List<URL> lookup(URL query) {
        return monitorService.lookup(query);
    }

    @Override
    public URL getUrl() {
        return monitorInvoker.getUrl();
    }

    @Override
    public boolean isAvailable() {
        return monitorInvoker.isAvailable();
    }

    @Override
    public void destroy() {
        try {
            sendFuture.cancel(true);
        } catch (Throwable t) {
            logger.error("Unexpected error occur at cancel sender timer, cause: " + t.getMessage(), t);
        }
        monitorInvoker.destroy();
    }

}
