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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorFactory;
import org.apache.dubbo.monitor.MonitorService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractMonitorFactory. (SPI, Singleton, ThreadSafe)
 * 监控中心骨架类
 */
public abstract class AbstractMonitorFactory implements MonitorFactory {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMonitorFactory.class);

    // lock for getting monitor center
    private static final ReentrantLock LOCK = new ReentrantLock();

    // monitor centers Map<RegistryAddress, Registry>
    private static final Map<String, Monitor> MONITORS = new ConcurrentHashMap<String, Monitor>();

    /**
     * 统计的 结果
     */
    private static final Map<String, CompletableFuture<Monitor>> FUTURES = new ConcurrentHashMap<String, CompletableFuture<Monitor>>();

    /**
     * 只能处理单个任务的线程池
     */
    private static final ExecutorService executor = new ThreadPoolExecutor(0, 10, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new NamedThreadFactory("DubboMonitorCreator", true));

    public static Collection<Monitor> getMonitors() {
        return Collections.unmodifiableCollection(MONITORS.values());
    }

    /**
     * 获取 监控中心对象
     * @param url
     * @return
     */
    @Override
    public Monitor getMonitor(URL url) {
        //设置 监控中心为 path
        url = url.setPath(MonitorService.class.getName()).addParameter(Constants.INTERFACE_KEY, MonitorService.class.getName());
        //将url 转换成key
        String key = url.toServiceStringWithoutResolving();
        //获取 全局 监控对象
        Monitor monitor = MONITORS.get(key);
        //这里是代表正在创建 监控中心
        Future<Monitor> future = FUTURES.get(key);
        if (monitor != null || future != null) {
            return monitor;
        }

        LOCK.lock();
        try {
            monitor = MONITORS.get(key);
            future = FUTURES.get(key);
            if (monitor != null || future != null) {
                return monitor;
            }

            final URL monitorUrl = url;
            //创建监控中心对象
            final CompletableFuture<Monitor> completableFuture = CompletableFuture.supplyAsync(() -> AbstractMonitorFactory.this.createMonitor(monitorUrl));
            //当执行完时 触发监听器
            completableFuture.thenRunAsync(new MonitorListener(key), executor);
            FUTURES.put(key, completableFuture);

            return null;
        } finally {
            // unlock
            LOCK.unlock();
        }
    }

    protected abstract Monitor createMonitor(URL url);


    class MonitorListener implements Runnable {

        private String key;

        public MonitorListener(String key) {
            this.key = key;
        }

        @Override
        public void run() {
            try {
                //获取future 对象
                CompletableFuture<Monitor> completableFuture = AbstractMonitorFactory.FUTURES.get(key);
                //保存结果
                AbstractMonitorFactory.MONITORS.put(key, completableFuture.get());
                AbstractMonitorFactory.FUTURES.remove(key);
            } catch (InterruptedException e) {
                logger.warn("Thread was interrupted unexpectedly, monitor will never be got.");
                AbstractMonitorFactory.FUTURES.remove(key);
            } catch (ExecutionException e) {
                logger.warn("Create monitor failed, monitor data will not be collected until you fix this problem. ", e);
            }
        }
    }

}
