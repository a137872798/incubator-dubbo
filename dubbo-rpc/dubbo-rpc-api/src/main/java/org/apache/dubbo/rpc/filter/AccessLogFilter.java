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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;

import com.alibaba.fastjson.JSON;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Record access log for the service.
 * <p>
 * Logger key is <code><b>dubbo.accesslog</b></code>.
 * In order to configure access log appear in the specified appender only, additivity need to be configured in log4j's
 * config file, for example:
 * <code>
 * <pre>
 * &lt;logger name="<b>dubbo.accesslog</b>" <font color="red">additivity="false"</font>&gt;
 *    &lt;level value="info" /&gt;
 *    &lt;appender-ref ref="foo" /&gt;
 * &lt;/logger&gt;
 * </pre></code>
 *
 *
 * 打印日志的内容一般是 时间地址 调用方法 参数
 */
@Activate(group = Constants.PROVIDER, value = Constants.ACCESS_LOG_KEY)
public class AccessLogFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

    /**
     * 日志属性在 url中的key 通过该key 去url 中找 日志 名
     */
    private static final String ACCESS_LOG_KEY = "dubbo.accesslog";

    /**
     * 文件的时间格式
     */
    private static final String FILE_DATE_FORMAT = "yyyyMMdd";

    /**
     * 消息时间格式
     */
    private static final String MESSAGE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 最大缓冲区
     */
    private static final int LOG_MAX_BUFFER = 5000;

    /**
     * 日志输出间隔
     */
    private static final long LOG_OUTPUT_INTERVAL = 5000;

    /**
     * 日志队列 key 是日志名 也可以看作是日志文件名 value 是对应的 日志队列
     */
    private final ConcurrentMap<String, Set<String>> logQueue = new ConcurrentHashMap<String, Set<String>>();

    /**
     * 定时器对象
     */
    private final ScheduledExecutorService logScheduled = Executors.newScheduledThreadPool(2, new NamedThreadFactory("Dubbo-Access-Log", true));

    /**
     * 定时任务 future
     */
    private volatile ScheduledFuture<?> logFuture = null;

    /**
     * 初始化方法
     */
    private void init() {
        if (logFuture == null) {
            synchronized (logScheduled) {
                if (logFuture == null) {
                    //启动定时任务 传入了一个 LogTask 封装任务的实际内容
                    logFuture = logScheduled.scheduleWithFixedDelay(new LogTask(), LOG_OUTPUT_INTERVAL, LOG_OUTPUT_INTERVAL, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    /**
     * 将消息 通过 key 保存到对应的日志队列中
     * @param accesslog
     * @param logmessage
     */
    private void log(String accesslog, String logmessage) {
        init();
        //通过key 获取对应的 队列
        Set<String> logSet = logQueue.get(accesslog);
        if (logSet == null) {
            //延迟初始化 设置 该文件名对应的 队列
            logQueue.putIfAbsent(accesslog, new ConcurrentHashSet<String>());
            logSet = logQueue.get(accesslog);
        }
        //只要小于最大缓冲就能添加新的消息
        if (logSet.size() < LOG_MAX_BUFFER) {
            logSet.add(logmessage);
        }
    }

    /**
     * 调用方法
     * @param invoker    service
     * @param inv
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        try {
            //通过日志键 获取到 日志的key
            String accesslog = invoker.getUrl().getParameter(Constants.ACCESS_LOG_KEY);
            if (ConfigUtils.isNotEmpty(accesslog)) {
                //获取上下文对象
                RpcContext context = RpcContext.getContext();
                //获取服务接口名
                String serviceName = invoker.getInterface().getName();
                //获取版本和 group
                String version = invoker.getUrl().getParameter(Constants.VERSION_KEY);
                String group = invoker.getUrl().getParameter(Constants.GROUP_KEY);
                StringBuilder sn = new StringBuilder();
                //将当前日志和 ip信息保存
                sn.append("[").append(new SimpleDateFormat(MESSAGE_DATE_FORMAT).format(new Date())).append("] ").append(context.getRemoteHost()).append(":").append(context.getRemotePort())
                        .append(" -> ").append(context.getLocalHost()).append(":").append(context.getLocalPort())
                        .append(" - ");
                //设置group 名
                if (null != group && group.length() > 0) {
                    sn.append(group).append("/");
                }
                //增加接口和版本
                sn.append(serviceName);
                if (null != version && version.length() > 0) {
                    sn.append(":").append(version);
                }
                //追加方法名信息
                sn.append(" ");
                sn.append(inv.getMethodName());
                sn.append("(");
                //追加参数类型信息
                Class<?>[] types = inv.getParameterTypes();
                if (types != null && types.length > 0) {
                    boolean first = true;
                    for (Class<?> type : types) {
                        if (first) {
                            first = false;
                        } else {
                            sn.append(",");
                        }
                        sn.append(type.getName());
                    }
                }
                sn.append(") ");
                //追加参数信息 这里做了 json 序列化
                Object[] args = inv.getArguments();
                if (args != null && args.length > 0) {
                    sn.append(JSON.toJSONString(args));
                }
                String msg = sn.toString();
                //使用了默认的 日志信息
                if (ConfigUtils.isDefault(accesslog)) {
                    LoggerFactory.getLogger(ACCESS_LOG_KEY + "." + invoker.getInterface().getName()).info(msg);
                } else {
                    //不是默认的 或者没有设置 都是将日志保存到消息队列
                    log(accesslog, msg);
                }
            }
        } catch (Throwable t) {
            logger.warn("Exception in AcessLogFilter of service(" + invoker + " -> " + inv + ")", t);
        }
        //继续走调用链
        return invoker.invoke(inv);
    }

    /**
     * 应该是定期从队列中将日志信息 打印 或写在什么位置  队列中的文件是什么时候被创建的
     */
    private class LogTask implements Runnable {
        @Override
        public void run() {
            try {
                //当队列中存在元素时
                if (logQueue != null && logQueue.size() > 0) {
                    for (Map.Entry<String, Set<String>> entry : logQueue.entrySet()) {
                        try {
                            //分别获取 日志键 和 队列对象
                            String accesslog = entry.getKey();
                            Set<String> logSet = entry.getValue();
                            //根据key 获取对应的文件
                            File file = new File(accesslog);
                            //parentFile 就是上一级路径
                            File dir = file.getParentFile();
                            //先创建父文件
                            if (null != dir && !dir.exists()) {
                                //创建文件夹同时将需要的父文件夹一起创建  createNewFile 是创建新文件
                                dir.mkdirs();
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("Append log to " + accesslog);
                            }
                            //在文件存在的情况下
                            if (file.exists()) {
                                //获取当前时间 和 文件的最后修改时间
                                String now = new SimpleDateFormat(FILE_DATE_FORMAT).format(new Date());
                                String last = new SimpleDateFormat(FILE_DATE_FORMAT).format(new Date(file.lastModified()));
                                //这里是 是否要进行改名
                                if (!now.equals(last)) {
                                    //根据时间来创建新的 文件
                                    File archive = new File(file.getAbsolutePath() + "." + last);
                                    //重命名 但是文件的内容还在  文件是不能直接改名的 必须通过创建一个新的file对象然后进行改名
                                    file.renameTo(archive);
                                }
                            }
                            //获取文件输出流
                            FileWriter writer = new FileWriter(file, true);
                            try {
                                //遍历 set对象 每次都将本元素移除
                                for (Iterator<String> iterator = logSet.iterator();
                                     iterator.hasNext();
                                     iterator.remove()) {
                                    //输入内容加换行
                                    writer.write(iterator.next());
                                    writer.write("\r\n");
                                }
                                writer.flush();
                            } finally {
                                writer.close();
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
