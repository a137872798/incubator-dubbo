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
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;

/**
 * LimitInvokerFilter
 *
 * limit 过滤器 应该是限流的 针对的是消费者 代表能允许的最大请求次数
 * 消费者端利用 内置锁来 完成限流 提供者端 通过信号量完成限流
 */
@Activate(group = Constants.CONSUMER, value = Constants.ACTIVES_KEY)
public class ActiveLimitFilter implements Filter {

    /**
     * 调用链上的方法
     * @param invoker    service
     * @param invocation invocation.
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //获取invoker 的 url 对象
        URL url = invoker.getUrl();
        //获取调用的方法
        String methodName = invocation.getMethodName();
        //从url 中 获取 最大活跃数量
        int max = invoker.getUrl().getMethodParameter(methodName, Constants.ACTIVES_KEY, 0);
        //通过 url 和 methodName 定位到具体的status 对象 如果没有就会延迟初始化
        RpcStatus count = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
        if (max > 0) {
            //获取 该方法的 超时时间属性
            long timeout = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.TIMEOUT_KEY, 0);
            //获取当前开始时间
            long start = System.currentTimeMillis();
            long remain = timeout;
            //获取当前的活跃量 也就是并发量
            int active = count.getActive();
            //超过最大活跃量
            if (active >= max) {
                synchronized (count) {
                    //当活跃数超过了 最大活跃数
                    while ((active = count.getActive()) >= max) {
                        try {
                            //等待 直到超时
                            count.wait(remain);
                        } catch (InterruptedException e) {
                        }
                        //获得 等待的时间
                        long elapsed = System.currentTimeMillis() - start;
                        //进一步 更新 距离超时剩余的时间
                        remain = timeout - elapsed;
                        //超时了
                        if (remain <= 0) {
                            throw new RpcException("Waiting concurrent invoke timeout in client-side for service:  "
                                    + invoker.getInterface().getName() + ", method: "
                                    + invocation.getMethodName() + ", elapsed: " + elapsed
                                    + ", timeout: " + timeout + ". concurrent invokes: " + active
                                    + ". max concurrent invoke limit: " + max);
                        }
                    }
                }
            }
        }
        try {
            //在能正常执行的 逻辑中
            long begin = System.currentTimeMillis();
            //开始增加活跃量
            RpcStatus.beginCount(url, methodName);
            try {
                Result result = invoker.invoke(invocation);
                //调用完后 减少活跃量
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, true);
                return result;
            } catch (RuntimeException t) {
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, false);
                throw t;
            }
        } finally {
            if (max > 0) {
                synchronized (count) {
                    //唤醒沉睡的 线程
                    count.notify();
                }
            }
        }
    }

}
