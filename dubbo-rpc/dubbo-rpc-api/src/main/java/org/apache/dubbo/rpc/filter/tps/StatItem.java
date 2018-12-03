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
package org.apache.dubbo.rpc.filter.tps;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于限流的 统计信息对象
 */
class StatItem {

    /**
     * 服务名
     */
    private String name;

    /**
     * 最后重置时间
     */
    private long lastResetTime;

    /**
     * TPS的单位时间
     */
    private long interval;

    /**
     * 单位时间内的 临时计数器
     */
    private AtomicInteger token;

    /**
     * 单位时间内 最大请求数
     */
    private int rate;

    StatItem(String name, int rate, long interval) {
        this.name = name;
        this.rate = rate;
        this.interval = interval;
        this.lastResetTime = System.currentTimeMillis();
        this.token = new AtomicInteger(rate);
    }

    /**
     * 判断是否允许
     * @return
     */
    public boolean isAllowable() {
        long now = System.currentTimeMillis();
        //代表需要重置了
        if (now > lastResetTime + interval) {
            //重新设置成 单位时间内最大 访问量
            token.set(rate);
            lastResetTime = now;
        }

        //每执行一次 都将 本次 单位时间内的 允许访问量-1
        int value = token.get();
        boolean flag = false;
        while (value > 0 && !flag) {
            //直到修改成功为止 就是将 value - 1
            flag = token.compareAndSet(value, value - 1);
            value = token.get();
        }

        //当0时 就是失败了 代表TPS 达到上限 本次单位时间不能再访问了
        return flag;
    }

    long getLastResetTime() {
        return lastResetTime;
    }

    int getToken() {
        return token.get();
    }

    @Override
    public String toString() {
        return new StringBuilder(32).append("StatItem ")
                .append("[name=").append(name).append(", ")
                .append("rate = ").append(rate).append(", ")
                .append("interval = ").append(interval).append("]")
                .toString();
    }

}
