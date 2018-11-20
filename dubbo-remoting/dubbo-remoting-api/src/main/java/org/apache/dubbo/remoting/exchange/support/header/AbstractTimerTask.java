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

package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.remoting.Channel;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * AbstractTimerTask
 * 定时任务的 骨架类
 */
public abstract class AbstractTimerTask implements TimerTask {

    /**
     * 能够 获取 channel 组的 对象
     */
    private final ChannelProvider channelProvider;

    /**
     * 记录滴答了 几次
     */
    private final Long tick;

    /**
     * 创建 定时任务对象
     * @param channelProvider
     * @param tick
     */
    AbstractTimerTask(ChannelProvider channelProvider, Long tick) {
        if (channelProvider == null || tick == null) {
            throw new IllegalArgumentException();
        }
        this.tick = tick;
        this.channelProvider = channelProvider;
    }

    /**
     * 通过 指定的 key 从channel 中获取属性
     * @param channel
     * @return
     */
    static Long lastRead(Channel channel) {
        return (Long) channel.getAttribute(HeaderExchangeHandler.KEY_READ_TIMESTAMP);
    }

    /**
     * 通过 指定的 key 从channel 中获取属性
     * @param channel
     * @return
     */
    static Long lastWrite(Channel channel) {
        return (Long) channel.getAttribute(HeaderExchangeHandler.KEY_WRITE_TIMESTAMP);
    }

    static Long now() {
        return System.currentTimeMillis();
    }

    /**
     * 这个看来是 针对 单次 定时任务 追加 执行次数的 每次 完成后 设置下一次定时时间
     * @param timeout 该对象 能够获取到TimeTask对象 和 定时器对象 像是一个 中间类  类似 channelhandlercontext
     * @param tick
     */
    private void reput(Timeout timeout, Long tick) {
        if (timeout == null || tick == null) {
            throw new IllegalArgumentException();
        }

        //获取 timeout 类的定时器对象
        Timer timer = timeout.timer();
        //如果 定时器 已经停止 或者 被 关闭了就不进行操作
        if (timer.isStop() || timeout.isCancelled()) {
            return;
        }

        //该定时器 增加一个新的 定时任务
        timer.newTimeout(timeout.task(), tick, TimeUnit.MILLISECONDS);
    }

    /**
     * 默认的 执行逻辑
     * @param timeout a handle which is associated with this task
     * @throws Exception
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        //从 provider 中 获取 一个 channel 容器
        Collection<Channel> c = channelProvider.getChannels();
        for (Channel channel : c) {
            if (channel.isClosed()) {
                continue;
            }
            //执行每个 channel 对象
            doTask(channel);
        }
        //每 触发一次定时任务后 要 设置下一次触发时间
        reput(timeout, tick);
    }

    protected abstract void doTask(Channel channel);

    interface ChannelProvider {
        Collection<Channel> getChannels();
    }
}
