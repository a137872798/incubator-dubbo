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
package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.ResponseCallback;
import org.apache.dubbo.remoting.exchange.ResponseFuture;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DefaultFuture.
 * 默认的 future对象
 */
public class DefaultFuture implements ResponseFuture {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFuture.class);

    /**
     * key 应该是请求的 唯一标识  channel 是针对本次请求的  channel 对象
     */
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<>();

    /**
     * 将请求 标识 与 future 对象关联起来
     */
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<>();

    /**
     * 使用时间轮 处理定时任务 这个类的实现先不看了
     */
    public static final Timer TIME_OUT_TIMER = new HashedWheelTimer(
            new NamedThreadFactory("dubbo-future-timeout", true),
            30,
            TimeUnit.MILLISECONDS);

    // invoke id.
    /**
     * 请求id 也就是要存放到容器中的值
     */
    private final long id;
    /**
     * 该future对象绑定的  channel
     */
    private final Channel channel;
    /**
     * 发起该次 invoker 的 请求对象
     */
    private final Request request;
    /**
     * 请求的超时时间
     */
    private final int timeout;
    private final Lock lock = new ReentrantLock();
    private final Condition done = lock.newCondition();
    /**
     * 记录 该 future 的创建时间
     */
    private final long start = System.currentTimeMillis();
    /**
     * 请求 的发起时间
     */
    private volatile long sent;
    /**
     * 返回的结果对象
     */
    private volatile Response response;
    /**
     * 执行的回调
     */
    private volatile ResponseCallback callback;

    private DefaultFuture(Channel channel, Request request, int timeout) {
        this.channel = channel;
        this.request = request;
        //是一个AtomicLong 对象 每次请求该值都会增加 当变成负数 时 能继续作为id 在传入请求对象的时候 已经携带了 id
        this.id = request.getId();
        //如果存在超时时间 就设置 否则就从配置中获取 默认的 超时时间
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        // put into waiting map.
        //将该请求id  与 channel 和 future 对象绑定起来
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    /**
     * check time out of the future
     * 检查是否超时
     */
    private static void timeoutCheck(DefaultFuture future) {
        //创建一个 定时任务 对象 用来 管理 过时的 future 对象
        TimeoutCheckTask task = new TimeoutCheckTask(future);
        //触发时间 就是 超时时间
        TIME_OUT_TIMER.newTimeout(task, future.getTimeout(), TimeUnit.MILLISECONDS);
    }

    /**
     * init a DefaultFuture
     * 1.init a DefaultFuture
     * 2.timeout check
     *
     * 使用该静态方法的 好处是在创建对象后 会对该任务设定 超时检查
     * @param channel channel
     * @param request the request
     * @param timeout timeout
     * @return a new DefaultFuture
     */
    public static DefaultFuture newFuture(Channel channel, Request request, int timeout) {
        final DefaultFuture future = new DefaultFuture(channel, request, timeout);
        // timeout check
        timeoutCheck(future);
        return future;
    }

    /**
     * 通过 请求 id 获取 future对象
     * @param id
     * @return
     */
    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    /**
     * 全局future 容器中是否存在 该 channel
     * @param channel
     * @return
     */
    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    /**
     * 在请求发送后 调用  通过请求的id 获取到future 对象
     * @param channel
     * @param request
     */
    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    /**
     * close a channel when a channel is inactive
     * directly return the unfinished requests.
     *
     * 关闭channel 对象  当channel 停止 活跃的时候直接返回未结束的 future对象
     * @param channel channel to close
     */
    public static void closeChannel(Channel channel) {
        for (long id : CHANNELS.keySet()) {
            //通过 遍历 channel 的方式获取到 channel 的 id
            if (channel.equals(CHANNELS.get(id))) {
                //通过id 获取 future
                DefaultFuture future = getFuture(id);
                //如果 该future对象还没有 完成  如果对象已经完成就 不用做其他操作了
                if (future != null && !future.isDone()) {
                    //将未完成的 future 设置到 响应对象中 并打上 inactive 的标识
                    Response disconnectResponse = new Response(future.getId());
                    disconnectResponse.setStatus(Response.CHANNEL_INACTIVE);
                    disconnectResponse.setErrorMessage("Channel " +
                            channel +
                            " is inactive. Directly return the unFinished request : " +
                            future.getRequest());
                    DefaultFuture.received(channel, disconnectResponse);
                }
            }
        }
    }

    /**
     * 当收到结果后
     * @param channel
     * @param response
     */
    public static void received(Channel channel, Response response) {
        try {
            //从 容器中 移除该请求对象
            DefaultFuture future = FUTURES.remove(response.getId());
            if (future != null) {
                //执行 接受方法 就是通过 done 唤醒阻塞线程
                future.doReceived(response);
            } else {
                logger.warn("The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response " + response
                        + (channel == null ? "" : ", channel: " + channel.getLocalAddress()
                        + " -> " + channel.getRemoteAddress()));
            }
        } finally {
            //将 本次请求 和channel 的关联关系移除
            CHANNELS.remove(response.getId());
        }
    }

    /**
     * 从 future 对象上 获取结果
     * @return
     * @throws RemotingException
     */
    @Override
    public Object get() throws RemotingException {
        return get(timeout);
    }

    /**
     * 在指定时间内 获取 future 的结果
     * @param timeout
     * @return
     * @throws RemotingException
     */
    @Override
    public Object get(int timeout) throws RemotingException {
        //如果传入的 参数是负数 就使用默认的超时时间
        if (timeout <= 0) {
            timeout = Constants.DEFAULT_TIMEOUT;
        }
        //如果还没有完成
        if (!isDone()) {
            long start = System.currentTimeMillis();
            lock.lock();
            try {
                //避免多个线程 调用 get 通过 condition 实现 条件等待 调用await的线程 会 沉睡 并释放锁 一旦执行完 应该会在一个地方唤醒
                while (!isDone()) {
                    done.await(timeout, TimeUnit.MILLISECONDS);
                    //一旦被唤醒 先判断 任务是否完成 或者是否超时  那么 超时应该也会 唤醒线程
                    if (isDone() || System.currentTimeMillis() - start > timeout) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
            if (!isDone()) {
                throw new TimeoutException(sent > 0, channel, getTimeoutMessage(false));
            }
        }
        //结束 就从 future 对象中 获取 结果
        return returnFromResponse();
    }

    /**
     * 关闭 本次future
     */
    public void cancel() {
        //创建一个 使用本次请求标识的 响应对象 并设置 错误信息 返回
        Response errorResult = new Response(id);
        errorResult.setErrorMessage("request future has been canceled.");
        response = errorResult;
        FUTURES.remove(id);
        CHANNELS.remove(id);
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    /**
     * 设置 回调对象
     * @param callback
     */
    @Override
    public void setCallback(ResponseCallback callback) {
        //如果 future 完成 直接触发回调对象
        if (isDone()) {
            invokeCallback(callback);
        } else {
            boolean isdone = false;
            //这里 加锁 应该是 保证在 设置回调的时候不能调用get方法
            lock.lock();
            try {
                if (!isDone()) {
                    this.callback = callback;
                } else {
                    isdone = true;
                }
            } finally {
                lock.unlock();
            }
            //最后 判断 如果 已经 完成 触发回调
            if (isdone) {
                invokeCallback(callback);
            }
        }
    }

    /**
     * 存放于 时间轮对象的 任务
     */
    private static class TimeoutCheckTask implements TimerTask {

        /**
         * 内部维护一个future 对象
         */
        private DefaultFuture future;

        TimeoutCheckTask(DefaultFuture future) {
            this.future = future;
        }

        /**
         * 启动任务时 触发的逻辑
         * @param timeout a handle which is associated with this task
         */
        @Override
        public void run(Timeout timeout) {
            //检查当前任务是否完成
            if (future == null || future.isDone()) {
                return;
            }
            // create exception response.
            //创建了一个 响应对象
            Response timeoutResponse = new Response(future.getId());
            // set timeout status.
            //根据 是否已经发送成功 返回 服务超时 或 客户端超时 这个 定时器的 触发时间应该就是超时时间  发送成功 没有收到结果就是客户端 没有及时响应
            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
            // handle response.
            DefaultFuture.received(future.getChannel(), timeoutResponse);

        }
    }

    /**
     * 触发 回调函数
     * @param c
     */
    private void invokeCallback(ResponseCallback c) {
        //获取 回调函数对象
        ResponseCallback callbackCopy = c;
        if (callbackCopy == null) {
            throw new NullPointerException("callback cannot be null.");
        }
        c = null;
        //触发回调的时候 这个对象应该已经被初始化
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null. url:" + channel.getUrl());
        }

        //如果 返回的 状态是 正常的 就 触发 正常 执行完的 回调函数
        if (res.getStatus() == Response.OK) {
            try {
                callbackCopy.done(res.getResult());
            } catch (Exception e) {
                logger.error("callback invoke error .reasult:" + res.getResult() + ",url:" + channel.getUrl(), e);
            }
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            try {
                //返回超时异常对象 并传给 回调对象
                TimeoutException te = new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
                callbackCopy.caught(te);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        } else {
            try {
                RuntimeException re = new RuntimeException(res.getErrorMessage());
                callbackCopy.caught(re);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        }
    }

    /**
     * 当future 对象完成时 调用get方法 就从这里 获取结果
     * @return
     * @throws RemotingException
     */
    private Object returnFromResponse() throws RemotingException {
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }
        //正常 处理 就直接返回对象
        if (res.getStatus() == Response.OK) {
            return res.getResult();
        }
        //异常就创建异常对象并返回
        if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            throw new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
        }
        throw new RemotingException(channel, res.getErrorMessage());
    }

    private long getId() {
        return id;
    }

    private Channel getChannel() {
        return channel;
    }

    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private long getStartTimestamp() {
        return start;
    }

    /**
     * 更新当前 发送到达的 时间
     */
    private void doSent() {
        sent = System.currentTimeMillis();
    }

    /**
     * 当 收到 结果时 唤醒 放弃锁并沉睡的线程
     * @param res
     */
    private void doReceived(Response res) {
        lock.lock();
        try {
            response = res;
            if (done != null) {
                done.signal();
            }
        } finally {
            lock.unlock();
        }
        if (callback != null) {
            invokeCallback(callback);
        }
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                + (scan ? " by scan timer" : "") + ". start time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + ","
                + (sent > 0 ? " client elapsed: " + (sent - start)
                + " ms, server elapsed: " + (nowTimestamp - sent)
                : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                + timeout + " ms, request: " + request + ", channel: " + channel.getLocalAddress()
                + " -> " + channel.getRemoteAddress();
    }
}
