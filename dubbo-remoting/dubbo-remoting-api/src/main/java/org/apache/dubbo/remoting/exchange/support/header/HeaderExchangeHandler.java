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

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;


/**
 * ExchangeReceiver
 * 这个类是处理解码后的结果
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    /**
     * 获取最后的 读时间戳
     */
    public static String KEY_READ_TIMESTAMP = HeartbeatHandler.KEY_READ_TIMESTAMP;

    /**
     * 获取最后的 写时间戳
     */
    public static String KEY_WRITE_TIMESTAMP = HeartbeatHandler.KEY_WRITE_TIMESTAMP;

    /**
     * 组合 exchangeHandler 实现 相关功能
     */
    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    /**
     * 处理响应结果 因为在创建发送请求的 时候 就有一个 future对象被创建  future 是保存在同一台机器上的 发起前创建 收到结果处理
     *
     * @param channel
     * @param response
     * @throws RemotingException
     */
    static void handleResponse(Channel channel, Response response) throws RemotingException {
        //非心跳响应结果 才有处理的必要 而且心跳事件好像在 上层已经处理了
        if (response != null && !response.isHeartbeat()) {
            //告诉future对象 已经收到 了结果  这样调用future 的get 方法就能获取到结果
            DefaultFuture.received(channel, response);
        }
    }

    /**
     * 判断当前是 client or server
     * @param channel
     * @return
     */
    private static boolean isClientSide(Channel channel) {
        //从channel 中 获取远程地址
        InetSocketAddress address = channel.getRemoteAddress();
        //获取 相关参数
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    /**
     * 处理事件
     * @param channel
     * @param req
     * @throws RemotingException
     */
    void handlerEvent(Channel channel, Request req) throws RemotingException {
        //如果是 只读事件 目前 内置的 只有 心跳 和 只读 2种事件
        if (req.getData() != null && req.getData().equals(Request.READONLY_EVENT)) {
            //给 channel 增加一个 只读属性 代表 该客户端 不能在访问 该服务器了
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    /**
     * 双向请求代表需要返回res 对象
     * @param channel
     * @param req
     * @throws RemotingException
     */
    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        //创建 返回的 响应结果对象
        Response res = new Response(req.getId(), req.getVersion());
        //如果 请求出现了异常 也就是 解析该请求对象时  失败了
        if (req.isBroken()) {
            //获取broken 信息
            Object data = req.getData();

            String msg;
            if (data == null) {
                msg = null;
            } else if (data instanceof Throwable) {
                msg = StringUtils.toString((Throwable) data);
            } else {
                msg = data.toString();
            }
            //将data 中的 消息 抽取出来 设置到 响应对象的 错误消息中
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            //直接将 数据返回给 请求端
            channel.send(res);
            return;
        }
        // find handler by message class.
        // 正常请求的情况
        Object msg = req.getData();
        try {

            //通过保存的 channel 发送消息 就能做到异步发送消息了 即使现在处理请求的是 其他线程 也能 准确将结果返回

            // handle data.
            // 委托 进行处理 并且是一个 异步请求  reply 代表是一个 需要结果的请求  received 不需要返回结果
            CompletableFuture<Object> future = handler.reply(channel, msg);
            //当 上面是 同步情况下 会直接返回结果
            /** //调用invoker.invoke 返回结果对象
             Result result = invoker.invoke(inv);
             如果是异步 结果
            if (result instanceof AsyncRpcResult) {
                //当有结果时 返回
                return ((AsyncRpcResult) result).getResultFuture().thenApply(r -> (Object) r);
            } else {
                //非异步 result中包含已经完成的结果
                return CompletableFuture.completedFuture(result);
            }
            */
            if (future.isDone()) {
                res.setStatus(Response.OK);
                res.setResult(future.get());
                channel.send(res);
                return;
            }
            //创建 回调 当产生结果时 将结果返回  result 应该是结果 t 是异常
            future.whenComplete((result, t) -> {
                try {
                    if (t == null) {
                        res.setStatus(Response.OK);
                        res.setResult(result);
                    } else {
                        res.setStatus(Response.SERVICE_ERROR);
                        res.setErrorMessage(StringUtils.toString(t));
                    }
                    channel.send(res);
                } catch (RemotingException e) {
                    logger.warn("Send result to consumer failed, channel is " + channel + ", msg is " + e);
                } finally {
                    // HeaderExchangeChannel.removeChannelIfDisconnected(channel);
                }
            });
        } catch (Throwable e) {
            //出现异常了 返回 异常响应信息
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    /**
     * 连接完成时
     * @param channel channel.
     * @throws RemotingException
     */
    @Override
    public void connected(Channel channel) throws RemotingException {
        //连接完成时 初始化 写入时间 和 读取时间
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        //一旦创建连接 就 创建一个关联关系
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            //委托 到上层实现
            handler.connected(exchangeChannel);
        } finally {
            //如果 channel断开连接了 移除 channel 上的exchangeChannel
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    /**
     * 当断开连接时
     * @param channel channel.
     * @throws RemotingException
     */
    @Override
    public void disconnected(Channel channel) throws RemotingException {
        //清除 读取 和 写入的时间戳
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
        //基本跟连接方法一样
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            DefaultFuture.closeChannel(channel);
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    /**
     * 设置 最后的发送时间
     * @param channel channel.
     * @param message message.
     * @throws RemotingException
     */
    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            //设置 最后的 写入时间戳
            channel.setAttribute(KEY_WRITE_TIMESTAMP, System.currentTimeMillis());
            //获取 以该channel 创建的  exchangechannel
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            try {
                //委托 发送消息
                handler.sent(exchangeChannel, message);
            } finally {
                //判断是否需要断开连接
                HeaderExchangeChannel.removeChannelIfDisconnected(channel);
            }
        } catch (Throwable t) {
            exception = t;
        }
        //如果消息是请求模式
        if (message instanceof Request) {
            Request request = (Request) message;
            //更新 future 的发送时间
            DefaultFuture.sent(channel, request);
        }
        //写入 抛出异常时
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                        exception.getMessage(), exception);
            }
        }
    }

    /**
     * 收到 数据时  这里是最外层的 处理
     * @param channel channel.
     * @param message message.
     * @throws RemotingException
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        //更新 读的时间戳
        channel.setAttribute(KEY_READ_TIMESTAMP, System.currentTimeMillis());
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            //如果是请求类型
            if (message instanceof Request) {
                // handle request.
                Request request = (Request) message;
                //如果是事件 就处理
                if (request.isEvent()) {
                    handlerEvent(channel, request);
                } else {
                    if (request.isTwoWay()) {
                        //双向请求时
                        handleRequest(exchangeChannel, request);
                    } else {
                        //单向请求直接 委托上层
                        handler.received(exchangeChannel, request.getData());
                    }
                }
            } else if (message instanceof Response) {
                handleResponse(channel, (Response) message);
                //如果是 指令就处理指令 这个是下层的编解码器产生的
            } else if (message instanceof String) {
                //客户端 不支持 telnet 指令
                if (isClientSide(channel)) {
                    Exception e = new Exception("Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: " + channel.getUrl());
                    logger.error(e.getMessage(), e);
                } else {
                    String echo = handler.telnet(channel, (String) message);
                    if (echo != null && echo.length() > 0) {
                        channel.send(echo);
                    }
                }
            } else {
                handler.received(exchangeChannel, message);
            }
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    /**
     * 处理收到的异常
     * @param channel   channel.
     * @param exception exception.
     * @throws RemotingException
     */
    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        //ExecutionException 类型异常在这层处理 否则 抛到上层
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                //需要返回结果  心跳包应该在上层做处理了
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
