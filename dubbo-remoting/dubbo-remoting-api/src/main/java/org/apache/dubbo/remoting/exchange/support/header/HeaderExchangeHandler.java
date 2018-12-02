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
 * 基于消息头 的 信息交换类
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
     * 处理响应结果 将 future 从等待的容器中移除
     *
     * @param channel
     * @param response
     * @throws RemotingException
     */
    static void handleResponse(Channel channel, Response response) throws RemotingException {
        //非心跳响应结果 才有处理的必要
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
     * 处理远程端发来的  请求
     * @param channel
     * @param req
     * @throws RemotingException
     */
    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        //创建 返回的 响应结果对象
        Response res = new Response(req.getId(), req.getVersion());
        //如果 请求出现了异常
        if (req.isBroken()) {
            //获取请求的 data 对象
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
            // handle data.
            // 委托 进行处理 并且是一个 异步请求
            CompletableFuture<Object> future = handler.reply(channel, msg);
            //已完成就 返回结果
            if (future.isDone()) {
                res.setStatus(Response.OK);
                res.setResult(future.get());
                channel.send(res);
                return;
            }
            //设置 回调函数  传入的 应该是 正常结果 和 throwable
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
        //给传入的 channel 绑定一个 装饰该channel 的 exchangeChannel
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
     * 收到 数据时
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
                        handleRequest(exchangeChannel, request);
                    } else {
                        handler.received(exchangeChannel, request.getData());
                    }
                }
            } else if (message instanceof Response) {
                handleResponse(channel, (Response) message);
                //如果是 指令就解析指令
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
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                //需要返回结果  心跳包是不需要处理异常情况的
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
