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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.Parameters;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.ResponseFuture;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * dubbo protocol support class.
 * 应该是 失去部分功能的 client 对象
 */
@SuppressWarnings("deprecation")
final class LazyConnectExchangeClient implements ExchangeClient {

    // when this warning rises from invocation, program probably have bug.
    static final String REQUEST_WITH_WARNING_KEY = "lazyclient_request_with_warning";
    private final static Logger logger = LoggerFactory.getLogger(LazyConnectExchangeClient.class);
    protected final boolean requestWithWarning;
    private final URL url;
    /**
     * 包装的handler 对象
     */
    private final ExchangeHandler requestHandler;
    private final Lock connectLock = new ReentrantLock();
    // lazy connect, initial state for connection
    /**
     * 连接 还未创建 client == null 时返回的状态
     */
    private final boolean initialState;
    private volatile ExchangeClient client;
    /**
     * 警告次数
     */
    private AtomicLong warningcount = new AtomicLong(0);

    public LazyConnectExchangeClient(URL url, ExchangeHandler requestHandler) {
        // lazy connect, need set send.reconnect = true, to avoid channel bad status.
        this.url = url.addParameter(Constants.SEND_RECONNECT_KEY, Boolean.TRUE.toString());
        this.requestHandler = requestHandler;
        //通过ReferenceCountExchangeClient 初始化 是false
        this.initialState = url.getParameter(Constants.LAZY_CONNECT_INITIAL_STATE_KEY, Constants.DEFAULT_LAZY_CONNECT_INITIAL_STATE);
        //是否在请求时要发出警告
        this.requestWithWarning = url.getParameter(REQUEST_WITH_WARNING_KEY, false);
    }


    /**
     * 初始化client 对象
     * @throws RemotingException
     */
    private void initClient() throws RemotingException {
        if (client != null) {
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Lazy connect to " + url);
        }
        connectLock.lock();
        try {
            if (client != null) {
                return;
            }
            //延时创建client对象
            this.client = Exchangers.connect(url, requestHandler);
        } finally {
            connectLock.unlock();
        }
    }

    /**
     * 当发起请求时 才开始创建client
     * @param request
     * @return
     * @throws RemotingException
     */
    @Override
    public ResponseFuture request(Object request) throws RemotingException {
        warning(request);
        initClient();
        //委托 给 初始化的client对象执行request
        return client.request(request);
    }

    @Override
    public URL getUrl() {
        return url;
    }

    /**
     * 获取远程地址
     * @return
     */
    @Override
    public InetSocketAddress getRemoteAddress() {
        if (client == null) {
            return InetSocketAddress.createUnresolved(url.getHost(), url.getPort());
        } else {
            return client.getRemoteAddress();
        }
    }

    @Override
    public ResponseFuture request(Object request, int timeout) throws RemotingException {
        warning(request);
        initClient();
        return client.request(request, timeout);
    }

    /**
     * If {@link #REQUEST_WITH_WARNING_KEY} is configured, then warn once every 5000 invocations.
     *
     * 如果设置了 在发起请求时要 打印 日志就打印
     * @param request
     */
    private void warning(Object request) {
        if (requestWithWarning) {
            if (warningcount.get() % 5000 == 0) {
                logger.warn(new IllegalStateException("safe guard client , should not be called ,must have a bug."));
            }
            warningcount.incrementAndGet();
        }
    }

    @Override
    public ChannelHandler getChannelHandler() {
        checkClient();
        return client.getChannelHandler();
    }

    /**
     * 判断 当前 是否连接 结果会受到initialState影响
     * @return
     */
    @Override
    public boolean isConnected() {
        if (client == null) {
            return initialState;
        } else {
            return client.isConnected();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        if (client == null) {
            return InetSocketAddress.createUnresolved(NetUtils.getLocalHost(), 0);
        } else {
            return client.getLocalAddress();
        }
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return requestHandler;
    }

    @Override
    public void send(Object message) throws RemotingException {
        initClient();
        client.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        initClient();
        client.send(message, sent);
    }

    @Override
    public boolean isClosed() {
        if (client != null) {
            return client.isClosed();
        } else {
            return true;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void close(int timeout) {
        if (client != null) {
            client.close(timeout);
        }
    }

    @Override
    public void startClose() {
        if (client != null) {
            client.startClose();
        }
    }

    @Override
    public void reset(URL url) {
        checkClient();
        client.reset(url);
    }

    /**
     * 在 reset 前 将属性保存到 url 中
     * @param parameters
     */
    @Override
    @Deprecated
    public void reset(Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void reconnect() throws RemotingException {
        checkClient();
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        if (client == null) {
            return null;
        } else {
            return client.getAttribute(key);
        }
    }

    @Override
    public void setAttribute(String key, Object value) {
        checkClient();
        client.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        checkClient();
        client.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        if (client == null) {
            return false;
        } else {
            return client.hasAttribute(key);
        }
    }

    /**
     * 检查 client对象是否为null
     */
    private void checkClient() {
        if (client == null) {
            throw new IllegalStateException(
                    "LazyConnectExchangeClient state error. the client has not be init .url:" + url);
        }
    }
}
