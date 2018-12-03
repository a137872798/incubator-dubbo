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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporters;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchanger;
import org.apache.dubbo.remoting.transport.DecodeHandler;

/**
 * DefaultMessenger
 *
 * 默认的 交换器实现
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    //handler 的包装顺序应该是 HeaderExchangeHandler -> DecodeHandler -> Dispatcher -> HeartbeatHandler -> MultiMessageHandler
    //调用顺序是 反过来 并且 这个handler 是在编解码后 的

    /**
     * 多层包装handler 后生成 client对象
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        //通过 装饰器模式层层包装 将传入的 handler 包装成 headerexchangehandler 在封装成解码对象 在连接到（url 和解码对象）返回client对象
        return new HeaderExchangeClient(Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))), true);
    }

    /**
     * 绑定生成 服务器对象
     * @param url 包含要绑定的 信息
     * @param handler 处理请求的 对象
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        //Transporters.bind 返回一个 transporter接口的自适应对象
        return new HeaderExchangeServer(Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

}
