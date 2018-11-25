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
package org.apache.dubbo.rpc.protocol.http;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.http.HttpBinder;
import org.apache.dubbo.remoting.http.HttpHandler;
import org.apache.dubbo.remoting.http.HttpServer;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.remoting.RemoteAccessException;
import org.springframework.remoting.httpinvoker.HttpComponentsHttpInvokerRequestExecutor;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;
import org.springframework.remoting.httpinvoker.SimpleHttpInvokerRequestExecutor;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HttpProtocol
 *
 * http协议
 */
public class HttpProtocol extends AbstractProxyProtocol {

    /**
     * 默认端口
     */
    public static final int DEFAULT_PORT = 80;

    /**
     * 服务器集合
     */
    private final Map<String, HttpServer> serverMap = new ConcurrentHashMap<String, HttpServer>();

    /**
     * value 是spring 中的 类  key 是url的 path 属性
     */
    private final Map<String, HttpInvokerServiceExporter> skeletonMap = new ConcurrentHashMap<String, HttpInvokerServiceExporter>();

    /**
     * HttpBinder 自适应对象
     */
    private HttpBinder httpBinder;

    public HttpProtocol() {
        //给父类添加指定的 异常类型
        super(RemoteAccessException.class);
    }

    public void setHttpBinder(HttpBinder httpBinder) {
        this.httpBinder = httpBinder;
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * 实现父类的 出口方法
     * @param impl
     * @param type
     * @param url
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    protected <T> Runnable doExport(final T impl, Class<T> type, URL url) throws RpcException {
        //通过url 获取地址信息 ip:port
        String addr = getAddr(url);
        //通过地址获取到对应的 Server
        HttpServer server = serverMap.get(addr);
        if (server == null) {
            //延迟初始化  绑定地址生成 服务对象
            server = httpBinder.bind(url, new InternalHandler());
            serverMap.put(addr, server);
        }
        //获取 url 的绝对路径 也就是 url 的path属性
        final String path = url.getAbsolutePath();
        //将上层传入的 impl封装成 export 添加到 容器中
        skeletonMap.put(path, createExporter(impl, type));

        //创建 泛型 path 就是 原path/generic
        final String genericPath = path + "/" + Constants.GENERIC_KEY;

        //创建泛化类型并保存
        skeletonMap.put(genericPath, createExporter(impl, GenericService.class));
        return new Runnable() {
            @Override
            public void run() {
                //这个 run 是在 unexport时执行的 就是从容器中移除相关属性
                skeletonMap.remove(path);
                skeletonMap.remove(genericPath);
            }
        };
    }

    /**
     * 返回 实现了 spring 类的  对象
     * @param impl
     * @param type
     * @param <T>
     * @return
     */
    private <T> HttpInvokerServiceExporter createExporter(T impl, Class<?> type) {
        //创建 Spring对象
        final HttpInvokerServiceExporter httpServiceExporter = new HttpInvokerServiceExporter();
        //设置 接口类型 和实现类
        httpServiceExporter.setServiceInterface(type);
        httpServiceExporter.setService(impl);
        try {
            httpServiceExporter.afterPropertiesSet();
        } catch (Exception e) {
            throw new RpcException(e.getMessage(), e);
        }
        return httpServiceExporter;
    }

    /**
     * 进行引用
     * @param serviceType
     * @param url
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings("unchecked")
    protected <T> T doRefer(final Class<T> serviceType, final URL url) throws RpcException {
        //获取 泛化信息  这里可能是null
        final String generic = url.getParameter(Constants.GENERIC_KEY);
        //判断是否是泛型类信息
        final boolean isGeneric = ProtocolUtils.isGeneric(generic) || serviceType.equals(GenericService.class);

        //spring 对象
        final HttpInvokerProxyFactoryBean httpProxyFactoryBean = new HttpInvokerProxyFactoryBean();
        //设置远程调用工厂
        httpProxyFactoryBean.setRemoteInvocationFactory(new RemoteInvocationFactory() {
            //创建远程调用对象
            @Override
            public RemoteInvocation createRemoteInvocation(MethodInvocation methodInvocation) {
                RemoteInvocation invocation = new HttpRemoteInvocation(methodInvocation);
                if (isGeneric) {
                    //给invoker 增加一个 generic 属性
                    invocation.addAttribute(Constants.GENERIC_KEY, generic);
                }
                return invocation;
            }
        });

        //相当于 获取url 的 字符串格式
        String key = url.toIdentityString();
        //如果是 泛化类型 就需要追加标识
        if (isGeneric) {
            key = key + "/" + Constants.GENERIC_KEY;
        }

        //设置url 和 接口类型
        httpProxyFactoryBean.setServiceUrl(key);
        httpProxyFactoryBean.setServiceInterface(serviceType);
        String client = url.getParameter(Constants.CLIENT_KEY);
        //如果是简单线程池
        if (client == null || client.length() == 0 || "simple".equals(client)) {
            //创建一个http请求线程池
            SimpleHttpInvokerRequestExecutor httpInvokerRequestExecutor = new SimpleHttpInvokerRequestExecutor() {
                //准备连接
                @Override
                protected void prepareConnection(HttpURLConnection con,
                                                 int contentLength) throws IOException {
                    super.prepareConnection(con, contentLength);
                    //为连接追加 设置2个值
                    con.setReadTimeout(url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT));
                    con.setConnectTimeout(url.getParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT));
                }
            };
            //设置线程池对象
            httpProxyFactoryBean.setHttpInvokerRequestExecutor(httpInvokerRequestExecutor);
            //如果是 公共客户端
        } else if ("commons".equals(client)) {
            //创建http请求 处理线程池
            HttpComponentsHttpInvokerRequestExecutor httpInvokerRequestExecutor = new HttpComponentsHttpInvokerRequestExecutor();
            httpInvokerRequestExecutor.setReadTimeout(url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT));
            httpInvokerRequestExecutor.setConnectTimeout(url.getParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT));
            httpProxyFactoryBean.setHttpInvokerRequestExecutor(httpInvokerRequestExecutor);
        } else {
            throw new IllegalStateException("Unsupported http protocol client " + client + ", only supported: simple, commons");
        }
        //这些操作都看不懂 先不管
        httpProxyFactoryBean.afterPropertiesSet();
        return (T) httpProxyFactoryBean.getObject();
    }

    /**
     * 转换异常类
     * @param e
     * @return
     */
    @Override
    protected int getErrorCode(Throwable e) {
        if (e instanceof RemoteAccessException) {
            e = e.getCause();
        }
        //其他异常类 根据类型返回 RPCException.code
        if (e != null) {
            Class<?> cls = e.getClass();
            if (SocketTimeoutException.class.equals(cls)) {
                return RpcException.TIMEOUT_EXCEPTION;
            } else if (IOException.class.isAssignableFrom(cls)) {
                return RpcException.NETWORK_EXCEPTION;
            } else if (ClassNotFoundException.class.isAssignableFrom(cls)) {
                return RpcException.SERIALIZATION_EXCEPTION;
            }
        }
        //都不是就 返回 父类的 异常 UNKNOWException
        return super.getErrorCode(e);
    }

    /**
     * httpHandler 内置的实现类
     */
    private class InternalHandler implements HttpHandler {

        //处理逻辑
        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            //获取  发起请求的远程客户端地址
            String uri = request.getRequestURI();
            //通过 path 获取 Spring 下的类对象
            HttpInvokerServiceExporter skeleton = skeletonMap.get(uri);
            //必须时 Post 请求
            if (!request.getMethod().equalsIgnoreCase("POST")) {
                response.setStatus(500);
            } else {
                //设置远程地址
                RpcContext.getContext().setRemoteAddress(request.getRemoteAddr(), request.getRemotePort());
                try {
                    //委托给 Spring 类来处理请求
                    skeleton.handleRequest(request, response);
                } catch (Throwable e) {
                    throw new ServletException(e);
                }
            }
        }

    }

}
