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
package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;

import com.alibaba.fastjson.JSON;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 虚假方法 调用对象
 * @param <T>
 */
final public class MockInvoker<T> implements Invoker<T> {
    /**
     * 代理工厂
     */
    private final static ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    /**
     * 管理服务者 方法 对应的mock对象
     */
    private final static Map<String, Invoker<?>> mocks = new ConcurrentHashMap<String, Invoker<?>>();
    /**
     * 管理服务者 方法 对应的返回类型
     */
    private final static Map<String, Throwable> throwables = new ConcurrentHashMap<String, Throwable>();

    /**
     * 该对象的 资源定位符
     */
    private final URL url;

    public MockInvoker(URL url) {
        this.url = url;
    }

    /**
     * 解析给与的 mock 字符串
     * @param mock
     * @return
     * @throws Exception
     */
    public static Object parseMockValue(String mock) throws Exception {
        return parseMockValue(mock, null);
    }

    /**
     * 根据 mock 信息 获取对应的默认值
     * @param mock
     * @return 默认为null
     * @throws Exception
     */
    public static Object parseMockValue(String mock, Type[] returnTypes) throws Exception {
        Object value = null;
        if ("empty".equals(mock)) {
            //根据 这个mock 对象的 返回值类型 初始化该对象的所有字段 包括携带的 父类字段以及 类字段内的其他字段
            //如果是 null 或者 基本 类型直接返回默认值就好
            value = ReflectUtils.getEmptyObject(returnTypes != null && returnTypes.length > 0 ? (Class<?>) returnTypes[0] : null);
        } else if ("null".equals(mock)) {
            value = null;
        } else if ("true".equals(mock)) {
            value = true;
        } else if ("false".equals(mock)) {
            value = false;
            //如果是用\ 包裹 就去掉这层
        } else if (mock.length() >= 2 && (mock.startsWith("\"") && mock.endsWith("\"")
                || mock.startsWith("\'") && mock.endsWith("\'"))) {
            value = mock.subSequence(1, mock.length() - 1);
        } else if (returnTypes != null && returnTypes.length > 0 && returnTypes[0] == String.class) {
            value = mock;
            //是否是数字
        } else if (StringUtils.isNumeric(mock)) {
            value = JSON.parse(mock);
        } else if (mock.startsWith("{")) {
            value = JSON.parseObject(mock, Map.class);
        } else if (mock.startsWith("[")) {
            value = JSON.parseObject(mock, List.class);
        } else {
            value = mock;
        }
        //返回值 怎么有2个???
        if (returnTypes != null && returnTypes.length > 0) {
            value = PojoUtils.realize(value, (Class<?>) returnTypes[0], returnTypes.length > 1 ? returnTypes[1] : null);
        }
        return value;
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        String mock = getUrl().getParameter(invocation.getMethodName() + "." + Constants.MOCK_KEY);
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(this);
        }
        if (StringUtils.isBlank(mock)) {
            mock = getUrl().getParameter(Constants.MOCK_KEY);
        }

        if (StringUtils.isBlank(mock)) {
            throw new RpcException(new IllegalAccessException("mock can not be null. url :" + url));
        }
        mock = normalizeMock(URL.decode(mock));
        if (mock.startsWith(Constants.RETURN_PREFIX)) {
            mock = mock.substring(Constants.RETURN_PREFIX.length()).trim();
            try {
                Type[] returnTypes = RpcUtils.getReturnTypes(invocation);
                Object value = parseMockValue(mock, returnTypes);
                return new RpcResult(value);
            } catch (Exception ew) {
                throw new RpcException("mock return invoke error. method :" + invocation.getMethodName()
                        + ", mock:" + mock + ", url: " + url, ew);
            }
        } else if (mock.startsWith(Constants.THROW_PREFIX)) {
            mock = mock.substring(Constants.THROW_PREFIX.length()).trim();
            if (StringUtils.isBlank(mock)) {
                throw new RpcException("mocked exception for service degradation.");
            } else { // user customized class
                Throwable t = getThrowable(mock);
                throw new RpcException(RpcException.BIZ_EXCEPTION, t);
            }
        } else { //impl mock
            try {
                Invoker<T> invoker = getInvoker(mock);
                return invoker.invoke(invocation);
            } catch (Throwable t) {
                throw new RpcException("Failed to create mock implementation class " + mock, t);
            }
        }
    }

    /**
     * 获取异常对象
     * @param throwstr
     * @return
     */
    public static Throwable getThrowable(String throwstr) {
        //尝试从缓存获取
        Throwable throwable = throwables.get(throwstr);
        if (throwable != null) {
            return throwable;
        }

        try {
            Throwable t;
            //反射创建异常对象
            Class<?> bizException = ReflectUtils.forName(throwstr);
            Constructor<?> constructor;
            //获取到 该异常的 带string 的构造器
            constructor = ReflectUtils.findConstructor(bizException, String.class);
            //通过构造器创建对象
            t = (Throwable) constructor.newInstance(new Object[]{"mocked exception for service degradation."});
            if (throwables.size() < 1000) {
                throwables.put(throwstr, t);
            }
            return t;
        } catch (Exception e) {
            throw new RpcException("mock throw error :" + throwstr + " argument error.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Invoker<T> getInvoker(String mockService) {
        Invoker<T> invoker = (Invoker<T>) mocks.get(mockService);
        if (invoker != null) {
            return invoker;
        }

        Class<T> serviceType = (Class<T>) ReflectUtils.forName(url.getServiceInterface());
        T mockObject = (T) getMockObject(mockService, serviceType);
        invoker = proxyFactory.getInvoker(mockObject, serviceType, url);
        if (mocks.size() < 10000) {
            mocks.put(mockService, invoker);
        }
        return invoker;
    }

    /**
     *
     * @param mockService
     * @param serviceType
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Object getMockObject(String mockService, Class serviceType) {
        //mock 是否为 default||true
        if (ConfigUtils.isDefault(mockService)) {
            //设置成 defaultMock||trueMock
            mockService = serviceType.getName() + "Mock";
        }

        //反射 获取对象
        Class<?> mockClass = ReflectUtils.forName(mockService);
        //该对象必须实现 给定接口
        if (!serviceType.isAssignableFrom(mockClass)) {
            throw new IllegalStateException("The mock class " + mockClass.getName() +
                    " not implement interface " + serviceType.getName());
        }

        try {
            //返回mock 对象实例
            return mockClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException("No default constructor from mock class " + mockClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Normalize mock string:
     *
     * <ol>
     * <li>return => return null</li>
     * <li>fail => default</li>
     * <li>force => default</li>
     * <li>fail:throw/return foo => throw/return foo</li>
     * <li>force:throw/return foo => throw/return foo</li>
     * </ol>
     *
     * @param mock mock string
     * @return normalized mock string
     */
    //将mock 对象 做一些处理
    public static String normalizeMock(String mock) {
        if (mock == null) {
            return mock;
        }

        //去空
        mock = mock.trim();

        if (mock.length() == 0) {
            return mock;
        }

        //如果是 return 就改成 return null
        if (Constants.RETURN_KEY.equalsIgnoreCase(mock)) {
            return Constants.RETURN_PREFIX + "null";
        }

        if (ConfigUtils.isDefault(mock) || "fail".equalsIgnoreCase(mock) || "force".equalsIgnoreCase(mock)) {
            return "default";
        }

        //去除特定前缀
        if (mock.startsWith(Constants.FAIL_PREFIX)) {
            mock = mock.substring(Constants.FAIL_PREFIX.length()).trim();
        }

        if (mock.startsWith(Constants.FORCE_PREFIX)) {
            mock = mock.substring(Constants.FORCE_PREFIX.length()).trim();
        }

        if (mock.startsWith(Constants.RETURN_PREFIX) || mock.startsWith(Constants.THROW_PREFIX)) {
            mock = mock.replace('`', '"');
        }

        return mock;
    }

    @Override
    public URL getUrl() {
        return this.url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        //do nothing
    }

    @Override
    public Class<T> getInterface() {
        //FIXME
        return null;
    }
}
