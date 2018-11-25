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
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcInvocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * callback service helper
 */
class CallbackServiceCodec {
    private static final Logger logger = LoggerFactory.getLogger(CallbackServiceCodec.class);

    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    private static final DubboProtocol protocol = DubboProtocol.getDubboProtocol();
    private static final byte CALLBACK_NONE = 0x0;
    private static final byte CALLBACK_CREATE = 0x1;
    private static final byte CALLBACK_DESTROY = 0x2;
    private static final String INV_ATT_CALLBACK_KEY = "sys_callback_arg-";

    /**
     * 判断是否是 回调
     * @param url  invoker url
     * @param methodName invoker 方法名
     * @param argIndex  参数下标
     * @return
     */
    private static byte isCallBack(URL url, String methodName, int argIndex) {
        // parameter callback rule: method-name.parameter-index(starting from 0).callback
        // 默认没有 回调
        byte isCallback = CALLBACK_NONE;
        if (url != null) {
            //从 方法名.下标.callback 中获取 对应的下标属性
            String callback = url.getParameter(methodName + "." + argIndex + ".callback");
            //回调属性不为空时
            if (callback != null) {
                //根据 Boolean标识 返回结果
                if (callback.equalsIgnoreCase("true")) {
                    isCallback = CALLBACK_CREATE;
                } else if (callback.equalsIgnoreCase("false")) {
                    isCallback = CALLBACK_DESTROY;
                }
            }
        }
        return isCallback;
    }

    /**
     * export or unexport callback service on client side
     *
     * @param channel
     * @param url
     * @param clazz
     * @param inst
     * @param export
     * @throws IOException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static String exportOrunexportCallbackService(Channel channel, URL url, Class clazz, Object inst, Boolean export) throws IOException {
        int instid = System.identityHashCode(inst);

        Map<String, String> params = new HashMap<String, String>(3);
        // no need to new client again
        params.put(Constants.IS_SERVER_KEY, Boolean.FALSE.toString());
        // mark it's a callback, for troubleshooting
        params.put(Constants.IS_CALLBACK_SERVICE, Boolean.TRUE.toString());
        String group = url.getParameter(Constants.GROUP_KEY);
        if (group != null && group.length() > 0) {
            params.put(Constants.GROUP_KEY, group);
        }
        // add method, for verifying against method, automatic fallback (see dubbo protocol)
        params.put(Constants.METHODS_KEY, StringUtils.join(Wrapper.getWrapper(clazz).getDeclaredMethodNames(), ","));

        Map<String, String> tmpmap = new HashMap<String, String>(url.getParameters());
        tmpmap.putAll(params);
        tmpmap.remove(Constants.VERSION_KEY);// doesn't need to distinguish version for callback
        tmpmap.put(Constants.INTERFACE_KEY, clazz.getName());
        URL exporturl = new URL(DubboProtocol.NAME, channel.getLocalAddress().getAddress().getHostAddress(), channel.getLocalAddress().getPort(), clazz.getName() + "." + instid, tmpmap);

        // no need to generate multiple exporters for different channel in the same JVM, cache key cannot collide.
        String cacheKey = getClientSideCallbackServiceCacheKey(instid);
        String countkey = getClientSideCountKey(clazz.getName());
        if (export) {
            // one channel can have multiple callback instances, no need to re-export for different instance.
            if (!channel.hasAttribute(cacheKey)) {
                if (!isInstancesOverLimit(channel, url, clazz.getName(), instid, false)) {
                    Invoker<?> invoker = proxyFactory.getInvoker(inst, clazz, exporturl);
                    // should destroy resource?
                    Exporter<?> exporter = protocol.export(invoker);
                    // this is used for tracing if instid has published service or not.
                    channel.setAttribute(cacheKey, exporter);
                    logger.info("export a callback service :" + exporturl + ", on " + channel + ", url is: " + url);
                    increaseInstanceCount(channel, countkey);
                }
            }
        } else {
            if (channel.hasAttribute(cacheKey)) {
                Exporter<?> exporter = (Exporter<?>) channel.getAttribute(cacheKey);
                exporter.unexport();
                channel.removeAttribute(cacheKey);
                decreaseInstanceCount(channel, countkey);
            }
        }
        return String.valueOf(instid);
    }

    /**
     * refer or destroy callback service on server side
     *
     * 引用或销毁 回调服务
     * @param channel
     * @param url
     * @param clazz
     * @param inv
     * @param instid  通过invoker 对象中的回调属性配上对应的参数下标作为key 获得
     * @param isRefer 引用还是销毁
     * @return
     */
    @SuppressWarnings("unchecked")
    private static Object referOrdestroyCallbackService(Channel channel, URL url, Class<?> clazz, Invocation inv, int instid, boolean isRefer) {
        Object proxy = null;
        //获取服务端 和 调用端的 缓存key  就是拼接 返回了一个 特殊的字符串
        String invokerCacheKey = getServerSideCallbackInvokerCacheKey(channel, clazz.getName(), instid);
        String proxyCacheKey = getServerSideCallbackServiceCacheKey(channel, clazz.getName(), instid);
        //通过key 获取代理对象
        proxy = channel.getAttribute(proxyCacheKey);
        //获取 特殊的key
        String countkey = getServerSideCountKey(channel, clazz.getName());
        if (isRefer) {
            if (proxy == null) {
                //创建了一个 回调地址
                URL referurl = URL.valueOf("callback://" + url.getAddress() + "/" + clazz.getName() + "?" + Constants.INTERFACE_KEY + "=" + clazz.getName());
                //将所有属性传入 并移除了 methods 属性
                referurl = referurl.addParametersIfAbsent(url.getParameters()).removeParameter(Constants.METHODS_KEY);
                //如果对象没有超过限制
                if (!isInstancesOverLimit(channel, referurl, clazz.getName(), instid, true)) {
                    @SuppressWarnings("rawtypes")
                    //生成 channel 的包装类
                    Invoker<?> invoker = new ChannelWrappedInvoker(clazz, channel, referurl, String.valueOf(instid));
                    proxy = proxyFactory.getProxy(invoker);
                    channel.setAttribute(proxyCacheKey, proxy);
                    channel.setAttribute(invokerCacheKey, invoker);
                    increaseInstanceCount(channel, countkey);

                    //convert error fail fast .
                    //ignore concurrent problem. 
                    Set<Invoker<?>> callbackInvokers = (Set<Invoker<?>>) channel.getAttribute(Constants.CHANNEL_CALLBACK_KEY);
                    if (callbackInvokers == null) {
                        callbackInvokers = new ConcurrentHashSet<Invoker<?>>(1);
                        callbackInvokers.add(invoker);
                        channel.setAttribute(Constants.CHANNEL_CALLBACK_KEY, callbackInvokers);
                    }
                    logger.info("method " + inv.getMethodName() + " include a callback service :" + invoker.getUrl() + ", a proxy :" + invoker + " has been created.");
                }
            }
        } else {
            if (proxy != null) {
                Invoker<?> invoker = (Invoker<?>) channel.getAttribute(invokerCacheKey);
                try {
                    Set<Invoker<?>> callbackInvokers = (Set<Invoker<?>>) channel.getAttribute(Constants.CHANNEL_CALLBACK_KEY);
                    if (callbackInvokers != null) {
                        callbackInvokers.remove(invoker);
                    }
                    invoker.destroy();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                // cancel refer, directly remove from the map
                channel.removeAttribute(proxyCacheKey);
                channel.removeAttribute(invokerCacheKey);
                decreaseInstanceCount(channel, countkey);
            }
        }
        return proxy;
    }

    private static String getClientSideCallbackServiceCacheKey(int instid) {
        return Constants.CALLBACK_SERVICE_KEY + "." + instid;
    }

    private static String getServerSideCallbackServiceCacheKey(Channel channel, String interfaceClass, int instid) {
        return Constants.CALLBACK_SERVICE_PROXY_KEY + "." + System.identityHashCode(channel) + "." + interfaceClass + "." + instid;
    }

    private static String getServerSideCallbackInvokerCacheKey(Channel channel, String interfaceClass, int instid) {
        return getServerSideCallbackServiceCacheKey(channel, interfaceClass, instid) + "." + "invoker";
    }

    private static String getClientSideCountKey(String interfaceClass) {
        return Constants.CALLBACK_SERVICE_KEY + "." + interfaceClass + ".COUNT";
    }

    private static String getServerSideCountKey(Channel channel, String interfaceClass) {
        return Constants.CALLBACK_SERVICE_PROXY_KEY + "." + System.identityHashCode(channel) + "." + interfaceClass + ".COUNT";
    }

    private static boolean isInstancesOverLimit(Channel channel, URL url, String interfaceClass, int instid, boolean isServer) {
        Integer count = (Integer) channel.getAttribute(isServer ? getServerSideCountKey(channel, interfaceClass) : getClientSideCountKey(interfaceClass));
        int limit = url.getParameter(Constants.CALLBACK_INSTANCES_LIMIT_KEY, Constants.DEFAULT_CALLBACK_INSTANCES);
        if (count != null && count >= limit) {
            //client side error
            throw new IllegalStateException("interface " + interfaceClass + " `s callback instances num exceed providers limit :" + limit
                    + " ,current num: " + (count + 1) + ". The new callback service will not work !!! you can cancle the callback service which exported before. channel :" + channel);
        } else {
            return false;
        }
    }

    private static void increaseInstanceCount(Channel channel, String countkey) {
        try {
            //ignore concurrent problem?
            Integer count = (Integer) channel.getAttribute(countkey);
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            channel.setAttribute(countkey, count);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void decreaseInstanceCount(Channel channel, String countkey) {
        try {
            Integer count = (Integer) channel.getAttribute(countkey);
            if (count == null || count <= 0) {
                return;
            } else {
                count--;
            }
            channel.setAttribute(countkey, count);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 为 invocation参数 编码
     * @param channel
     * @param inv
     * @param paraIndex
     * @return
     * @throws IOException
     */
    public static Object encodeInvocationArgument(Channel channel, RpcInvocation inv, int paraIndex) throws IOException {
        // get URL directly  获取invoker 的url 对象
        URL url = inv.getInvoker() == null ? null : inv.getInvoker().getUrl();
        //判断是否是 回调参数 返回值 0x1 true 0x2 false    就是通过 key：方法名加参数下标加.callback 去param 中拿属性
        byte callbackstatus = isCallBack(url, inv.getMethodName(), paraIndex);
        //获取参数列表 参数类型
        Object[] args = inv.getArguments();
        Class<?>[] pts = inv.getParameterTypes();
        switch (callbackstatus) {
            case CallbackServiceCodec.CALLBACK_NONE:
                //返回 原参数
                return args[paraIndex];
            case CallbackServiceCodec.CALLBACK_CREATE:
                //增加一个 回调标识 并且 不再返回参数
                inv.setAttachment(INV_ATT_CALLBACK_KEY + paraIndex, exportOrunexportCallbackService(channel, url, pts[paraIndex], args[paraIndex], true));
                return null;
            case CallbackServiceCodec.CALLBACK_DESTROY:
                inv.setAttachment(INV_ATT_CALLBACK_KEY + paraIndex, exportOrunexportCallbackService(channel, url, pts[paraIndex], args[paraIndex], false));
                return null;
            default:
                return args[paraIndex];
        }
    }

    /**
     * 应该是对参数对象做什么处理
     * @param channel 通道对象
     * @param inv invoker 对象
     * @param pts 参数类型
     * @param paraIndex 第几个参数
     * @param inObject 参数对象
     * @return
     * @throws IOException
     */
    public static Object decodeInvocationArgument(Channel channel, RpcInvocation inv, Class<?>[] pts, int paraIndex, Object inObject) throws IOException {
        // if it's a callback, create proxy on client side, callback interface on client side can be invoked through channel
        // need get URL from channel and env when decode
        URL url = null;
        try {
            //获取 url 对象
            url = DubboProtocol.getDubboProtocol().getInvoker(channel, inv).getUrl();
        } catch (RemotingException e) {
            if (logger.isInfoEnabled()) {
                logger.info(e.getMessage(), e);
            }
            return inObject;
        }
        //通过 查看 url中 有没有 设置回调状态属性返回结果
        byte callbackstatus = isCallBack(url, inv.getMethodName(), paraIndex);
        switch (callbackstatus) {
            //没有设置回调属性 直接返回原对象
            case CallbackServiceCodec.CALLBACK_NONE:
                return inObject;
                //设置了 回调属性
            case CallbackServiceCodec.CALLBACK_CREATE:
                try {
                    //引入 or 销毁回调服务  跟下面 只有最后一个参数不一样
                    return referOrdestroyCallbackService(channel, url, pts[paraIndex], inv, Integer.parseInt(inv.getAttachment(INV_ATT_CALLBACK_KEY + paraIndex)), true);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new IOException(StringUtils.toString(e));
                }
                ///没有设置回调
            case CallbackServiceCodec.CALLBACK_DESTROY:
                try {
                    return referOrdestroyCallbackService(channel, url, pts[paraIndex], inv, Integer.parseInt(inv.getAttachment(INV_ATT_CALLBACK_KEY + paraIndex)), false);
                } catch (Exception e) {
                    throw new IOException(StringUtils.toString(e));
                }
            default:
                return inObject;
        }
    }
}