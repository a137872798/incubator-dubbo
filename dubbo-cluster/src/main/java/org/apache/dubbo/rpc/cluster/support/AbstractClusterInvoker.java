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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AbstractClusterInvoker
 *
 * 集群invoker 的 抽象类
 */
public abstract class AbstractClusterInvoker<T> implements Invoker<T> {

    private static final Logger logger = LoggerFactory
            .getLogger(AbstractClusterInvoker.class);

    /**
     * 能够选择的所有invoker对象目录
     */
    protected final Directory<T> directory;

    /**9
     * 集群做负载时是否要检查该节点是否可用 默认为true
     */
    protected final boolean availablecheck;

    /**
     * 是否已经销毁
     */
    private AtomicBoolean destroyed = new AtomicBoolean(false);

    /**
     * 粘滞连接用于有状态的服务 尽可能让可以端总是向同一个提供者发起调用
     */
    private volatile Invoker<T> stickyInvoker = null;

    /**
     * 通过可选invoker 目录和 对应的url 创建invoker对象
     * @param directory
     */
    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        //初始化 目录对象
        if (directory == null) {
            throw new IllegalArgumentException("service directory == null");
        }

        this.directory = directory;
        //sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        //在 选择节点的 时候应该要保证检查
        this.availablecheck = url.getParameter(Constants.CLUSTER_AVAILABLE_CHECK_KEY, Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK);
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public URL getUrl() {
        return directory.getUrl();
    }

    @Override
    public boolean isAvailable() {
        //如果 存在 固定的invoker 就 检查该对象是否可用  否则就从目录中选择的那个对象进行检查
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            directory.destroy();
        }
    }

    /**
     * Select a invoker using loadbalance policy.</br>
     * a) Firstly, select an invoker using loadbalance. If this invoker is in previously selected list, or,
     * if this invoker is unavailable, then continue step b (reselect), otherwise return the first selected invoker</br>
     * <p>
     * b) Reselection, the validation rule for reselection: selected > available. This rule guarantees that
     * the selected invoker has the minimum chance to be one in the previously selected list, and also
     * guarantees this invoker is available.
     *
     * @param loadbalance load balance policy
     * @param invocation  invocation
     * @param invokers    invoker candidates
     * @param selected    exclude selected invokers or not
     * @return the invoker which will final to do invoke.
     * @throws RpcException
     *
     * 通过均衡负载策略 选择一个 invoker 对象如果选择的 之前被选择过 或者不可用 就重新选择
     */
    protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
        //如果可供选择的 invoker 为null 直接返回null
        if (invokers == null || invokers.isEmpty()) {
            return null;
        }

        //从invocation 中获取调用的方法名
        String methodName = invocation == null ? "" : invocation.getMethodName();

        //判断是否是 粘滞连接 默认是false
        boolean sticky = invokers.get(0).getUrl().getMethodParameter(methodName, Constants.CLUSTER_STICKY_KEY, Constants.DEFAULT_CLUSTER_STICKY);
        {
            //ignore overloaded method 代表之前的粘滞invoker 对象失效
            if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
                stickyInvoker = null;
            }
            //ignore concurrency problem
            //如果 存在粘滞对象 且 是 粘滞模式 直接返回粘滞对象  selected 代表上次使用过的 可能代表 集群失败时 会将失败的加入到该容器中 避免下次被选择 也就是粘滞对象不能在
            //失败容器中
            if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {
                //需要检查 可用性 并且可用
                if (availablecheck && stickyInvoker.isAvailable()) {
                    return stickyInvoker;
                }
            }
        }
        //没有可返回的粘滞对象 就要 进行选择 并返回合适的invoker对象
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        if (sticky) {
            //将sticky对象设置为返回的 合适的invoker 这样下次 可以直接返回 粘滞对象
            stickyInvoker = invoker;
        }
        return invoker;
    }

    /**
     * 选择的 实际逻辑
     * @param loadbalance 负载策略
     * @param invocation
     * @param invokers 供选择的列表
     * @param selected 代表之前被选择过了
     * @return
     * @throws RpcException
     */
    private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
        //invoker为 null 直接返回
        if (invokers == null || invokers.isEmpty()) {
            return null;
        }
        //如果只有一个元素可供选择 就直接返回
        if (invokers.size() == 1) {
            return invokers.get(0);
        }
        //委托给 负载策略进行选择
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        //If the `invoker` is in the  `selected` or invoker is unavailable && availablecheck is true, reselect.
        //如果选择出来的 对象已经存在于selected
        if ((selected != null && selected.contains(invoker))
                //如果不可用
                || (!invoker.isAvailable() && getUrl() != null && availablecheck)) {
            try {
                //进行重新选择
                Invoker<T> rinvoker = reselect(loadbalance, invocation, invokers, selected, availablecheck);
                if (rinvoker != null) {
                    invoker = rinvoker;
                } else {
                    //Check the index of current selected invoker, if it's not the last one, choose the one at index+1.
                    //重新选择 失败 获取 之前找到的invoker 的下标
                    int index = invokers.indexOf(invoker);
                    try {
                        //Avoid collision
                        //获取 下标的后一个invoker对象
                        invoker = index < invokers.size() - 1 ? invokers.get(index + 1) : invokers.get(0);
                    } catch (Exception e) {
                        logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("cluster reselect fail reason is :" + t.getMessage() + " if can not solve, you can set cluster.availablecheck=false in url", t);
            }
        }
        //如果不进行 available检查 就会直接返回选择结果
        return invoker;
    }

    /**
     * Reselect, use invokers not in `selected` first, if all invokers are in `selected`, just pick an available one using loadbalance policy.
     *
     * 重新选择
     * @param loadbalance 负载策略
     * @param invocation 调用的上下文对象
     * @param invokers 供选择的invoker 列表
     * @param selected 之前被选择过的 且失败的invoker 列表
     * @param availablecheck 是否需要检查可用性
     * @return
     * @throws RpcException
     */
    private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availablecheck)
            throws RpcException {

        //Allocating one in advance, this list is certain to be used.
        //创建一个 长度-1 的 invoker 列表 因为能够进入到这里selected 中肯定有至少一个元素 就是通过选出的invoker 对象在 selected中存在 才判断需要重选
        List<Invoker<T>> reselectInvokers = new ArrayList<Invoker<T>>(invokers.size() > 1 ? (invokers.size() - 1) : invokers.size());

        //First, try picking a invoker not in `selected`.
        //如果需要进行检查
        if (availablecheck) { // invoker.isAvailable() should be checked
            for (Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    //首先可以进行重新选择的 不能是 出现在 selected 中的
                    if (selected == null || !selected.contains(invoker)) {
                        //代表可供选择的 列表
                        reselectInvokers.add(invoker);
                    }
                }
            }
            //再次选择 这里返回的不是 selected 中存在的
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        } else { // do not check invoker.isAvailable()
            //不检查的情况 逻辑基本同上 只是 没有 invoker.isAvaliable
            for (Invoker<T> invoker : invokers) {
                if (selected == null || !selected.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        }
        // Just pick an available invoker using loadbalance policy
        {
            //这里代表reselectInvoker 中没有元素 就只能从selected 中选择 元素
            if (selected != null) {
                for (Invoker<T> invoker : selected) {
                    if ((invoker.isAvailable()) // available first
                            && !reselectInvokers.contains(invoker)) {
                        reselectInvokers.add(invoker);
                    }
                }
            }
            if (!reselectInvokers.isEmpty()) {
                return loadbalance.select(reselectInvokers, getUrl(), invocation);
            }
        }
        return null;
    }

    /**
     * 当针对 集群对象调用invoker 方法时 就会到这里 并在内部实现了 route 均衡负载 configurator 等一系列处理 返回唯一一个invoker 对象
     * @param invocation  这里是调用的 上下文对象 封装了想调用的方法
     * @return
     * @throws RpcException
     */
    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        //判断该 集群是否 被关闭 关闭就抛出异常
        checkWhetherDestroyed();

        // binding attachments into invocation.
        // 获取上下文绑定的attachment
        Map<String, String> contextAttachments = RpcContext.getContext().getAttachments();
        if (contextAttachments != null && contextAttachments.size() != 0) {
            //将绑定 属性转移到 rpcinvocation上
            ((RpcInvocation) invocation).addAttachments(contextAttachments);
        }

        //委托给 directory 对象返回一组invoker对象 这里已经被路由过了
        List<Invoker<T>> invokers = list(invocation);
        //创建负载对象  这个invokers 的url 都是 被 消费者 merge 过的 不是单纯的 提供者的 url
        LoadBalance loadbalance = initLoadBalance(invokers, invocation);
        //为 异步invocation 设置id属性  还不懂
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);
        return doInvoke(invocation, invokers, loadbalance);
    }

    protected void checkWhetherDestroyed() {

        if (destroyed.get()) {
            throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                    + " use dubbo version " + Version.getVersion()
                    + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (invokers == null || invokers.isEmpty()) {
            throw new RpcException("Failed to invoke the method "
                    + invocation.getMethodName() + " in the service " + getInterface().getName()
                    + ". No provider available for the service " + directory.getUrl().getServiceKey()
                    + " from registry " + directory.getUrl().getAddress()
                    + " on the consumer " + NetUtils.getLocalHost()
                    + " using the dubbo version " + Version.getVersion()
                    + ". Please check if the providers have been started and registered.");
        }
    }

    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
                                       LoadBalance loadbalance) throws RpcException;

    /**
     * 返回 directory 中的 一系列调用者
     * @param invocation
     * @return
     * @throws RpcException
     */
    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return directory.list(invocation);
    }

    /**
     * Init LoadBalance.
     * <p>
     * if invokers is not empty, init from the first invoke's url and invocation
     * if invokes is empty, init a default LoadBalance(RandomLoadBalance)
     * </p>
     *
     * @param invokers   invokers
     * @param invocation invocation
     * @return LoadBalance instance. if not need init, return null.
     *
     * 初始化均衡负载对象
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        //判断该方法级别 有没有限定的 负载策略
        if (CollectionUtils.isNotEmpty(invokers)) {
            return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(invokers.get(0).getUrl()
                    .getMethodParameter(RpcUtils.getMethodName(invocation), Constants.LOADBALANCE_KEY, Constants.DEFAULT_LOADBALANCE));
        } else {
            //否则使用默认的 负载对象
            return ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(Constants.DEFAULT_LOADBALANCE);
        }
    }
}
