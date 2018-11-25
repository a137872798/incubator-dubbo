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
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassHelper;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proxy.
 *
 * 代理对象
 */

public abstract class Proxy {
    /**
     * 创建一个 空的 invoker 对象  通过传入这个invoker 对象 该proxy 执行任何方法 都只会返回null
     */
    public static final InvocationHandler RETURN_NULL_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    };
    /**
     * 没有实现任何方法的 invoker
     */
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };
    /**
     * 代理类计数器
     */
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    /**
     * 包名
     */
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();

    /**
     * 当对象 不被使用时 加快回收
     */
    private static final Map<ClassLoader, Map<String, Object>> ProxyCacheMap = new WeakHashMap<ClassLoader, Map<String, Object>>();

    /**
     * 因为是 内置锁修饰 就不需要 volatile 修饰
     */
    private static final Object PendingGenerationMarker = new Object();

    protected Proxy() {
    }

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassHelper.getClassLoader(Proxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * 获取代理对象
     * @param cl  class loader.
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
        //传入的 可变数组对象 过多
        if (ics.length > 65535) {
            throw new IllegalArgumentException("interface limit exceeded");
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            //传入的 必须是 接口类型
            String itf = ics[i].getName();
            if (!ics[i].isInterface()) {
                throw new RuntimeException(itf + " is not a interface.");
            }

            Class<?> tmp = null;
            try {
                //创建接口对象类型
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            //找不到该类 代表不适用 该类加载器创建的
            if (tmp != ics[i]) {
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");
            }

            //将接口 拼接起来
            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        //拼接的 接口名作为key
        String key = sb.toString();

        // get cache by class loader.
        Map<String, Object> cache;
        //创建 以 classLoader 为key 的 对象  value 是一个 map对象 key 为 接口
        synchronized (ProxyCacheMap) {
            cache = ProxyCacheMap.get(cl);
            if (cache == null) {
                cache = new HashMap<String, Object>();
                ProxyCacheMap.put(cl, cache);
            }
        }

        //创建代理对象
        Proxy proxy = null;
        synchronized (cache) {
            do {
                Object value = cache.get(key);
                //value 也是被 weakReference 包装的
                if (value instanceof Reference<?>) {
                    proxy = (Proxy) ((Reference<?>) value).get();
                    if (proxy != null) {
                        return proxy;
                    }
                }

                //如果value 是某个 特殊值  这是 在 缩小同步块 其他线程在进入的时候 发现 已经被占位了 代表要开始生成代理对象了 就沉睡等待其他线程
                //创建代理对象
                if (value == PendingGenerationMarker) {
                    try {
                        //释放内置锁 沉睡
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    //这里代表的 是value == null 就保存默认值
                    cache.put(key, PendingGenerationMarker);
                    break;
                }
            }
            while (true);
        }

        //每次生成代理对象 计数器都会加1
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        ClassGenerator ccp = null, ccm = null;
        try {
            //创建了一个 classGenerator 对象
            ccp = ClassGenerator.newInstance(cl);

            Set<String> worked = new HashSet<String>();
            List<Method> methods = new ArrayList<Method>();

            //遍历每个接口
            for (int i = 0; i < ics.length; i++) {
                //接口也是有修饰符的  如果是不为 public 的 修饰符 就代表一定是导入的 实现的所有非public接口必须来自相同的 包
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    //获取该接口的  包名 对比包名是否相同
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg)) {
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                        }
                    }
                }
                //将接口 加入到 classgenerator 中 这样生成的类就会 实现这些接口
                ccp.addInterface(ics[i]);

                //获取 该接口的所有方法
                for (Method method : ics[i].getMethods()) {
                    //获取该方法的 描述信息 通过方法名和 参数列表确定唯一方法对象
                    String desc = ReflectUtils.getDesc(method);
                    //将描述信息保存到容器中 这是为了避免重复
                    if (worked.contains(desc)) {
                        continue;
                    }
                    worked.add(desc);

                    int ix = methods.size();
                    //获得方法返回值和 参数列表
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    //这里相当于是每个 接口方法的实现

                    //Object[] args = new Object[length];  针对参数列表 依次赋值
                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    //遍历每个参数类型  args[0] = ($w)$1; 生成这样的字符串 那么 $1 $2 就是参数的占位符
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    //通过传入的 参数构造成的参数数组去invoker 生成对象
                    //Object ret = handler.invoker(this, methods[index], args); size 相当于是记录了方法的下标 因为 因为这些方法是每个接口的 方法总和
                    //因为 这个 方法数组是静态类 全局共享 所以 在创建的 时候 每个方法就要知道自己的 下标 这样生成正确的代理类
                    code.append(" Object ret = handler.invoke(this, methods[" + ix + "], args);");
                    if (!Void.TYPE.equals(rt)) {
                        //return (classname)ret; 这里强转了 返回类型
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    }

                    //该代理类应该是 实现了所有的 接口方法的 并且 通过下标知道哪个方法要实现哪个功能 但是实现实际功能的 还是 handler.   invoker

                    //将每个接口的每个方法保存到容器中
                    methods.add(method);
                    //为 ctclass 增加方法对象  这里第一个参数 就是方法名
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());

                    //到上面为止 只是为 ccp 对象 增加了 方法 属性还没有设置
                }
            }

            if (pkg == null) {
                //使用 proxy 所在的包名
                pkg = PACKAGE_NAME;
            }

            // create ProxyInstance class.
            //生成的代理类名 className: proxy1 proxy2
            String pcn = pkg + ".proxy" + id;
            ccp.setClassName(pcn);
            //含有 方法属性 因为这里加入的 field 都是 含有 public/private 等的所以 classGenerator 可以这样解析
            //这个方法 代表着能实现的全部方法
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            //handler 成员 每个方法的实现都是通过委托给这个handler 对象
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            //创建构造函数  返回值 抛出异常类 方法体内容  就是初始化 handler对象
            //proxy1(InvocationHandler $1){ handler = $1;}  可能用javassist 生成的 类 的参数 就是 $1
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            //添加默认的构造函数
            ccp.addDefaultConstructor();
            Class<?> clazz = ccp.toClass();
            //设置 静态属性 实体对象就是 传 null 也就是这个代理类的 静态方法属性就是 methods
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            //以上创建了 proxy序号 类对象  一个invoker成员变量 一个method[] 数组对象 然后实现了 所有给定接口的 方法体 这里只是该类的 bytecode 类还没生成
            //需要通过classGenerator对象 同时在创建过程中 该类 也可以对创建的 对象做拓展 这里使得该类 实现 Proxy 也就是增加一个 newInstance方法 返回 代理对象
            //还实现便于测试的echoService 和 DC 接口代表是拓展类
            //也就是说 实现 proxy功能的 还是 handler对象 通过传入指定方法名 调用invoker 创建真正的结果对象

            // create Proxy class.  这个就是代理类名
            String fcn = Proxy.class.getName() + id;
            //创建第二个 classgenerator对象
            ccm = ClassGenerator.newInstance(cl);
            ccm.setClassName(fcn);
            ccm.addDefaultConstructor();
            //实现 proxy 类 也就是实现类 抽象方法 newInstance
            ccm.setSuperClass(Proxy.class);
            //添加  public Object newInstance(Invoker h){ return new proxy1($1);}  返回上面创建的代理类 $1 应该是参数的占位符也就代表h对象
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            Class<?> pc = ccm.toClass();
            //通过反射创建实例对象
            proxy = (Proxy) pc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release ClassGenerator
            //释放资源
            if (ccp != null) {
                ccp.release();
            }
            if (ccm != null) {
                ccm.release();
            }
            synchronized (cache) {
                if (proxy == null) {
                    cache.remove(key);
                } else {
                    //通过传入 弱引用使得 这个对象容易被 回收
                    cache.put(key, new WeakReference<Proxy>(proxy));
                }
                //唤醒阻塞线程
                cache.notifyAll();
            }
        }
        return proxy;
    }

    /**
     * 根据 指定类型生成 返回的  字符串信息
     * @param cl
     * @param name
     * @return
     */
    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl) {
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            }
            if (Byte.TYPE == cl) {
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            }
            if (Character.TYPE == cl) {
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            }
            if (Double.TYPE == cl) {
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            }
            if (Float.TYPE == cl) {
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            }
            if (Integer.TYPE == cl) {
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            }
            if (Long.TYPE == cl) {
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            }
            if (Short.TYPE == cl) {
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            }
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * 使用默认 的 handler 创建 proxy 对象
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);
}
