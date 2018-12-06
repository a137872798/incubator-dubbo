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

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.NotFoundException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClassGenerator
 *
 * 生成代理类对象 的 生产者  基于javassist 实现
 */
public final class ClassGenerator {

    /**
     * 方法名 计数器
     */
    private static final AtomicLong CLASS_NAME_COUNTER = new AtomicLong(0);
    /**
     * 特殊标签
     */
    private static final String SIMPLE_NAME_TAG = "<init>";
    /**
     * 类加载器 与 该类加载器对应的 ClassPool 也是全局变量
     */
    private static final Map<ClassLoader, ClassPool> POOL_MAP = new ConcurrentHashMap<ClassLoader, ClassPool>(); //ClassLoader - ClassPool
    /**
     * 本类的 ClassPool
     */
    private ClassPool mPool;
    /**
     * 产生的 CtClass 看来一个 generator 对应一个 ctclass
     */
    private CtClass mCtc;
    /**
     * 拓展类名
     */
    private String mClassName;
    /**
     * 父类
     */
    private String mSuperClass;
    /**
     * 该类 实现的 接口数组
     */
    private Set<String> mInterfaces;
    /**
     * 该类的 字段
     */
    private List<String> mFields;
    /**
     * 该类的全部构造器
     */
    private List<String> mConstructors;
    /**
     * 获取所有方法
     */
    private List<String> mMethods;
    /**
     * 方法描述 和方法 实例
     */
    private Map<String, Method> mCopyMethods; // <method desc,method instance>
    /**
     * 构造方法描述 和 实例
     */
    private Map<String, Constructor<?>> mCopyConstructors; // <constructor desc,constructor instance>
    private boolean mDefaultConstructor = false;

    private ClassGenerator() {
    }

    private ClassGenerator(ClassPool pool) {
        mPool = pool;
    }

    /**
     * 返回一个实例对象
     * @return
     */
    public static ClassGenerator newInstance() {
        //从当前类加载器中 获取对应的 classpool对象 并 初始化 classGenerator
        return new ClassGenerator(getClassPool(Thread.currentThread().getContextClassLoader()));
    }

    public static ClassGenerator newInstance(ClassLoader loader) {
        return new ClassGenerator(getClassPool(loader));
    }

    /**
     * 判断该类是否是动态生成的  看来这里对动态生成的类就是 实现了 DC
     * @param cl
     * @return
     */
    public static boolean isDynamicClass(Class<?> cl) {
        return ClassGenerator.DC.class.isAssignableFrom(cl);
    }

    /**
     * 通过 给定的 classLoader 返回对应的 classPool
     * @param loader
     * @return
     */
    public static ClassPool getClassPool(ClassLoader loader) {
        if (loader == null) {
            //获取 默认的 classPool
            return ClassPool.getDefault();
        }

        //延时初始化
        ClassPool pool = POOL_MAP.get(loader);
        if (pool == null) {
            pool = new ClassPool(true);
            //添加 传入的  classLoader 路径
            pool.appendClassPath(new LoaderClassPath(loader));
            POOL_MAP.put(loader, pool);
        }
        return pool;
    }

    /**
     * 传入的 int 属于 哪种修饰符
     * @param mod
     * @return
     */
    private static String modifier(int mod) {
        StringBuilder modifier = new StringBuilder();
        if (Modifier.isPublic(mod)) {
            modifier.append("public");
        }
        if (Modifier.isProtected(mod)) {
            modifier.append("protected");
        }
        if (Modifier.isPrivate(mod)) {
            modifier.append("private");
        }

        if (Modifier.isStatic(mod)) {
            modifier.append(" static");
        }
        if (Modifier.isVolatile(mod)) {
            modifier.append(" volatile");
        }

        return modifier.toString();
    }

    public String getClassName() {
        return mClassName;
    }

    public ClassGenerator setClassName(String name) {
        mClassName = name;
        return this;
    }

    public ClassGenerator addInterface(String cn) {
        if (mInterfaces == null) {
            mInterfaces = new HashSet<String>();
        }
        mInterfaces.add(cn);
        return this;
    }

    /**
     * 通过 传入接口的 类型 来添加接口
     * @param cl
     * @return
     */
    public ClassGenerator addInterface(Class<?> cl) {
        return addInterface(cl.getName());
    }

    public ClassGenerator setSuperClass(String cn) {
        mSuperClass = cn;
        return this;
    }

    public ClassGenerator setSuperClass(Class<?> cl) {
        mSuperClass = cl.getName();
        return this;
    }

    /**
     * 将属性变成 String 类型后 添加到容器中
     * @param code
     * @return
     */
    public ClassGenerator addField(String code) {
        if (mFields == null) {
            mFields = new ArrayList<String>();
        }
        mFields.add(code);
        return this;
    }

    /**
     * 添加属性
     * @param name 属性名
     * @param mod 属性访问修饰符
     * @param type 属性类型
     * @return
     */
    public ClassGenerator addField(String name, int mod, Class<?> type) {
        return addField(name, mod, type, null);
    }

    /**
     * 添加属性
     * @param name 属性名
     * @param mod 属性访问修饰符
     * @param type 属性类型
     * @param def 默认值
     * @return
     */
    public ClassGenerator addField(String name, int mod, Class<?> type, String def) {
        StringBuilder sb = new StringBuilder();
        sb.append(modifier(mod)).append(' ').append(ReflectUtils.getName(type)).append(' ');
        sb.append(name);
        if (def != null && def.length() > 0) {
            sb.append('=');
            sb.append(def);
        }
        sb.append(';');
        //生成 类似 private AAA aa;
        return addField(sb.toString());
    }

    /**
     * 将生成的 方法 码 添加到容器中
     * @param code
     * @return
     */
    public ClassGenerator addMethod(String code) {
        if (mMethods == null) {
            mMethods = new ArrayList<String>();
        }
        mMethods.add(code);
        return this;
    }

    /**
     * 根据 传入的 参数 生成方法字节码
     * @param name 方法名
     * @param mod 访问修饰符
     * @param rt 返回值类型
     * @param pts  参数列表
     * @param body 方法体内容
     * @return
     */
    public ClassGenerator addMethod(String name, int mod, Class<?> rt, Class<?>[] pts, String body) {
        return addMethod(name, mod, rt, pts, null, body);
    }

    /**
     * 通过传入的参数 构建方法体
     * @param name
     * @param mod
     * @param rt
     * @param pts
     * @param ets
     * @param body
     * @return
     */
    public ClassGenerator addMethod(String name, int mod, Class<?> rt, Class<?>[] pts, Class<?>[] ets,
                                    String body) {
        StringBuilder sb = new StringBuilder();
        //public returnclassname
        sb.append(modifier(mod)).append(' ').append(ReflectUtils.getName(rt)).append(' ').append(name);
        sb.append('(');
        for (int i = 0; i < pts.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ReflectUtils.getName(pts[i]));
            sb.append(" arg").append(i);
        }
        sb.append(')');
        if (ets != null && ets.length > 0) {
            sb.append(" throws ");
            for (int i = 0; i < ets.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(ReflectUtils.getName(ets[i]));
            }
        }
        sb.append('{').append(body).append('}');
        return addMethod(sb.toString());
    }

    /**
     * 增加 方法对象
     * @param m
     * @return
     */
    public ClassGenerator addMethod(Method m) {
        addMethod(m.getName(), m);
        return this;
    }

    public ClassGenerator addMethod(String name, Method m) {
        //生成方法描述
        String desc = name + ReflectUtils.getDescWithoutMethodName(m);
        //将 描述 添加到 方法容器中  注意 描述 作为 方法信息被添加到容器中是否 特殊的 标识作为开头的 这样等下是可以区分的
        addMethod(':' + desc);
        if (mCopyMethods == null) {
            mCopyMethods = new ConcurrentHashMap<String, Method>(8);
        }
        mCopyMethods.put(desc, m);
        return this;
    }

    /**
     * 添加构造函数
     * @param code
     * @return
     */
    public ClassGenerator addConstructor(String code) {
        if (mConstructors == null) {
            mConstructors = new LinkedList<String>();
        }
        mConstructors.add(code);
        return this;
    }

    /**
     * 添加构造函数
     * @param mod
     * @param pts
     * @param body
     * @return
     */
    public ClassGenerator addConstructor(int mod, Class<?>[] pts, String body) {
        return addConstructor(mod, pts, null, body);
    }

    /**
     * 添加构造函数  构造函数还能抛出异常???
     * @param mod 访问修饰符
     * @param pts 参数列表
     * @param ets 抛出的异常
     * @param body 实际的 内容
     * @return
     */
    public ClassGenerator addConstructor(int mod, Class<?>[] pts, Class<?>[] ets, String body) {
        StringBuilder sb = new StringBuilder();
        //public <init> (Class1 arg1, Class2 arg2)
        sb.append(modifier(mod)).append(' ').append(SIMPLE_NAME_TAG);
        sb.append('(');
        for (int i = 0; i < pts.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(ReflectUtils.getName(pts[i]));
            sb.append(" arg").append(i);
        }
        sb.append(')');
        if (ets != null && ets.length > 0) {
            sb.append(" throws ");
            for (int i = 0; i < ets.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(ReflectUtils.getName(ets[i]));
            }
        }
        sb.append('{').append(body).append('}');
        return addConstructor(sb.toString());
    }

    /**
     * 获取构造函数的 描述信息 并绑定到容器中
     * @param c
     * @return
     */
    public ClassGenerator addConstructor(Constructor<?> c) {
        String desc = ReflectUtils.getDesc(c);
        addConstructor(":" + desc);
        if (mCopyConstructors == null) {
            mCopyConstructors = new ConcurrentHashMap<String, Constructor<?>>(4);
        }
        mCopyConstructors.put(desc, c);
        return this;
    }

    public ClassGenerator addDefaultConstructor() {
        mDefaultConstructor = true;
        return this;
    }

    public ClassPool getClassPool() {
        return mPool;
    }

    /**
     * 生成 Class 对象
     * @return
     */
    public Class<?> toClass() {
        return toClass(ClassHelper.getClassLoader(ClassGenerator.class),
                getClass().getProtectionDomain());
    }

    /**
     * 根据 类加载器 和 保护域生成 class对象  通过 classGenerator生成的类 都实现了 DC 接口
     * @param loader
     * @param pd
     * @return
     */
    public Class<?> toClass(ClassLoader loader, ProtectionDomain pd) {
        //如果 存在 ctclass 将该对象从 classpool 中移除
        if (mCtc != null) {
            mCtc.detach();
        }
        //每产生 一个 类就修改计数器
        long id = CLASS_NAME_COUNTER.getAndIncrement();
        try {
            //获取 父类对象
            CtClass ctcs = mSuperClass == null ? null : mPool.get(mSuperClass);
            //如果没有设置 class 的名字就使用 接口$sc 的方式 创建 类名 如果接口名也不存在 就使用  ClassGenerator 的类名
            if (mClassName == null) {
                mClassName = (mSuperClass == null || javassist.Modifier.isPublic(ctcs.getModifiers())
                        ? ClassGenerator.class.getName() : mSuperClass + "$sc") + id;
            }
            //创建类对象 声明父类对象
            mCtc = mPool.makeClass(mClassName);
            if (mSuperClass != null) {
                mCtc.setSuperclass(ctcs);
            }
            //增加实现的 DC 接口  代表这个类是动态生成的
            mCtc.addInterface(mPool.get(DC.class.getName())); // add dynamic class tag.
            if (mInterfaces != null) {
                for (String cl : mInterfaces) {
                    mCtc.addInterface(mPool.get(cl));
                }
            }
            //生成对应属性
            if (mFields != null) {
                for (String code : mFields) {
                    mCtc.addField(CtField.make(code, mCtc));
                }
            }
            //以：开头的就是特殊情况
            if (mMethods != null) {
                for (String code : mMethods) {
                    if (code.charAt(0) == ':') {
                        //：后面的就是 方法描述 可以在 对应容器中直接找到该方法对象 在传入 getCtMethod 后获取方法对象 增加指定方法
                        mCtc.addMethod(CtNewMethod.copy(getCtMethod(mCopyMethods.get(code.substring(1))),
                                code.substring(1, code.indexOf('(')), mCtc, null));
                    } else {
                        //否则 就可以直接 方法的字符串格式 生成方法
                        mCtc.addMethod(CtNewMethod.make(code, mCtc));
                    }
                }
            }
            //存在默认构造器 就 使用 应该就是无参构造器
            if (mDefaultConstructor) {
                mCtc.addConstructor(CtNewConstructor.defaultConstructor(mCtc));
            }
            //如果 存在 多个 构造器
            if (mConstructors != null) {
                for (String code : mConstructors) {
                    if (code.charAt(0) == ':') {
                        //这个跟上面一样 通过描述 定位到构造器 然后添加
                        mCtc.addConstructor(CtNewConstructor
                                .copy(getCtConstructor(mCopyConstructors.get(code.substring(1))), mCtc, null));
                    } else {
                        //类名 会 存在 $   需要拆出前面的部分添加构造器
                        String[] sn = mCtc.getSimpleName().split("\\$+"); // inner class name include $.
                        mCtc.addConstructor(
                                CtNewConstructor.make(code.replaceFirst(SIMPLE_NAME_TAG, sn[sn.length - 1]), mCtc));
                    }
                }
            }
            //生成  ctclass 后 获取 类对象
            return mCtc.toClass(loader, pd);
        } catch (RuntimeException e) {
            throw e;
        } catch (NotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (CannotCompileException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 清理 该对象
     */
    public void release() {
        if (mCtc != null) {
            mCtc.detach();
        }
        if (mInterfaces != null) {
            mInterfaces.clear();
        }
        if (mFields != null) {
            mFields.clear();
        }
        if (mMethods != null) {
            mMethods.clear();
        }
        if (mConstructors != null) {
            mConstructors.clear();
        }
        if (mCopyMethods != null) {
            mCopyMethods.clear();
        }
        if (mCopyConstructors != null) {
            mCopyConstructors.clear();
        }
    }

    /**
     * 从 pool中找到 对应类对象
     * @param c
     * @return
     * @throws NotFoundException
     */
    private CtClass getCtClass(Class<?> c) throws NotFoundException {
        return mPool.get(c.getName());
    }

    //从ctClass 中获取 指定方法  这个描述 应该是 javassist 的 特性
    private CtMethod getCtMethod(Method m) throws NotFoundException {
        return getCtClass(m.getDeclaringClass())
                .getMethod(m.getName(), ReflectUtils.getDescWithoutMethodName(m));
    }

    private CtConstructor getCtConstructor(Constructor<?> c) throws NotFoundException {
        return getCtClass(c.getDeclaringClass()).getConstructor(ReflectUtils.getDesc(c));
    }

    public static interface DC {

    } // dynamic class tag interface.
}