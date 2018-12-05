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
package org.apache.dubbo.common.compiler.support;

import org.apache.dubbo.common.utils.ClassHelper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JavassistCompiler. (SPI, Singleton, ThreadSafe)
 *
 * javassist 编译类
 */
public class JavassistCompiler extends AbstractCompiler {

    //---------- 匹配各种特殊字符的 正则对象 ---------------//

    private static final Pattern IMPORT_PATTERN = Pattern.compile("import\\s+([\\w\\.\\*]+);\n");

    private static final Pattern EXTENDS_PATTERN = Pattern.compile("\\s+extends\\s+([\\w\\.]+)[^\\{]*\\{\n");

    private static final Pattern IMPLEMENTS_PATTERN = Pattern.compile("\\s+implements\\s+([\\w\\.]+)\\s*\\{\n");

    private static final Pattern METHODS_PATTERN = Pattern.compile("\n(private|public|protected)\\s+");

    private static final Pattern FIELD_PATTERN = Pattern.compile("[^\n]+=[^\n]+;");

    /**
     * javassist 实现 动态编译 这样就可以通过类加载器 进行加载了  这个相当于就是把 sourceCode 中的各个部分拆成package import 等 信息 设置到 ctclass中 然后就可以编译了
     * @param name  这里经过上层处理后 只剩下类的全限定名
     * @param source
     * @return
     * @throws Throwable
     */
    @Override
    public Class<?> doCompile(String name, String source) throws Throwable {
        int i = name.lastIndexOf('.');
        //获取类名
        String className = i < 0 ? name : name.substring(i + 1);
        //javassist 的 对象池
        ClassPool pool = new ClassPool(true);
        //首先定位 带 该类的 路径 这样才能找到 这个类的字节码 这个动作应该是能在 classPool 中导入该类加载器加载的所有类
        pool.appendClassPath(new LoaderClassPath(ClassHelper.getCallerClassLoader(getClass())));
        //匹配 import
        Matcher matcher = IMPORT_PATTERN.matcher(source);
        //存在 import 的所有类
        List<String> importPackages = new ArrayList<String>();
        Map<String, String> fullNames = new HashMap<String, String>();
        while (matcher.find()) {
            String pkg = matcher.group(1);
            if (pkg.endsWith(".*")) {
                //截取 * 前的 所有数据
                String pkgName = pkg.substring(0, pkg.length() - 2);
                //从类池 中 找到 对应的 包
                pool.importPackage(pkgName);
                importPackages.add(pkgName);
            } else {
                int pi = pkg.lastIndexOf('.');
                if (pi > 0) {
                    //截取到包名
                    String pkgName = pkg.substring(0, pi);
                    pool.importPackage(pkgName);
                    importPackages.add(pkgName);
                    //完整名和 完整包名的 关联
                    fullNames.put(pkg.substring(pi + 1), pkg);
                }
            }
        }
        //获取 一共导入了 多少包
        String[] packages = importPackages.toArray(new String[0]);
        matcher = EXTENDS_PATTERN.matcher(source);
        //创建 javassist 生成的 动态类
        CtClass cls;
        if (matcher.find()) {
            //获取到 继承的类
            String extend = matcher.group(1).trim();
            //继承的 类的 全限定名
            String extendClass;
            if (extend.contains(".")) {
                extendClass = extend;
            } else if (fullNames.containsKey(extend)) {
                //如果能找到 导入的 类 就直接从那里 获取全限定名
                extendClass = fullNames.get(extend);
            } else {
                //通过 导入的 包 和 类的 简化名 找到全限定名
                extendClass = ClassUtils.forName(packages, extend).getName();
            }
            //通过 指定的全限定名 生成动态类 第二个参数 应该是父类  从类池中获取 指定类
            cls = pool.makeClass(name, pool.get(extendClass));
        } else {
            //没有找到 extends 就直接 创建动态类
            cls = pool.makeClass(name);
        }
        //查看有没有实现某个接口
        matcher = IMPLEMENTS_PATTERN.matcher(source);
        if (matcher.find()) {
            //看看有几个 接口
            String[] ifaces = matcher.group(1).trim().split("\\,");
            for (String iface : ifaces) {
                iface = iface.trim();
                //接口 全限定名
                String ifaceClass;
                if (iface.contains(".")) {
                    ifaceClass = iface;
                    //如果从import中直接找到了 就 直接使用这个类
                } else if (fullNames.containsKey(iface)) {
                    ifaceClass = fullNames.get(iface);
                } else {
                    ifaceClass = ClassUtils.forName(packages, iface).getName();
                }
                //添加指定接口
                cls.addInterface(pool.get(ifaceClass));
            }
        }
        //获取 从实现接口 开始后面的全部数据
        String body = source.substring(source.indexOf("{") + 1, source.length() - 1);
        //匹配每个 方法体  这个动态类 一般就是 几个待实现方法
        String[] methods = METHODS_PATTERN.split(body);
        for (String method : methods) {
            method = method.trim();
            if (method.length() > 0) {
                //代表是构造函数
                if (method.startsWith(className)) {
                    cls.addConstructor(CtNewConstructor.make("public " + method, cls));
                    //匹配 "="
                } else if (FIELD_PATTERN.matcher(method).matches()) {
                    //生成 私有 属性
                    cls.addField(CtField.make("private " + method, cls));
                } else {
                    cls.addMethod(CtNewMethod.make("public " + method, cls));
                }
            }
        }
        //使用指定类加载器 生成 class 对象   设置 保护域 不知道什么意思
        return cls.toClass(ClassHelper.getCallerClassLoader(getClass()), JavassistCompiler.class.getProtectionDomain());
    }

}
