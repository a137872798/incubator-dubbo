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

import org.apache.dubbo.common.compiler.Compiler;
import org.apache.dubbo.common.utils.ClassHelper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract compiler. (SPI, Prototype, ThreadSafe)
 *
 * 动态代理类的 基类
 */
public abstract class AbstractCompiler implements Compiler {

    /**
     * 包名正则
     */
    private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([$_a-zA-Z][$_a-zA-Z0-9\\.]*);");

    /**
     * 类名正则
     */
    private static final Pattern CLASS_PATTERN = Pattern.compile("class\\s+([$_a-zA-Z][$_a-zA-Z0-9]*)\\s+");

    /**
     * 编译公共实现
     * @param code        Java source code
     * @param classLoader classloader
     * @return
     */
    @Override
    public Class<?> compile(String code, ClassLoader classLoader) {
        //获取 code 值
        code = code.trim();
        //匹配包名
        Matcher matcher = PACKAGE_PATTERN.matcher(code);
        String pkg;
        if (matcher.find()) {
            //1 代表第一个 小括号
            pkg = matcher.group(1);
        } else {
            pkg = "";
        }
        //匹配类名
        matcher = CLASS_PATTERN.matcher(code);
        String cls;
        //设置类名
        if (matcher.find()) {
            cls = matcher.group(1);
        } else {
            throw new IllegalArgumentException("No such class name in " + code);
        }
        //将 package 后面的 路径名 加上类名 变成全限定名
        String className = pkg != null && pkg.length() > 0 ? pkg + "." + cls : cls;
        try {
            //生成类对象  如果已经编译过可能就 会编译成功
            return Class.forName(className, true, ClassHelper.getCallerClassLoader(getClass()));
        } catch (ClassNotFoundException e) {
            //如果 这段代码不是已 } 结尾就抛出异常
            if (!code.endsWith("}")) {
                throw new IllegalStateException("The java code not endsWith \"}\", code: \n" + code + "\n");
            }
            try {
                return doCompile(className, code);
            } catch (RuntimeException t) {
                throw t;
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to compile class, cause: " + t.getMessage() + ", class: " + className + ", code: \n" + code + "\n, stack: " + ClassUtils.toString(t));
            }
        }
    }

    /**
     * 子类实现编译的 实际逻辑
     * @param name
     * @param source
     * @return
     * @throws Throwable
     */
    protected abstract Class<?> doCompile(String name, String source) throws Throwable;

}
