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
package org.apache.dubbo.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods and public methods for parsing configuration
 *
 * @export
 */
//dubbo 配置类的 基类 用于封装 解析配置 以及校验的工具
public abstract class AbstractConfig implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractConfig.class);
    private static final long serialVersionUID = 4267533505537413570L;
    /**
     * 配置的最大长度
     */
    private static final int MAX_LENGTH = 200;

    /**
     * 路径的最大长度
     */
    private static final int MAX_PATH_LENGTH = 200;

    /**
     * 中括号内的 符号 都满足条件
     */
    private static final Pattern PATTERN_NAME = Pattern.compile("[\\-._0-9a-zA-Z]+");
    /**
     * 中括号内的 符号 都满足条件 多匹配一个","
     */
    private static final Pattern PATTERN_MULTI_NAME = Pattern.compile("[,\\-._0-9a-zA-Z]+");
    /**
     * 方法名 不包括特殊符号
     */
    private static final Pattern PATTERN_METHOD_NAME = Pattern.compile("[a-zA-Z][0-9a-zA-Z]*");
    /**
     * 匹配路径
     */
    private static final Pattern PATTERN_PATH = Pattern.compile("[/\\-$._0-9a-zA-Z]+");
    /**
     * 包含一些特殊符号的匹配
     */
    private static final Pattern PATTERN_NAME_HAS_SYMBOL = Pattern.compile("[:*,\\s/\\-._0-9a-zA-Z]+");

    private static final Pattern PATTERN_KEY = Pattern.compile("[*,\\-._0-9a-zA-Z]+");
    private static final Map<String, String> legacyProperties = new HashMap<String, String>();
    /**
     * 配置的 后缀名 可能是用 config 也可能是用 bean
     */
    private static final String[] SUFFIXES = new String[]{"Config", "Bean"};

    static {
        legacyProperties.put("dubbo.protocol.name", "dubbo.service.protocol");
        legacyProperties.put("dubbo.protocol.host", "dubbo.service.server.host");
        legacyProperties.put("dubbo.protocol.port", "dubbo.service.server.port");
        legacyProperties.put("dubbo.protocol.threads", "dubbo.service.max.thread.pool.size");
        legacyProperties.put("dubbo.consumer.timeout", "dubbo.service.invoke.timeout");
        legacyProperties.put("dubbo.consumer.retries", "dubbo.service.max.retry.providers");
        legacyProperties.put("dubbo.consumer.check", "dubbo.service.allow.no.provider");
        legacyProperties.put("dubbo.service.url", "dubbo.service.address");

        // this is only for compatibility
        // 给进程设置终结钩子 在程序结束时 会触发
        Runtime.getRuntime().addShutdownHook(DubboShutdownHook.getDubboShutdownHook());
    }

    /**
     * 标记配置的 id api模式下是不需要的
     */
    protected String id;

    /**
     * 对设置的参数做一下处理
     * @param key
     * @param value
     * @return
     */
    private static String convertLegacyValue(String key, String value) {
        if (value != null && value.length() > 0) {
            if ("dubbo.service.max.retry.providers".equals(key)) {
                return String.valueOf(Integer.parseInt(value) - 1);
            } else if ("dubbo.service.allow.no.provider".equals(key)) {
                return String.valueOf(!Boolean.parseBoolean(value));
            }
        }
        return value;
    }

    /**
     * 通过系统变量初始化 配置
     * @param config
     */
    protected static void appendProperties(AbstractConfig config) {
        if (config == null) {
            return;
        }
        //生成对应的配置前缀  如果是 serviceBean 或者 serviceConfig 那解析出来 就是 dubbo.service.
        String prefix = "dubbo." + getTagName(config.getClass()) + ".";
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {
            try {
                String name = method.getName();
                //如果是 setXXX 方法
                if (name.length() > 3 && name.startsWith("set") && Modifier.isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 1 && isPrimitive(method.getParameterTypes()[0])) {
                    //通过 setXXX 方法 就能知道 这个config对象具有什么 属性
                    //转换成属性 如果属性中存在大写字符 要转换成小写并补上分隔符
                    //例如： dsadaDsDDs ==> dsada.ds.d.ds
                    String property = StringUtils.camelToSplitName(name.substring(3, 4).toLowerCase() + name.substring(4), ".");

                    String value = null;
                    if (config.getId() != null && config.getId().length() > 0) {
                        //拼接 id 和 前缀
                        String pn = prefix + config.getId() + "." + property;
                        //从系统变量获取对应的值 那么 这种获取属性的抽取方法就是一种规范
                        //id 应该不是简单的数字 那么是如何被定义的
                        value = System.getProperty(pn);
                        if (!StringUtils.isBlank(value)) {
                            logger.info("Use System Property " + pn + " to config dubbo");
                        }
                    }
                    //没有查询到对应的属性 就不携带id 进行查找
                    if (value == null || value.length() == 0) {
                        String pn = prefix + property;
                        value = System.getProperty(pn);
                        if (!StringUtils.isBlank(value)) {
                            logger.info("Use System Property " + pn + " to config dubbo");
                        }
                    }
                    //还是没有获取到 属性
                    if (value == null || value.length() == 0) {
                        Method getter;
                        try {
                            //尝试获取config 的 getXXX 方法
                            getter = config.getClass().getMethod("get" + name.substring(3));
                        } catch (NoSuchMethodException e) {
                            try {
                                //没有就尝试 is方法
                                getter = config.getClass().getMethod("is" + name.substring(3));
                            } catch (NoSuchMethodException e2) {
                                getter = null;
                            }
                        }
                        //找到了
                        if (getter != null) {
                            //如果属性没有被设置
                            if (getter.invoke(config) == null) {
                                if (config.getId() != null && config.getId().length() > 0) {
                                    //从另一个地方获取 配置 这里先不看
                                    value = ConfigUtils.getProperty(prefix + config.getId() + "." + property);
                                }
                                //没有获取到就去掉id 获取
                                if (value == null || value.length() == 0) {
                                    value = ConfigUtils.getProperty(prefix + property);
                                }
                                //还是获取不到 就从本地缓存的配置中 获取对应的value
                                if (value == null || value.length() == 0) {
                                    String legacyKey = legacyProperties.get(prefix + property);
                                    if (legacyKey != null && legacyKey.length() > 0) {
                                        //如果key 满足特殊值 就处理获取到的value 并重新赋值
                                        value = convertLegacyValue(legacyKey, ConfigUtils.getProperty(legacyKey));
                                    }
                                }

                            }
                        }
                    }
                    //存在 调用set方法设置属性
                    if (value != null && value.length() > 0) {
                        method.invoke(config, convertPrimitive(method.getParameterTypes()[0], value));
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 截取掉配置类的 后缀 返回简化的名字
     * @param cls
     * @return
     */
    private static String getTagName(Class<?> cls) {
        String tag = cls.getSimpleName();
        for (String suffix : SUFFIXES) {
            if (tag.endsWith(suffix)) {
                tag = tag.substring(0, tag.length() - suffix.length());
                break;
            }
        }
        tag = tag.toLowerCase();
        return tag;
    }

    /**
     * 从配置中读取属性设置到容器中
     * @param parameters
     * @param config
     */
    protected static void appendParameters(Map<String, String> parameters, Object config) {
        appendParameters(parameters, config, null);
    }

    @SuppressWarnings("unchecked")
    /**
     * 从配置中读取属性设置到容器中
     * @param parameters
     * @param config
     */
    protected static void appendParameters(Map<String, String> parameters, Object config, String prefix) {
        if (config == null) {
            return;
        }
        //获取该对象的每个方法
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {
            try {
                String name = method.getName();
                //找到 配置对象的get方法 或 is方法
                if ((name.startsWith("get") || name.startsWith("is"))
                        && !"getClass".equals(name)
                        && Modifier.isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && isPrimitive(method.getReturnType())) {
                    //获取 该方法的注解对象
                    Parameter parameter = method.getAnnotation(Parameter.class);
                    //如果是 忽略方法 就跳过
                    if (method.getReturnType() == Object.class || parameter != null && parameter.excluded()) {
                        continue;
                    }
                    int i = name.startsWith("get") ? 3 : 2;
                    //将方法转换成对应属性名
                    String prop = StringUtils.camelToSplitName(name.substring(i, i + 1).toLowerCase() + name.substring(i + 1), ".");
                    String key;
                    //获取 注解的 key 不存在就将 key 设置成属性名
                    if (parameter != null && parameter.key().length() > 0) {
                        key = parameter.key();
                    } else {
                        key = prop;
                    }
                    //获取属性
                    Object value = method.invoke(config);
                    String str = String.valueOf(value).trim();
                    if (value != null && str.length() > 0) {
                        //如果 参数需要转义就encode
                        if (parameter != null && parameter.escaped()) {
                            str = URL.encode(str);
                        }
                        //如果是 append 模式 就将现在的属性追加到原来的属性中
                        if (parameter != null && parameter.append()) {
                            //从map 中获取属性
                            String pre = parameters.get(Constants.DEFAULT_KEY + "." + key);
                            if (pre != null && pre.length() > 0) {
                                //增加属性
                                str = pre + "," + str;
                            }
                            //继续拼接
                            pre = parameters.get(key);
                            if (pre != null && pre.length() > 0) {
                                str = pre + "," + str;
                            }
                        }
                        //给key 增加前缀 如果前缀不存在
                        if (prefix != null && prefix.length() > 0) {
                            key = prefix + "." + key;
                        }
                        //使用新的 key 保存属性  就是覆盖原属性
                        parameters.put(key, str);
                        //当获取的 value 为null 时 而注解要求是必填
                    } else if (parameter != null && parameter.required()) {
                        throw new IllegalStateException(config.getClass().getSimpleName() + "." + key + " == null");
                    }
                    //这里应该是 返回类型不是原始类型 返回的是属性的集合
                } else if ("getParameters".equals(name)
                        && Modifier.isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && method.getReturnType() == Map.class) {
                    //get方法这里怎么会需要参数 可能getParameters 这个方法需要参数
                    Map<String, String> map = (Map<String, String>) method.invoke(config, new Object[0]);
                    if (map != null && map.size() > 0) {
                        String pre = (prefix != null && prefix.length() > 0 ? prefix + "." : "");
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            //提取属性并 保存到给与的容器 可能是覆盖也可能是增加新属性
                            parameters.put(pre + entry.getKey().replace('-', '.'), entry.getValue());
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    protected static void appendAttributes(Map<String, Object> parameters, Object config) {
        appendAttributes(parameters, config, null);
    }

    /**
     * 将attr 设置到map 中
     * @param parameters
     * @param config
     * @param prefix
     */
    protected static void appendAttributes(Map<String, Object> parameters, Object config, String prefix) {
        if (config == null) {
            return;
        }
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {
            try {
                Parameter parameter = method.getAnnotation(Parameter.class);
                //前提是要设置了 attr
                if (parameter == null || !parameter.attribute()) {
                    continue;
                }
                String name = method.getName();
                //需要是 is 或 get 开头
                if ((name.startsWith("get") || name.startsWith("is"))
                        && !"getClass".equals(name)
                        && Modifier.isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && isPrimitive(method.getReturnType())) {
                    String key;
                    if (parameter.key().length() > 0) {
                        key = parameter.key();
                    } else {
                        int i = name.startsWith("get") ? 3 : 2;
                        key = name.substring(i, i + 1).toLowerCase() + name.substring(i + 1);
                    }
                    //获取 属性
                    Object value = method.invoke(config);
                    if (value != null) {
                        if (prefix != null && prefix.length() > 0) {
                            key = prefix + "." + key;
                        }
                        //覆盖 或 添加属性
                        parameters.put(key, value);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    /**
     * 通过给定配置生成 方法的 回调对象
     * @param methodConfig
     * @return
     */
    protected static ConsumerMethodModel.AsyncMethodInfo convertMethodConfig2AyncInfo(MethodConfig methodConfig) {
        //如果 该对象中 没有设置对应的回调函数 就返回
        if (methodConfig == null || (methodConfig.getOninvoke() == null && methodConfig.getOnreturn() == null && methodConfig.getOnthrow() == null)) {
            return null;
        }

        //check config conflict
        //检查 配置冲突
        if (Boolean.FALSE.equals(methodConfig.isReturn()) && (methodConfig.getOnreturn() != null || methodConfig.getOnthrow() != null)) {
            throw new IllegalStateException("method config error : return attribute must be set true when onreturn or onthrow has been set.");
        }

        ConsumerMethodModel.AsyncMethodInfo asyncMethodInfo = new ConsumerMethodModel.AsyncMethodInfo();

        //设置回调对象
        asyncMethodInfo.setOninvokeInstance(methodConfig.getOninvoke());
        asyncMethodInfo.setOnreturnInstance(methodConfig.getOnreturn());
        asyncMethodInfo.setOnthrowInstance(methodConfig.getOnthrow());

        try {
            //调用时 触发的 回调对象的方法名
            String oninvokeMethod = methodConfig.getOninvokeMethod();
            if (StringUtils.isNotEmpty(oninvokeMethod)) {
                asyncMethodInfo.setOninvokeMethod(getMethodByName(methodConfig.getOninvoke().getClass(), oninvokeMethod));
            }

            String onreturnMethod = methodConfig.getOnreturnMethod();
            if (StringUtils.isNotEmpty(onreturnMethod)) {
                asyncMethodInfo.setOnreturnMethod(getMethodByName(methodConfig.getOnreturn().getClass(), onreturnMethod));
            }

            String onthrowMethod = methodConfig.getOnthrowMethod();
            if (StringUtils.isNotEmpty(onthrowMethod)) {
                asyncMethodInfo.setOnthrowMethod(getMethodByName(methodConfig.getOnthrow().getClass(), onthrowMethod));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        return asyncMethodInfo;
    }

    //在给定的类中 找到对应的方法
    private static Method getMethodByName(Class<?> clazz, String methodName) {
        try {
            return ReflectUtils.findMethodByMethodName(clazz, methodName);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    //判断参数是否是原始类型
    private static boolean isPrimitive(Class<?> type) {
        return type.isPrimitive()
                || type == String.class
                || type == Character.class
                || type == Boolean.class
                || type == Byte.class
                || type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class
                || type == Object.class;
    }

    /**
     * 将 值 转换为给定的 原始类型
     * @param type
     * @param value
     * @return
     */
    private static Object convertPrimitive(Class<?> type, String value) {
        if (type == char.class || type == Character.class) {
            return value.length() > 0 ? value.charAt(0) : '\0';
        } else if (type == boolean.class || type == Boolean.class) {
            return Boolean.valueOf(value);
        } else if (type == byte.class || type == Byte.class) {
            return Byte.valueOf(value);
        } else if (type == short.class || type == Short.class) {
            return Short.valueOf(value);
        } else if (type == int.class || type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == long.class || type == Long.class) {
            return Long.valueOf(value);
        } else if (type == float.class || type == Float.class) {
            return Float.valueOf(value);
        } else if (type == double.class || type == Double.class) {
            return Double.valueOf(value);
        }
        return value;
    }

    /**
     * 检查延展???
     * @param type
     * @param property
     * @param value
     */
    protected static void checkExtension(Class<?> type, String property, String value) {
        //检验参数
        checkName(property, value);
        if (value != null && value.length() > 0
                && !ExtensionLoader.getExtensionLoader(type).hasExtension(value)) {
            throw new IllegalStateException("No such extension " + value + " for " + property + "/" + type.getName());
        }
    }

    protected static void checkMultiExtension(Class<?> type, String property, String value) {
        checkMultiName(property, value);
        if (value != null && value.length() > 0) {
            String[] values = value.split("\\s*[,]+\\s*");
            for (String v : values) {
                if (v.startsWith(Constants.REMOVE_VALUE_PREFIX)) {
                    v = v.substring(1);
                }
                if (Constants.DEFAULT_KEY.equals(v)) {
                    continue;
                }
                if (!ExtensionLoader.getExtensionLoader(type).hasExtension(v)) {
                    throw new IllegalStateException("No such extension " + v + " for " + property + "/" + type.getName());
                }
            }
        }
    }

    protected static void checkLength(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, null);
    }

    protected static void checkPathLength(String property, String value) {
        checkProperty(property, value, MAX_PATH_LENGTH, null);
    }

    /**
     * 检查名字是否符合正则表达式
     * @param property
     * @param value
     */
    protected static void checkName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_NAME);
    }

    protected static void checkNameHasSymbol(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_NAME_HAS_SYMBOL);
    }

    protected static void checkKey(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_KEY);
    }

    protected static void checkMultiName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_MULTI_NAME);
    }

    protected static void checkPathName(String property, String value) {
        checkProperty(property, value, MAX_PATH_LENGTH, PATTERN_PATH);
    }

    protected static void checkMethodName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_METHOD_NAME);
    }

    protected static void checkParameterName(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return;
        }
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            checkNameHasSymbol(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 检查 给定的 value 是否符合正则表达式
     * @param property 只是用来打印日志的
     * @param value
     * @param maxlength
     * @param pattern
     */
    protected static void checkProperty(String property, String value, int maxlength, Pattern pattern) {
        if (value == null || value.length() == 0) {
            return;
        }
        if (value.length() > maxlength) {
            throw new IllegalStateException("Invalid " + property + "=\"" + value + "\" is longer than " + maxlength);
        }
        if (pattern != null) {
            //value 不匹配
            Matcher matcher = pattern.matcher(value);
            if (!matcher.matches()) {
                throw new IllegalStateException("Invalid " + property + "=\"" + value + "\" contains illegal " +
                        "character, only digit, letter, '-', '_' or '.' is legal.");
            }
        }
    }

    /**
     * 该属性是被排除的 也就是不能获取
     * @return
     */
    @Parameter(excluded = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * 使用 注解类型 和对应的注解对象 初始化
     * @param annotationClass
     * @param annotation
     */
    protected void appendAnnotation(Class<?> annotationClass, Object annotation) {
        Method[] methods = annotationClass.getMethods();
        for (Method method : methods) {
            //获取该方法 所属的 对象 != Object 代表不是从 Object 继承过来的方法 因为 getMethods 会获取所有方法
            if (method.getDeclaringClass() != Object.class
                    && method.getReturnType() != void.class
                    && method.getParameterTypes().length == 0
                    && Modifier.isPublic(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())) {
                try {
                    //获取 方法名
                    String property = method.getName();
                    //特殊情况方法名要转换
                    if ("interfaceClass".equals(property) || "interfaceName".equals(property)) {
                        property = "interface";
                    }
                    //从 注解中 抽出 对应的 属性 并生成对应的 set 方法
                    String setter = "set" + property.substring(0, 1).toUpperCase() + property.substring(1);
                    //获取注解上的属性
                    Object value = method.invoke(annotation);
                    //这个 defaultValue 经测试 基本都是返回 null
                    if (value != null && !value.equals(method.getDefaultValue())) {
                        //将对应的类型 转换成 包装类
                        Class<?> parameterType = ReflectUtils.getBoxedClass(method.getReturnType());
                        //如果 该属性是 监听器 or 拦截器
                        if ("filter".equals(property) || "listener".equals(property)) {
                            //属性值 就 固定为 string
                            parameterType = String.class;
                            //将 返回值 从 string[] 转换成string
                            value = StringUtils.join((String[]) value, ",");
                            //如果是 参数类型
                        } else if ("parameters".equals(property)) {
                            parameterType = Map.class;
                            //string 转换成map
                            value = CollectionUtils.toStringMap((String[]) value);
                        }
                        try {
                            //获取调用该方法的 类对象 并获取生成指定的  setXXX 方法 调用设置属性
                            Method setterMethod = getClass().getMethod(setter, parameterType);
                            setterMethod.invoke(this, value);
                        } catch (NoSuchMethodException e) {
                            // ignore
                        }
                    }
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public String toString() {
        try {
            StringBuilder buf = new StringBuilder();
            buf.append("<dubbo:");
            buf.append(getTagName(getClass()));
            Method[] methods = getClass().getMethods();
            for (Method method : methods) {
                try {
                    String name = method.getName();
                    if ((name.startsWith("get") || name.startsWith("is"))
                            && !"get".equals(name) && !"is".equals(name)
                            && !"getClass".equals(name) && !"getObject".equals(name)
                            && Modifier.isPublic(method.getModifiers())
                            && method.getParameterTypes().length == 0
                            && isPrimitive(method.getReturnType())) {
                        int i = name.startsWith("get") ? 3 : 2;
                        String key = name.substring(i, i + 1).toLowerCase() + name.substring(i + 1);
                        Object value = method.invoke(this);
                        if (value != null) {
                            buf.append(" ");
                            buf.append(key);
                            buf.append("=\"");
                            buf.append(value);
                            buf.append("\"");
                        }
                    }
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            buf.append(" />");
            return buf.toString();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
            return super.toString();
        }
    }

}
