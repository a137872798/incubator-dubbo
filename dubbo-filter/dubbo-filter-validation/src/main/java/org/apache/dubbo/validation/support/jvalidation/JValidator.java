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
package org.apache.dubbo.validation.support.jvalidation;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.ClassGenerator;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.validation.MethodValidated;
import org.apache.dubbo.validation.Validator;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.ByteMemberValue;
import javassist.bytecode.annotation.CharMemberValue;
import javassist.bytecode.annotation.ClassMemberValue;
import javassist.bytecode.annotation.DoubleMemberValue;
import javassist.bytecode.annotation.EnumMemberValue;
import javassist.bytecode.annotation.FloatMemberValue;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.LongMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.ShortMemberValue;
import javassist.bytecode.annotation.StringMemberValue;

import javax.validation.Constraint;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.validation.groups.Default;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JValidator
 *
 * javax 自带 的 过滤器对象
 */
public class JValidator implements Validator {

    private static final Logger logger = LoggerFactory.getLogger(JValidator.class);

    /**
     * 要过滤的目标服务接口
     */
    private final Class<?> clazz;

    private final Map<String, Class> methodClassMap;

    /**
     * 过滤器对象
     */
    private final javax.validation.Validator validator;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public JValidator(URL url) {
        this.clazz = ReflectUtils.forName(url.getServiceInterface());
        //查看有没有相关配置信息  这个应该是过滤器工厂类的全限定名
        String jvalidation = url.getParameter("jvalidation");
        ValidatorFactory factory;
        if (jvalidation != null && jvalidation.length() > 0) {
            //这里是 javax的 实现 大体就是根据传入的全限定名生成 过滤器工厂后 构建对象
            factory = Validation.byProvider((Class) ReflectUtils.forName(jvalidation)).configure().buildValidatorFactory();
        } else {
            //没有设置就使用默认的 过滤器工厂
            factory = Validation.buildDefaultValidatorFactory();
        }
        //获取 过滤器对象 真正的过滤动作委托给了这个对象
        this.validator = factory.getValidator();
        this.methodClassMap = new ConcurrentHashMap<String, Class>();
    }

    /**
     * 判断给定class 是否是数组对象
     * @param cls
     * @return
     */
    private static boolean isPrimitives(Class<?> cls) {
        if (cls.isArray()) {
            //如果是数组对象 就先变成获取 单个元素 然后再判断是否是原始类型
            return isPrimitive(cls.getComponentType());
        }
        return isPrimitive(cls);
    }

    /**
     * 判断是否是原始类型
     * @param cls
     * @return
     */
    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == String.class || cls == Boolean.class || cls == Character.class
                || Number.class.isAssignableFrom(cls) || Date.class.isAssignableFrom(cls);
    }

    /**
     *
     * @param clazz 服务接口类型
     * @param method 服务方法
     * @param args 该方法需要的参数
     * @return
     */
    private static Object getMethodParameterBean(Class<?> clazz, Method method, Object[] args) {
        //判断 方法中每个参数是否有 COnstraint 注解
        if (!hasConstraintParameter(method)) {
            return null;
        }
        try {
            //获取 类名 方法名参数列表拼接后的字符串
            String parameterClassName = generateMethodParameterClassName(clazz, method);
            Class<?> parameterClass;
            try {
                //反射创建的类
                parameterClass = (Class<?>) Class.forName(parameterClassName, true, clazz.getClassLoader());
            } catch (ClassNotFoundException e) {
                //没有找到 该对象 通过  javassist 创建动态类对象
                ClassPool pool = ClassGenerator.getClassPool(clazz.getClassLoader());
                //创建对应类名的对象
                CtClass ctClass = pool.makeClass(parameterClassName);
                ClassFile classFile = ctClass.getClassFile();
                classFile.setVersionToJava5();
                //添加默认构造器
                ctClass.addConstructor(CtNewConstructor.defaultConstructor(pool.getCtClass(parameterClassName)));
                // parameter fields  获取方法的参数信息
                Class<?>[] parameterTypes = method.getParameterTypes();
                //获取方法的 所有参数的注解
                Annotation[][] parameterAnnotations = method.getParameterAnnotations();
                for (int i = 0; i < parameterTypes.length; i++) {
                    //参数类型
                    Class<?> type = parameterTypes[i];
                    //参数对应的 注解数组
                    Annotation[] annotations = parameterAnnotations[i];
                    //javassist 的类 应该是 构建 注解的
                    AnnotationsAttribute attribute = new AnnotationsAttribute(classFile.getConstPool(), AnnotationsAttribute.visibleTag);
                    for (Annotation annotation : annotations) {
                        if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                            javassist.bytecode.annotation.Annotation ja = new javassist.bytecode.annotation.Annotation(
                                    classFile.getConstPool(), pool.getCtClass(annotation.annotationType().getName()));
                            Method[] members = annotation.annotationType().getMethods();
                            for (Method member : members) {
                                if (Modifier.isPublic(member.getModifiers())
                                        && member.getParameterTypes().length == 0
                                        && member.getDeclaringClass() == annotation.annotationType()) {
                                    Object value = member.invoke(annotation, new Object[0]);
                                    if (null != value) {
                                        MemberValue memberValue = createMemberValue(
                                                classFile.getConstPool(), pool.get(member.getReturnType().getName()), value);
                                        ja.addMemberValue(member.getName(), memberValue);
                                    }
                                }
                            }
                            attribute.addAnnotation(ja);
                        }
                    }
                    String fieldName = method.getName() + "Argument" + i;
                    CtField ctField = CtField.make("public " + type.getCanonicalName() + " " + fieldName + ";", pool.getCtClass(parameterClassName));
                    ctField.getFieldInfo().addAttribute(attribute);
                    ctClass.addField(ctField);
                }
                parameterClass = ctClass.toClass(clazz.getClassLoader(), null);
            }
            Object parameterBean = parameterClass.newInstance();
            for (int i = 0; i < args.length; i++) {
                Field field = parameterClass.getField(method.getName() + "Argument" + i);
                field.set(parameterBean, args[i]);
            }
            return parameterBean;
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 构建类名_方法名_参数列表的  字符串
     * @param clazz
     * @param method
     * @return
     */
    private static String generateMethodParameterClassName(Class<?> clazz, Method method) {
        StringBuilder builder = new StringBuilder().append(clazz.getName())
                .append("_")
                .append(toUpperMethoName(method.getName()))
                .append("Parameter");

        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            builder.append("_").append(parameterType.getName());
        }

        return builder.toString();
    }

    /**
     * 获取 服务方调用的方法对象
     * @param method
     * @return
     */
    private static boolean hasConstraintParameter(Method method) {
        //获取该方法的每个参数 的注解
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        if (parameterAnnotations != null && parameterAnnotations.length > 0) {
            //二层循环 判断 每个参数 是否有 Constraint 注解  这个注解是 javax 自带的
            for (Annotation[] annotations : parameterAnnotations) {
                for (Annotation annotation : annotations) {
                    if (annotation.annotationType().isAnnotationPresent(Constraint.class)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 将方法首字母大写
     * @param methodName
     * @return
     */
    private static String toUpperMethoName(String methodName) {
        return methodName.substring(0, 1).toUpperCase() + methodName.substring(1);
    }

    // Copy from javassist.bytecode.annotation.Annotation.createMemberValue(ConstPool, CtClass);
    private static MemberValue createMemberValue(ConstPool cp, CtClass type, Object value) throws NotFoundException {
        MemberValue memberValue = javassist.bytecode.annotation.Annotation.createMemberValue(cp, type);
        if (memberValue instanceof BooleanMemberValue) {
            ((BooleanMemberValue) memberValue).setValue((Boolean) value);
        } else if (memberValue instanceof ByteMemberValue) {
            ((ByteMemberValue) memberValue).setValue((Byte) value);
        } else if (memberValue instanceof CharMemberValue) {
            ((CharMemberValue) memberValue).setValue((Character) value);
        } else if (memberValue instanceof ShortMemberValue) {
            ((ShortMemberValue) memberValue).setValue((Short) value);
        } else if (memberValue instanceof IntegerMemberValue) {
            ((IntegerMemberValue) memberValue).setValue((Integer) value);
        } else if (memberValue instanceof LongMemberValue) {
            ((LongMemberValue) memberValue).setValue((Long) value);
        } else if (memberValue instanceof FloatMemberValue) {
            ((FloatMemberValue) memberValue).setValue((Float) value);
        } else if (memberValue instanceof DoubleMemberValue) {
            ((DoubleMemberValue) memberValue).setValue((Double) value);
        } else if (memberValue instanceof ClassMemberValue) {
            ((ClassMemberValue) memberValue).setValue(((Class<?>) value).getName());
        } else if (memberValue instanceof StringMemberValue) {
            ((StringMemberValue) memberValue).setValue((String) value);
        } else if (memberValue instanceof EnumMemberValue) {
            ((EnumMemberValue) memberValue).setValue(((Enum<?>) value).name());
        }
        /* else if (memberValue instanceof AnnotationMemberValue) */
        else if (memberValue instanceof ArrayMemberValue) {
            CtClass arrayType = type.getComponentType();
            int len = Array.getLength(value);
            MemberValue[] members = new MemberValue[len];
            for (int i = 0; i < len; i++) {
                members[i] = createMemberValue(cp, arrayType, Array.get(value, i));
            }
            ((ArrayMemberValue) memberValue).setValue(members);
        }
        return memberValue;
    }

    /**
     * 过滤的 核心逻辑
     * @param methodName
     * @param parameterTypes
     * @param arguments
     * @throws Exception
     */
    @Override
    public void validate(String methodName, Class<?>[] parameterTypes, Object[] arguments) throws Exception {
        List<Class<?>> groups = new ArrayList<Class<?>>();
        //根据方法名 获取 拓展对象
        Class<?> methodClass = methodClass(methodName);
        if (methodClass != null) {
            //添加到容器中
            groups.add(methodClass);
        }
        Set<ConstraintViolation<?>> violations = new HashSet<ConstraintViolation<?>>();
        //获取指定方法
        Method method = clazz.getMethod(methodName, parameterTypes);
        Class<?>[] methodClasses = null;
        //获取方法上的 methodValidate 注解
        if (method.isAnnotationPresent(MethodValidated.class)){
            methodClasses = method.getAnnotation(MethodValidated.class).value();
            //这里代表需要被哪些过滤组过滤  @MethodValidated({Save.class, Update.class})
            groups.addAll(Arrays.asList(methodClasses));
        }
        // add into default group  添加 默认的过滤组
        //这个是 javax 的接口  这个指定下标的添加 不会删除掉旧的元素
        groups.add(0, Default.class);
        groups.add(1, clazz);

        // convert list to array
        Class<?>[] classgroups = groups.toArray(new Class[0]);

        //通过 服务接口类 调用的方法 和参数 创建了一个特殊的类 因为涉及到 javassist 实现的动态代理就先不看了 反正返回了可以被 校验的 对象
        Object parameterBean = getMethodParameterBean(clazz, method, arguments);
        if (parameterBean != null) {
            violations.addAll(validator.validate(parameterBean, classgroups ));
        }

        for (Object arg : arguments) {
            validate(violations, arg, classgroups);
        }

        if (!violations.isEmpty()) {
            logger.error("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations);
            throw new ConstraintViolationException("Failed to validate service: " + clazz.getName() + ", method: " + methodName + ", cause: " + violations, violations);
        }
    }

    /**
     * 传入方法名获取 拓展的 类名$方法名  的类对象
     * @param methodName
     * @return
     */
    private Class methodClass(String methodName) {
        Class<?> methodClass = null;
        //获取 服务接口类名 拼接上方法名
        String methodClassName = clazz.getName() + "$" + toUpperMethoName(methodName);
        //从容器中获取对应类对象
        Class cached = methodClassMap.get(methodClassName);
        if (cached != null) {
            //如果是接口类自身就 返回null
            return cached == clazz ? null : cached;
        }
        try {
            //反射创建对应的类并保存到容器中
            methodClass = Class.forName(methodClassName, false, Thread.currentThread().getContextClassLoader());
            methodClassMap.put(methodClassName, methodClass);
        } catch (ClassNotFoundException e) {
            methodClassMap.put(methodClassName, clazz);
        }
        return methodClass;
    }

    /**
     * 这里对对象进行校验
     * @param violations
     * @param arg 被过滤的参数对象
     * @param groups
     */
    private void validate(Set<ConstraintViolation<?>> violations, Object arg, Class<?>... groups) {
        if (arg != null && !isPrimitives(arg.getClass())) {
            if (Object[].class.isInstance(arg)) {
                for (Object item : (Object[]) arg) {
                    validate(violations, item, groups);
                }
            } else if (Collection.class.isInstance(arg)) {
                for (Object item : (Collection<?>) arg) {
                    validate(violations, item, groups);
                }
            } else if (Map.class.isInstance(arg)) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) arg).entrySet()) {
                    validate(violations, entry.getKey(), groups);
                    validate(violations, entry.getValue(), groups);
                }
            } else {
                violations.addAll(validator.validate(arg, groups));
            }
        }
    }

}
