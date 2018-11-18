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
package org.apache.dubbo.config.support;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Parameter
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Parameter {

    /**
     * 标识
     * @return
     */
    String key() default "";

    /**
     * 是否必填
     * @return
     */
    boolean required() default false;

    /**
     * 是否忽略
     * @return
     */
    boolean excluded() default false;

    /**
     * 是否转义
     * @return
     */
    boolean escaped() default false;

    /**
     * 标明该字段是否是  属性 这样就可以使用 appendAttribute 了
     * @return
     */
    boolean attribute() default false;

    /**
     * 设置属性是 追加还是 覆盖之前的属性
     * @return
     */
    boolean append() default false;

}