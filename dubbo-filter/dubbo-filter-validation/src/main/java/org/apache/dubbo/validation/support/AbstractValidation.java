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
package org.apache.dubbo.validation.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.validation.Validation;
import org.apache.dubbo.validation.Validator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * AbstractValidation
 * 默认的 过滤器工厂对象
 */
public abstract class AbstractValidation implements Validation {

    /**
     * 保存url 与过滤器的 关联关系 做缓存功能  这些缓存不做清理吗???
     */
    private final ConcurrentMap<String, Validator> validators = new ConcurrentHashMap<String, Validator>();

    /**
     * 获取 过滤器对象
     * @param url
     * @return
     */
    @Override
    public Validator getValidator(URL url) {
        //将url 转换成 key
        String key = url.toFullString();
        //尝试从缓存中获取
        Validator validator = validators.get(key);
        if (validator == null) {
            //没有获取到 就创建 并保存
            validators.put(key, createValidator(url));
            validator = validators.get(key);
        }
        return validator;
    }

    protected abstract Validator createValidator(URL url);

}
