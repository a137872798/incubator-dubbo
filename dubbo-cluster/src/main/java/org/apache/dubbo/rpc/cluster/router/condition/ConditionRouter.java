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
package org.apache.dubbo.rpc.cluster.router.condition;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ConditionRouter
 *
 * 条件路由对象
 */
public class ConditionRouter implements Router, Comparable<Router> {

    private static final Logger logger = LoggerFactory.getLogger(ConditionRouter.class);
    private static Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");
    /**
     * 路由对象的url 属性
     */
    private final URL url;
    /**
     * 优先级
     */
    private final int priority;
    /**
     * 当路由结果为空时，是否强制执行，如果不强制执行，路由结果为空的路由规则将自动失效，可不填，缺省为 false 。
     */
    private final boolean force;
    /**
     * 消费者匹配条件集合，通过解析【条件表达式 rule 的 `=>` 之前半部分】
     */
    private final Map<String, MatchPair> whenCondition;
    /**
     * 提供者地址列表的过滤条件，通过解析【条件表达式 rule 的 `=>` 之后半部分】
     */
    private final Map<String, MatchPair> thenCondition;

    public ConditionRouter(URL url) {
        this.url = url;
        //从url 中 获取相关配置
        this.priority = url.getParameter(Constants.PRIORITY_KEY, 0);
        this.force = url.getParameter(Constants.FORCE_KEY, false);
        try {
            //获取 rule 信息
            String rule = url.getParameterAndDecoded(Constants.RULE_KEY);
            //不存在 路由规则 直接抛出异常
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            //将消费者 和 提供者 替换成 ""
            rule = rule.replace("consumer.", "").replace("provider.", "");
            int i = rule.indexOf("=>");
            //将 => 前后的 数据分别拆出来
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();
            //为空 或是 默认值 就设置 空对象 否则 解析数据实体
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);
            // NOTE: It should be determined on the business level whether the `When condition` can be empty or not.
            // 将解析后的结果赋值
            this.whenCondition = when;
            this.thenCondition = then;
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 解析 rule 对象  host = 10.20.153.10 => host = 10.20.153.11
     * @param rule
     * @return
     * @throws ParseException
     */
    private static Map<String, MatchPair> parseRule(String rule)
            throws ParseException {
        Map<String, MatchPair> condition = new HashMap<String, MatchPair>();
        //如果为空 返回空容器对象
        if (StringUtils.isBlank(rule)) {
            return condition;
        }
        // Key-Value pair, stores both match and mismatch conditions
        MatchPair pair = null;
        // Multiple values
        Set<String> values = null;
        //这个正则是匹配特殊符号的
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);
        while (matcher.find()) { // Try to match one by one
            //匹配到的 特殊符号
            String separator = matcher.group(1);
            //符号后的内容
            String content = matcher.group(2);
            // Start part of the condition expression. 代表的是 分隔符之前的 部分 如：host
            if (separator == null || separator.length() == 0) {
                //如果没有匹配到分隔符
                pair = new MatchPair();
                //将 host 和 空对象存入
                condition.put(content, pair);
            }
            // The KV part of the condition expression
            //如果是以 & 拆分  代表是新的一对 数据了 例如 & port 100
            else if ("&".equals(separator)) {
                //添加一组新的 键值对
                if (condition.get(content) == null) {
                    pair = new MatchPair();
                    condition.put(content, pair);
                } else {
                    pair = condition.get(content);
                }
            }
            // The Value in the KV part.
            //如果是 = 拆分
            else if ("=".equals(separator)) {
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                //例如 这次匹配到的是 = 10.20.153.11
                values = pair.matches;
                //这次就是针对 host 的 匹配结果增加 10.20.153.11
                values.add(content);
            }
            // The Value in the KV part.
            else if ("!=".equals(separator)) {
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                //增加 丢弃的 content
                values = pair.mismatches;
                values.add(content);
            }
            // The Value in the KV part, if Value have more than one items.
            else if (",".equals(separator)) { // Should be seperateed by ','
                if (values == null || values.isEmpty()) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }
                //紧接着 上次匹配到的分隔符类型继续进行 添加
                values.add(content);
            } else {
                throw new ParseException("Illegal route rule \"" + rule
                        + "\", The error char '" + separator + "' at index "
                        + matcher.start() + " before \"" + content + "\".", matcher.start());
            }
        }
        return condition;
    }

    /**
     * 进行路由
     * @param invokers
     * @param url        refer url
     * @param invocation
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation)
            throws RpcException {
        if (invokers == null || invokers.isEmpty()) {
            return invokers;
        }
        try {
            //匹配前段部分 不匹配 直接返回 原数据
            if (!matchWhen(url, invocation)) {
                return invokers;
            }
            //如果不需要匹配 直接返回
            List<Invoker<T>> result = new ArrayList<Invoker<T>>();
            if (thenCondition == null) {
                logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                return result;
            }
            //将invoker 对象的 url 与 url 做匹配
            for (Invoker<T> invoker : invokers) {
                if (matchThen(invoker.getUrl(), url)) {
                    //代表匹配该路由的数据
                    result.add(invoker);
                }
            }
            if (!result.isEmpty()) {
                return result;
                //如果必须要返回 就返回一组空的 并打印日志
            } else if (force) {
                logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(Constants.RULE_KEY));
                return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        //匹配不到路由信息也是全数返回
        return invokers;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != ConditionRouter.class) {
            return 1;
        }
        ConditionRouter c = (ConditionRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }

    /**
     * 匹配前段部分
     * @param url
     * @param invocation
     * @return
     */
    boolean matchWhen(URL url, Invocation invocation) {
        return whenCondition == null || whenCondition.isEmpty() || matchCondition(whenCondition, url, null, invocation);
    }

    /**
     * 匹配后半部分
     * @param url
     * @param param
     * @return
     */
    private boolean matchThen(URL url, URL param) {
        return !(thenCondition == null || thenCondition.isEmpty()) && matchCondition(thenCondition, url, param, null);
    }

    /**
     * 匹配条件  如果 匹配属性以$ 开头 就是将 url 与 param 中的属性进行匹配
     * @param condition 数据 以键值对形式 存在 MatchPair 中保存了 匹配的和 排除的数据
     * @param url 传入 的需要匹配的 资源
     * @param param 传入一组辅助参数 如果 match 数据是以$开头就代表要从 这里取数据
     * @param invocation
     * @return
     */
    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param, Invocation invocation) {
        //将url中属性 以map形式返回
        Map<String, String> sample = url.toMap();
        boolean result = false;
        for (Map.Entry<String, MatchPair> matchPair : condition.entrySet()) {
            //返回属性key
            String key = matchPair.getKey();
            String sampleValue;
            //get real invoked method name from invocation
            //如果 key 是方法名 就从invocation 上获得方法名
            if (invocation != null && (Constants.METHOD_KEY.equals(key) || Constants.METHODS_KEY.equals(key))) {
                sampleValue = invocation.getMethodName();
            } else {
                //否则从 url 中获取对应属性
                sampleValue = sample.get(key);
                if (sampleValue == null) {
                    sampleValue = sample.get(Constants.DEFAULT_KEY_PREFIX + key);
                }
            }
            if (sampleValue != null) {
                if (!matchPair.getValue().isMatch(sampleValue, param)) {
                    return false;
                } else {
                    result = true;
                }
            } else {
                //not pass the condition
                //没有找到数据就看看这里是不是空的
                if (!matchPair.getValue().matches.isEmpty()) {
                    return false;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * 存放匹配结果的对象
     */
    private static final class MatchPair {
        /**
         * 匹配到的数据
         */
        final Set<String> matches = new HashSet<String>();
        /**
         * 匹配到 != 的数据
         */
        final Set<String> mismatches = new HashSet<String>();

        /**
         * 判断是否匹配
         * @param value
         * @param param
         * @return
         */
        private boolean isMatch(String value, URL param) {
            // != 的 数据要为null 且 = 的数据要不为null
            if (!matches.isEmpty() && mismatches.isEmpty()) {
                for (String match : matches) {
                    //遍历每个 匹配到的数据
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                //没匹配上返回false
                return false;
            }

            // 只匹配 != 的数据
            if (!mismatches.isEmpty() && matches.isEmpty()) {
                for (String mismatch : mismatches) {
                    //这里 逻辑刚好相反找到匹配的 就 返回false 代表在!= 中找到了 结果就是匹配不上
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                //没找到 代表能匹配
                return true;
            }

            //同时匹配 matches 和 mismatches
            if (!matches.isEmpty() && !mismatches.isEmpty()) {
                //when both mismatches and matches contain the same value, then using mismatches first
                for (String mismatch : mismatches) {
                    //在排除列表中找到就直接  返回false
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                //在 匹配列表中也没有找到 就直接返回false
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }
            return false;
        }
    }
}
