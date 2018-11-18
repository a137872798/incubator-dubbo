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
package org.apache.dubbo.common;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassHelper;

import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Version
 */
//dubbo 的版本对象
public final class Version {
    private static final Logger logger = LoggerFactory.getLogger(Version.class);

    // Dubbo RPC protocol version, for compatibility, it must not be between 2.0.10 ~ 2.6.2
    /**
     * 协议版本
     */
    public static final String DEFAULT_DUBBO_PROTOCOL_VERSION = "2.0.2";
    /**
     * jar 包版本
     */
    // Dubbo implementation version, usually is jar version.
    private static final String VERSION = getVersion(Version.class, "");

    /**
     * For protocol compatibility purpose.
     * Because {@link #isSupportResponseAttatchment} is checked for every call, int compare expect to has higher performance than string.
     */
    //最低版本
    private static final int LOWEST_VERSION_FOR_RESPONSE_ATTATCHMENT = 20002; // 2.0.2
    /**
     * 版本 和 int 的映射
     */
    private static final Map<String, Integer> VERSION2INT = new HashMap<String, Integer>();

    static {
        // check if there's duplicated jar
        Version.checkDuplicate(Version.class);
    }

    private Version() {
    }

    public static String getProtocolVersion() {
        return DEFAULT_DUBBO_PROTOCOL_VERSION;
    }

    public static String getVersion() {
        return VERSION;
    }

    public static boolean isSupportResponseAttatchment(String version) {
        if (version == null || version.length() == 0) {
            return false;
        }
        // for previous dubbo version(2.0.10/020010~2.6.2/020602), this version is the jar's version, so they need to be ignore
        int iVersion = getIntVersion(version);
        if (iVersion >= 20010 && iVersion <= 20602) {
            return false;
        }

        return iVersion >= LOWEST_VERSION_FOR_RESPONSE_ATTATCHMENT;
    }

    public static int getIntVersion(String version) {
        Integer v = VERSION2INT.get(version);
        if (v == null) {
            v = parseInt(version);
            VERSION2INT.put(version, v);
        }
        return v;
    }

    private static int parseInt(String version) {
        int v = 0;
        String[] vArr = version.split("\\.");
        int len = vArr.length;
        for (int i = 0; i < len; i++) {
            v += Integer.parseInt(getDigital(vArr[i])) * Math.pow(10, (len - i - 1) * 2);
        }
        return v;
    }

    private static String getDigital(String v) {
        int index = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (Character.isDigit(c)) {
                if (i == v.length() - 1) {
                    index = i + 1;
                } else {
                    index = i;
                }
                continue;
            } else {
                index = i;
                break;
            }
        }
        return v.substring(0, index);
    }

    private static boolean hasResource(String path) {
        try {
            return Version.class.getClassLoader().getResource(path) != null;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * 获取 jar 包版本
     * @param cls
     * @param defaultVersion
     * @return
     */
    public static String getVersion(Class<?> cls, String defaultVersion) {
        try {
            // find version info from MANIFEST.MF first
            // 直接从包中获取版本
            String version = cls.getPackage().getImplementationVersion();
            if (version == null || version.length() == 0) {
                //获取明确版本
                version = cls.getPackage().getSpecificationVersion();
            }
            if (version == null || version.length() == 0) {
                // guess version fro jar file name if nothing's found from MANIFEST.MF
                CodeSource codeSource = cls.getProtectionDomain().getCodeSource();
                if (codeSource == null) {
                    logger.info("No codeSource for class " + cls.getName() + " when getVersion, use default version " + defaultVersion);
                } else {
                    //从这里获取 版本信息
                    String file = codeSource.getLocation().getFile();
                    if (file != null && file.length() > 0 && file.endsWith(".jar")) {
                        file = file.substring(0, file.length() - 4);
                        int i = file.lastIndexOf('/');
                        if (i >= 0) {
                            file = file.substring(i + 1);
                        }
                        i = file.indexOf("-");
                        if (i >= 0) {
                            file = file.substring(i + 1);
                        }
                        while (file.length() > 0 && !Character.isDigit(file.charAt(0))) {
                            i = file.indexOf("-");
                            if (i >= 0) {
                                file = file.substring(i + 1);
                            } else {
                                break;
                            }
                        }
                        version = file;
                    }
                }
            }
            // return default version if no version info is found
            //不存在 返回默认版本号
            return version == null || version.length() == 0 ? defaultVersion : version;
        } catch (Throwable e) {
            // return default version when any exception is thrown
            logger.error("return default version, ignore exception " + e.getMessage(), e);
            return defaultVersion;
        }
    }

    public static void checkDuplicate(Class<?> cls, boolean failOnError) {
        checkDuplicate(cls.getName().replace('.', '/') + ".class", failOnError);
    }

    public static void checkDuplicate(Class<?> cls) {
        checkDuplicate(cls, false);
    }

    public static void checkDuplicate(String path, boolean failOnError) {
        try {
            // search in caller's classloader
            Enumeration<URL> urls = ClassHelper.getCallerClassLoader(Version.class).getResources(path);
            Set<String> files = new HashSet<String>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        files.add(file);
                    }
                }
            }
            // duplicated jar is found
            if (files.size() > 1) {
                String error = "Duplicate class " + path + " in " + files.size() + " jar " + files;
                if (failOnError) {
                    throw new IllegalStateException(error);
                } else {
                    logger.error(error);
                }
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

}
