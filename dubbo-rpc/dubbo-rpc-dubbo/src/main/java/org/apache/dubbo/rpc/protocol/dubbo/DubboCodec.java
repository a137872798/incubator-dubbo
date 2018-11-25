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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.codec.ExchangeCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.encodeInvocationArgument;

/**
 * Dubbo codec.
 *
 * dubbo的 编解码对象
 */
public class DubboCodec extends ExchangeCodec implements Codec2 {

    /**
     * 协议名
     */
    public static final String NAME = "dubbo";
    /**
     * 版本号
     */
    public static final String DUBBO_VERSION = Version.getProtocolVersion();
    /**
     * 异常响应
     */
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    /**
     * 正常响应
     */
    public static final byte RESPONSE_VALUE = 1;
    /**
     * 正常无结果响应
     */
    public static final byte RESPONSE_NULL_VALUE = 2;
    //增加 attachment 属性
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    /**
     * 方法参数为空
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    /**
     * 方法类型为空
     */
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);

    /**
     * 解码  这是 覆盖了父类的方法
     * @param channel
     * @param is
     * @param header 包含了包括 head 和 body
     * @return
     * @throws IOException
     */
    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        //从[2] 元素中获取标识信息 并获取 序列化方式
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.  [4-11] 存的是 请求id
        long id = Bytes.bytes2long(header, 4);
        //代表是响应类型
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);
            //如果 存在 event 就设置心跳事件 因为响应 好像只有 心跳事件
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // get status. 代表本次请求是成功还是失败
            byte status = header[3];
            res.setStatus(status);
            try {
                //反序列化
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                //响应状态是 成功时
                if (status == Response.OK) {
                    Object data;
                    //如果是心跳类型
                    if (res.isHeartbeat()) {
                        //解析心跳包 就是通过 dubbo封装的 对象流读取
                        data = decodeHeartbeatData(channel, in);
                    } else if (res.isEvent()) {
                        //解析事件数据 也是直接从对象流中获取数据
                        data = decodeEventData(channel, in);
                    } else {
                        //创建 结果对象
                        DecodeableRpcResult result;
                        //如果是在 通信框架的 io 线程中解码 不懂???
                        if (channel.getUrl().getParameter(
                                Constants.DECODE_IN_IO_THREAD_KEY,
                                Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                            result = new DecodeableRpcResult(channel, res, is,
                                    //通过id 定位 到 future 然后定位到针对本次请求id 的req 对象并获取里面的  mData
                                    (Invocation) getRequestData(id), proto);
                            //执行解码
                            result.decode();
                        } else {
                            //在 dubbo 的线程中 使用DecoderHandler解码
                            result = new DecodeableRpcResult(channel, res,
                                    //从is 中读取数据并包装到 unsafe 对象中 但是好像没有进行解码
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    res.setResult(data);
                } else {
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode response failed: " + t.getMessage(), t);
                }
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.
            //解析请求 数据
            Request req = new Request(id);
            //设置协议版本
            req.setVersion(Version.getProtocolVersion());
            //设置 是否需要 响应结果
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            //存在事件 就设置心跳检测
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                Object data;
                //解析 输入流 获取 dubbo 封装的  对象流
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                if (req.isHeartbeat()) {
                    data = decodeHeartbeatData(channel, in);
                } else if (req.isEvent()) {
                    data = decodeEventData(channel, in);
                } else {
                    DecodeableRpcInvocation inv;
                    //和 响应的 类似 不过 使用的对象不同 响应是 DecodeableRpcResult
                    if (channel.getUrl().getParameter(
                            Constants.DECODE_IN_IO_THREAD_KEY,
                            Constants.DEFAULT_DECODE_IN_IO_THREAD)) {
                        inv = new DecodeableRpcInvocation(channel, req, is, proto);
                        inv.decode();
                    } else {
                        inv = new DecodeableRpcInvocation(channel, req,
                                new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                req.setData(data);
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // bad request
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    /**
     * 从 输入流中读取数据
     * @param is
     * @return
     * @throws IOException
     */
    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            //读取逻辑
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    /**
     * 为请求数据编码
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 为响应结果 编码
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    /**
     * 为请求数据编码
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        //将传入参数 改成 RPC invocation对象
        RpcInvocation inv = (RpcInvocation) data;

        //写入dubbo版本 path version
        out.writeUTF(version);
        out.writeUTF(inv.getAttachment(Constants.PATH_KEY));
        out.writeUTF(inv.getAttachment(Constants.VERSION_KEY));

        //写入方法名和 参数类型
        out.writeUTF(inv.getMethodName());
        out.writeUTF(ReflectUtils.getDesc(inv.getParameterTypes()));
        //获取参数列表
        Object[] args = inv.getArguments();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                //将 参数写入到 out中 这个参数会根据是否是异步决定是否直接写入
                out.writeObject(encodeInvocationArgument(channel, inv, i));
            }
        }

        //将inv 中无关异步信息移除后 写入
        out.writeObject(RpcUtils.getNecessaryAttachments(inv));
    }

    /**
     * 为响应结果编码
     * @param channel
     * @param out
     * @param data
     * @param version dubbo 版本
     * @throws IOException
     */
    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        //将结果转为 对应的类型
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        // 版本是否允许 attach
        boolean attach = Version.isSupportResponseAttatchment(version);
        Throwable th = result.getException();
        //代表本次请求是正常的
        if (th == null) {
            //获取 返回的结果
            Object ret = result.getValue();
            if (ret == null) {
                //根据 是否 支持写入 是否包含 with_attachment 的 枚举
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeObject(th);
        }

        //如果 允许 attach
        if (attach) {
            // returns current version of Response to consumer side.
            //在 attachment 中增加一个 dubbo 版本信息
            result.getAttachments().put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeObject(result.getAttachments());
        }
    }
}
