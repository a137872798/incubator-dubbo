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
package org.apache.dubbo.remoting.exchange.codec;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.StreamUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.telnet.codec.TelnetCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.IOException;
import java.io.InputStream;

/**
 * ExchangeCodec.
 *
 * exchange 编解码器
 */
public class ExchangeCodec extends TelnetCodec {

    /**
     * 请求头长度 16字节
     * header length.
     */
    protected static final int HEADER_LENGTH = 16;
    /**
     * 协议头 魔法数 2字节
     * magic header.
     */
    protected static final short MAGIC = (short) 0xdabb;
    /**
     * 将 魔法数(2字节) 拆分 出 高位 和 低位数
     */
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];
    // message flag.
    //标记消息标识的位置 80*16 = 128
    protected static final byte FLAG_REQUEST = (byte) 0x80;
    //64
    protected static final byte FLAG_TWOWAY = (byte) 0x40;
    //32
    protected static final byte FLAG_EVENT = (byte) 0x20;
    //31
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    /**
     * 编码
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        //request 和 response 调用不同的 编码逻辑
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        //针对响应结果进行编码
        } else if (msg instanceof Response) {
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            //不符合 交换层 的 对象 通过上层进行编码 交换层 是基于 request response的
            super.encode(channel, buffer, msg);
        }
    }

    /**
     * 解码
     * @param channel
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        //将数据 读取到 header 数组中
        buffer.readBytes(header);
        //解码
        return decode(channel, buffer, readable, header);
    }

    /**
     * 解码
     * @param channel
     * @param buffer   存放原数据的 容器
     * @param readable  buffer 的 可读数据
     * @param header  存放提取数据后的 byte[]
     * @return
     * @throws IOException
     */
    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // check magic number. 对比 魔法数 高低位是否正确 错误时
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {
            int length = header.length;
            //代表被 裁剪了一部分
            if (header.length < readable) {
                //为 header 扩容
                header = Bytes.copyOf(header, readable);
                //读取 剩余的数据
                buffer.readBytes(header, length, readable - length);
            }
            //尝试从数据流中找到匹配 魔法数的部分 应该是 针对 拆包和粘包
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    //从 该位置 开始读取数据
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    //从能读取到魔法数的地方开始 读取数据
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            //读取完后 通过上层解码
            return super.decode(channel, buffer, readable, header);
        }
        // check length.
        // 如果 正好能匹配上 魔法数 代表下层 处理了粘包拆包问题 但是长度不够 代表 需要更多的输入
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // get data length.
        // 从12 字节开始将数据转换成int 就是获取 byte数组12，13，14，15 进行位 计算后在加起来
        int len = Bytes.bytes2int(header, 12);
        //检查负荷量 当长度 超过给定负荷时抛出异常
        checkPayload(channel, len);

        //长度 加 头部 得到总长度
        int tt = len + HEADER_LENGTH;
        //可读长度不满 tt 需要更多 这里是 头部和魔法数对上了所以可以直接用readable 否则是不能直接用的
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        //这里代表可以正常读取 将buffer 包装成一个 输入流
        // limit input stream.
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            //开始解析body
            return decodeBody(channel, is, header);
        } finally {
            //如果数据没有用完 也就是 有多余数据
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    //跳到末尾
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 解析数据体
     * @param channel
     * @param is
     * @param header 包含了包括 head 和 body
     * @return
     * @throws IOException
     */
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        //解析 包含特殊标识的 [2]  通过解析 序列化标识 获取 方式
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        //[4-11] 是请求id
        long id = Bytes.bytes2long(header, 4);
        //代表是响应对象
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);
            //代表是 事件对象 而 响应头 只有 心跳检测是事件
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // get status. 代表 该次请求是成功还是失败
            byte status = header[3];
            res.setStatus(status);
            try {
                //通过 给定的 序列化方式 和 输入流 将数据 解析成 对象 输入流中保存了 head + body
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                if (status == Response.OK) {
                    //这些都是 操作 对象流 现在还不清楚实现 先不管
                    Object data;
                    if (res.isHeartbeat()) {
                        data = decodeHeartbeatData(channel, in);
                    } else if (res.isEvent()) {
                        data = decodeEventData(channel, in);
                    } else {
                        data = decodeResponseData(channel, in, getRequestData(id));
                    }
                    res.setResult(data);
                } else {
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                Object data;
                if (req.isHeartbeat()) {
                    data = decodeHeartbeatData(channel, in);
                } else if (req.isEvent()) {
                    data = decodeEventData(channel, in);
                } else {
                    data = decodeRequestData(channel, in);
                }
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                // 抛出异常的时候 设置 broken 为true 且 设置data
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    protected Object getRequestData(long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null) {
            return null;
        }
        Request req = future.getRequest();
        if (req == null) {
            return null;
        }
        return req.getData();
    }

    /**
     * 解析请求对象
     * @param channel
     * @param buffer
     * @param req
     * @throws IOException
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        //通过channel中的url 对象获取 序列化对象
        Serialization serialization = getSerialization(channel);
        // header.
        byte[] header = new byte[HEADER_LENGTH];
        // set magic number. 将 MAGIC 拆分成2个byte 并设置到给定的 header 对象
        Bytes.short2bytes(MAGIC, header);

        // set request and serialization flag.
        // contentTypeId 记录的 是 序列化类型的 每一种对应一个数值  与 FLAG_REQUEST 做 | 运算后保存到第三位
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());

        //这里还设置了双向以及 是否是事件的标识
        if (req.isTwoWay()) {
            header[2] |= FLAG_TWOWAY;
        }
        if (req.isEvent()) {
            header[2] |= FLAG_EVENT;
        }

        // set request id. 将long 转换成8位 byte 保存到 header中 这里是 4-11 字节
        Bytes.long2bytes(req.getId(), header, 4);

        // encode request data.
        //记录当前的 下标 现在应该还是0 可能 传过来的 时候已经移动过了吧
        int savedWriteIndex = buffer.writerIndex();
        //跳过 头部的长度
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        //创建输出流对象
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
        //将channel.url序列化 这里先不看
        ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
        //根据是否是 事件 进行二次编码
        //将req.data 编码
        if (req.isEvent()) {
            //就是直接将 data 写入out  channel 是无效参数
            encodeEventData(channel, out, req.getData());
        } else {
            //就是直接将 data 写入out  version 是无效参数
            encodeRequestData(channel, out, req.getData(), req.getVersion());
        }
        //刷盘 并 执行清理工作
        out.flushBuffer();
        if (out instanceof Cleanable) {
            ((Cleanable) out).cleanup();
        }
        //将bos 的数据再次刷盘
        bos.flush();
        bos.close();
        //负荷检测
        int len = bos.writtenBytes();
        checkPayload(channel, len);
        //从12 字节 开始 写到15字节 代表长度
        Bytes.int2bytes(len, header, 12);

        // write 回到 0指针
        buffer.writerIndex(savedWriteIndex);
        //写入头部 对象
        buffer.writeBytes(header); // write header.
        //将指针移动到 写入的数据末尾  也就是这个len 不包含头部的长度
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 针对响应结果进行编码
     * @param channel
     * @param buffer
     * @param res
     * @throws IOException
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        //记录当前指针
        int savedWriteIndex = buffer.writerIndex();
        try {
            //通过 channel 的url 获取序列化对象
            Serialization serialization = getSerialization(channel);
            // header.
            // 创建头部对象
            byte[] header = new byte[HEADER_LENGTH];
            // set magic number.
            // [0-1] 保存的是魔法数的 高低位
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            // [2] 保存的是 响应标识 序列化类型 是否是 event类型  心跳检测 就属于事件类型
            header[2] = serialization.getContentTypeId();
            //mEvent == true && mResult == null
            if (res.isHeartbeat()) {
                header[2] |= FLAG_EVENT;
            }
            // set response status.
            // [3] 设置状态
            byte status = res.getStatus();
            header[3] = status;
            // set request id.
            // [4-11] 设置请求标识
            Bytes.long2bytes(res.getId(), header, 4);

            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
            // encode response data or error message.
            // 请求成功时
            if (status == Response.OK) {
                if (res.isHeartbeat()) {
                    //就是 out.write(result)
                    encodeHeartbeatData(channel, out, res.getResult());
                } else {
                    //同上
                    encodeResponseData(channel, out, res.getResult(), res.getVersion());
                }
            } else {
                //失败时 通过UTF 写入数据
                out.writeUTF(res.getErrorMessage());
            }
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
            bos.flush();
            bos.close();

            //设置数据到buffer中
            int len = bos.writtenBytes();
            checkPayload(channel, len);
            Bytes.int2bytes(len, header, 12);
            // write
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // write header.
            //结尾时 记得更新指针
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            //还原指针
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                //重新发起一个 响应结果
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        //通过channel 应该会在哪个地方 再次进入这里的 编码器 进行编码然后发送到 客户端
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // Rethrow exception
            // 继续抛出该异常 保证能被上层捕获
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    @Deprecated
    protected Object decodeHeartbeatData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 针对 事件类型的 请求就是直接将data 写入输出流
     * @param out
     * @param data
     * @throws IOException
     */
    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 为心跳检测 编码 其实就是为 事件编码
     * @param out
     * @param data
     * @throws IOException
     */
    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    /**
     * 对请求对象编码
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 为结果编码
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Deprecated
    protected Object decodeHeartbeatData(Channel channel, ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    /**
     * 针对事件类型的 请求 进行编码
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    /**
     * 为 心跳数据 编码
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    /**
     * 对请求对象编码
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 为结果编码
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeResponseData(out, data);
    }


}
