package org.apache.dubbo.proxy.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.apache.dubbo.proxy.dao.ServiceDefinition;
import org.apache.dubbo.proxy.dao.ServiceMapping;
import org.apache.dubbo.proxy.metadata.MetadataCollector;
import org.apache.dubbo.proxy.utils.NamingThreadFactory;
import org.apache.dubbo.proxy.worker.RequestWorker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


@ChannelHandler.Sharable
public class HttpProcessHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private ExecutorService businessThreadPool;
    private MetadataCollector metadataCollector;
    private ServiceMapping serviceMapping;
    private Logger logger = LoggerFactory.getLogger(HttpProcessHandler.class);


    public HttpProcessHandler(int businessThreadCount, ServiceMapping serviceMapping, MetadataCollector metadataCollector) {
        super();
        this.businessThreadPool = Executors.newFixedThreadPool(businessThreadCount, new NamingThreadFactory("Dubbo-proxy-request-worker"));
        this.metadataCollector = metadataCollector;
        this.serviceMapping = serviceMapping;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
        ByteBuf raw = msg.content();
        String info = raw.toString(CharsetUtil.UTF_8);

        // TODO error handle
        try {
            ServiceDefinition serviceDefinition = JSON.parseObject(info, ServiceDefinition.class);
            doRequest(ctx, serviceDefinition, msg);
        } catch (JSONException e) {
            Map<String, String> result = new HashMap<>();
            result.put("msg", "invalid json");

            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, OK,
                    Unpooled.copiedBuffer(JSON.toJSONString(result), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            ctx.writeAndFlush(response);
            ctx.close();
        }
    }

    private void doRequest(ChannelHandlerContext ctx, ServiceDefinition serviceDefinition, HttpRequest msg) {
        businessThreadPool.execute(new RequestWorker(serviceDefinition, ctx, msg, metadataCollector, serviceMapping));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
