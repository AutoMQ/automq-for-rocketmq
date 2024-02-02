/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.proxy.remoting.activity;

import com.automq.rocketmq.proxy.exception.ExceptionHandler;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import io.netty.channel.ChannelHandlerContext;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.ConsumerManagerActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

public class ExtendConsumerManagerActivity extends ConsumerManagerActivity implements CommonRemotingBehavior {
    public ExtendConsumerManagerActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        Optional<RemotingCommand> response = checkClientVersion(request);
        if (response.isPresent()) {
            return response.get();
        }

        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_CONNECTION_LIST,
                 RequestCode.UNLOCK_BATCH_MQ,
                 RequestCode.LOCK_BATCH_MQ,
                 RequestCode.GET_CONSUMER_LIST_BY_GROUP -> {
                // These four requests are handled by the parent class.
                return super.processRequest0(ctx, request, context);
            }

            // The following requests must be handled by ourself.
            case RequestCode.UPDATE_CONSUMER_OFFSET -> {
                return updateConsumerOffset(ctx, request, context);
            }
            case RequestCode.QUERY_CONSUMER_OFFSET -> {
                return queryConsumerOffset(ctx, request, context);
            }
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP -> {
                return searchOffsetByTimestamp(ctx, request, context);
            }
            case RequestCode.GET_MIN_OFFSET -> {
                return getMinOffset(ctx, request, context);
            }
            case RequestCode.GET_MAX_OFFSET -> {
                return getMaxOffset(ctx, request, context);
            }
            case RequestCode.GET_EARLIEST_MSG_STORETIME -> {
                // The upstream has already deprecated this request.
                // We just return a codeNotSupportedResponse.
                return RemotingUtil.codeNotSupportedResponse(request);
            }
        }

        return RemotingUtil.codeNotSupportedResponse(request);
    }

    @Override
    protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context,
        long timeoutMillis) throws Exception {
        // The parent class use this method to proxy the request to the broker.
        // We disable this behavior here.
        return null;
    }

    @Override
    protected RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        LockBatchRequestBody lockQueueRequest = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        CompletableFuture<Set<MessageQueue>> lockCf = messagingProcessor.lockBatchMQ(
            context,
            lockQueueRequest.getMqSet(),
            lockQueueRequest.getConsumerGroup(),
            context.getClientID(),
            context.getRemainingMs());

        lockCf.whenComplete((lockedQueueSet, ex) -> {
            if (ex != null) {
                writeErrResponse(ctx, context, request, ex);
                return;
            }

            final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS);
            LockBatchResponseBody responseBody = new LockBatchResponseBody();
            responseBody.setLockOKMQSet(lockedQueueSet);
            response.setBody(responseBody.encode());
            writeResponse(ctx, context, request, response);
        });

        return null;
    }

    @Override
    protected RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        UnlockBatchRequestBody unlockQueueRequest = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS);

        CompletableFuture<Void> unlockCf = messagingProcessor.unlockBatchMQ(
            context,
            unlockQueueRequest.getMqSet(),
            unlockQueueRequest.getConsumerGroup(),
            context.getClientID(),
            context.getRemainingMs());

        unlockCf.whenComplete((v, ex) -> {
            if (ex != null) {
                writeErrResponse(ctx, context, request, ex);
                return;
            }
            writeResponse(ctx, context, request, response);
        });

        return null;
    }

    @Override
    protected ProxyContext createContext(ChannelHandlerContext ctx, RemotingCommand request) {
        return createExtendContext(super.createContext(ctx, request));
    }

    @Override
    protected void writeResponse(ChannelHandlerContext ctx, ProxyContext context, RemotingCommand request,
        RemotingCommand response, Throwable t) {
        recordRpcLatency(context, response);
        super.writeResponse(ctx, context, request, response, t);
    }

    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        // TODO: We should implement these requests to support the Admin tool
        return RemotingUtil.codeNotSupportedResponse(request);
    }

    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context)
        throws RemotingCommandException {
        // Retrieve the request header.
        final GetMaxOffsetRequestHeader requestHeader = (GetMaxOffsetRequestHeader)
            request.decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        // Build the response.
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS,
            GetMaxOffsetResponseHeader.class);

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        messagingProcessor.getMaxOffset(context, messageQueue, context.getRemainingMs()).whenComplete((offset, ex) -> {
            if (ex != null) {
                writeErrResponse(ctx, context, request, ex);
                return;
            }
            GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            writeResponse(ctx, context, request, response);
        });

        return null;
    }

    private RemotingCommand getMinOffset(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context)
        throws RemotingCommandException {
        // Retrieve the request header.
        final GetMinOffsetRequestHeader requestHeader = (GetMinOffsetRequestHeader)
            request.decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);

        // Build the response.
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS,
            GetMinOffsetResponseHeader.class);

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        messagingProcessor.getMinOffset(context, messageQueue, context.getRemainingMs()).whenComplete((offset, ex) -> {
            if (ex != null) {
                writeErrResponse(ctx, context, request, ex);
                return;
            }
            GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            writeResponse(ctx, context, request, response);
        });

        return null;
    }

    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws RemotingCommandException {
        // Retrieve the request header.
        final QueryConsumerOffsetRequestHeader requestHeader = (QueryConsumerOffsetRequestHeader)
            request.decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        // Build the response.
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS,
            QueryConsumerOffsetResponseHeader.class);

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        messagingProcessor.queryConsumerOffset(context, messageQueue, requestHeader.getConsumerGroup(),
            context.getRemainingMs()).whenComplete((offset, ex) -> {
                if (ex != null) {
                    writeErrResponse(ctx, context, request, ex);
                    return;
                }

                QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader)
                    response.readCustomHeader();

                if (offset < 0) {
                    // The client will decide the offset to use, min or max offset of the queue.
                    response.setCode(ResponseCode.QUERY_NOT_FOUND);
                } else {
                    responseHeader.setOffset(offset);
                }

                writeResponse(ctx, context, request, response);
            });
        return null;
    }

    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws RemotingCommandException {
        UpdateConsumerOffsetRequestHeader requestHeader = (UpdateConsumerOffsetRequestHeader)
            request.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS);

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        CompletableFuture<Void> updateOffsetCf = messagingProcessor.updateConsumerOffset(
            context,
            messageQueue,
            requestHeader.getConsumerGroup(),
            requestHeader.getCommitOffset(),
            context.getRemainingMs());

        updateOffsetCf.whenComplete((v, ex) -> {
            if (ex != null) {
                writeErrResponse(ctx, context, request, ex);
                return;
            }
            writeResponse(ctx, context, request, response);
        });
        return null;
    }

    @Override
    protected void writeErrResponse(ChannelHandlerContext ctx, ProxyContext context, RemotingCommand request,
        Throwable t) {
        Optional<RemotingCommand> response = ExceptionHandler.convertToRemotingResponse(t);
        if (response.isPresent()) {
            writeResponse(ctx, context, request, response.get(), t);
            return;
        }

        super.writeErrResponse(ctx, context, request, t);
    }
}
