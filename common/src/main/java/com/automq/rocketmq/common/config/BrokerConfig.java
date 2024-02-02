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

package com.automq.rocketmq.common.config;

import com.automq.rocketmq.common.exception.RocketMQException;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.rocketmq.common.utils.NetworkUtil;

@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class BrokerConfig implements ControllerConfig {
    /**
     * Node ID
     */
    private int id;

    private long epoch;

    private String name;

    /**
     * If the broker is running on an EC2 instance, this is the instance-id.
     */
    private String instanceId;

    /**
     * Sample bind address are:
     * 0.0.0.0:0
     * 0.0.0.0:8080
     * 10.0.0.1:0
     * 10.0.0.1:8081
     */
    private String bindAddress;

    /**
     * Advertise address in HOST:PORT format.
     */
    private String advertiseAddress;

    /**
     * Access key and secret key for system internal access.
     */
    private String innerAccessKey;
    private String innerSecretKey;

    private final MetricsConfig metrics;
    private final TraceConfig trace;
    private final ProfilerConfig profiler;
    private final ProxyConfig proxy;
    private final StoreConfig store;
    private final S3StreamConfig s3Stream;

    private final DatabaseConfig db;

    private final Controller controller;

    private boolean terminating;

    public BrokerConfig() {
        this.metrics = new MetricsConfig();
        this.trace = new TraceConfig();
        this.profiler = new ProfilerConfig();
        this.proxy = new ProxyConfig();
        this.store = new StoreConfig();
        this.s3Stream = new S3StreamConfig();
        this.db = new DatabaseConfig();
        this.controller = new Controller();
    }

    private static String parseHost(String address) {
        int pos = address.lastIndexOf(':');
        return address.substring(0, pos);
    }

    private static int parsePort(String address) {
        int pos = address.lastIndexOf(':');
        return Integer.parseInt(address.substring(pos + 1));
    }

    private static final String ENV_CONFIG_PREFIX = "ROCKETMQ_NODE";

    /**
     * The following environment is supported
     * ROCKETMQ_NODE_NAME
     * ROCKETMQ_NODE_BIND_ADDRESS
     * ROCKETMQ_NODE_ADVERTISE_ADDRESS
     */
    public void initEnvVar() {
        Method[] methods = BrokerConfig.class.getMethods();
        for (Method method : methods) {
            if (method.getName().startsWith("set")) {
                if (method.getParameterCount() == 1 && method.getParameterTypes()[0] == String.class) {
                    String envName = envVarName(method.getName());
                    String value = System.getenv(envName);
                    if (!Strings.isNullOrEmpty(value)) {
                        System.out.printf("Accept environment variable %s --> %s%n", envName, value);
                        try {
                            method.invoke(this, value.trim());
                            System.out.printf("%s to %s%n", method.getName(), value);
                        } catch (IllegalAccessException | InvocationTargetException ignore) {
                        }
                    }
                }
            }
        }
    }

    private String envVarName(String setterName) {
        String upperSnakeCase = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_UNDERSCORE).convert(setterName);
        assert upperSnakeCase != null;
        return ENV_CONFIG_PREFIX + upperSnakeCase.substring(3);
    }

    public void validate() throws RocketMQException {
        if (null == advertiseAddress) {
            String host = NetworkUtil.getLocalAddress();
            this.advertiseAddress = host + ":" + parsePort(bindAddress);
        }

        int mainPort = parsePort(advertiseAddress);

        if (mainPort != parsePort(bindAddress)) {
            throw new RocketMQException(500, "Listen port does not match advertise address port");
        }

        proxy.setHostName(parseHost(advertiseAddress));

        // Use the main port as the Remoting port
        proxy.setRemotingListenPort(mainPort);

        // Use the main port + 1 as the gRPC port
        proxy.setGrpcListenPort(mainPort + 1);
    }

    @Override
    public int nodeId() {
        return this.id;
    }

    @Override
    public void setNodeId(int nodeId) {
        this.id = nodeId;
    }

    @Override
    public long epoch() {
        return this.epoch;
    }

    @Override
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String dbUrl() {
        return this.db.getUrl();
    }

    @Override
    public String dbUserName() {
        return this.db.getUserName();
    }

    @Override
    public String dbPassword() {
        return this.db.getPassword();
    }

    public MetricsConfig metrics() {
        return metrics;
    }

    public TraceConfig trace() {
        return trace;
    }

    public ProfilerConfig profiler() {
        return profiler;
    }

    public ProxyConfig proxy() {
        return proxy;
    }

    public StoreConfig store() {
        return store;
    }

    public S3StreamConfig s3Stream() {
        return s3Stream;
    }

    public DatabaseConfig db() {
        return db;
    }

    public String name() {
        return name;
    }

    public String instanceId() {
        return instanceId;
    }

    public String advertiseAddress() {
        return advertiseAddress;
    }

    @Override
    public boolean goingAway() {
        return terminating;
    }

    @Override
    public void flagGoingAway() {
        terminating = true;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public void setAdvertiseAddress(String advertiseAddress) {
        this.advertiseAddress = advertiseAddress;
    }

    public void setInnerAccessKey(String innerAccessKey) {
        this.innerAccessKey = innerAccessKey;
    }

    public void setInnerSecretKey(String innerSecretKey) {
        this.innerSecretKey = innerSecretKey;
    }

    public String getInnerAccessKey() {
        return innerAccessKey;
    }

    public String getInnerSecretKey() {
        return innerSecretKey;
    }

    @Override
    public long scanIntervalInSecs() {
        return controller.getScanIntervalInSecs();
    }

    @Override
    public int leaseLifeSpanInSecs() {
        return controller.getLeaseLifeSpanInSecs();
    }

    @Override
    public long nodeAliveIntervalInSecs() {
        return controller.getNodeAliveIntervalInSecs();
    }

    @Override
    public int deletedTopicLingersInSecs() {
        return controller.getDeletedTopicLingersInSecs();
    }

    @Override
    public int deletedGroupLingersInSecs() {
        return controller.getDeletedGroupLingersInSecs();
    }

    @Override
    public long balanceWorkloadIntervalInSecs() {
        return controller.getBalanceWorkloadIntervalInSecs();
    }

    @Override
    public long recycleS3IntervalInSecs() {
        return controller.getRecycleS3IntervalInSecs();
    }

    @Override
    public int workloadTolerance() {
        return controller.getWorkloadTolerance();
    }

    @Override
    public boolean dumpHeapOnError() {
        return controller.isDumpHeapOnError();
    }
}
