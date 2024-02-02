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

package com.automq.rocketmq.metadata.dao;

import java.util.Date;

public class Node {
    /**
     * Node ID
     */
    private int id;

    /**
     * Node name, required.
     */
    private String name;

    /**
     * Optional EC2 instance-id
     */
    private String instanceId;

    /**
     * Optional EBS volume-id
     */
    private String volumeId;

    /**
     * Optional host-name
     */
    private String hostName;

    /**
     * Optional vpc-id
     */
    private String vpcId;

    /**
     * Address of the node, through which to accept TCP connections.
     */
    private String address;

    /**
     * Epoch of the current broker process. Each registration increase it by one when it starts.
     */
    private long epoch;

    private Date createTime = new Date();

    private Date updateTime = new Date();

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getVolumeId() {
        return volumeId;
    }

    public void setVolumeId(String volumeId) {
        this.volumeId = volumeId;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getVpcId() {
        return vpcId;
    }

    public void setVpcId(String vpcId) {
        this.vpcId = vpcId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "Node {" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", address='" + address + '\'' +
            ", epoch=" + epoch +
            ", updateTime=" + updateTime +
            '}';
    }
}
