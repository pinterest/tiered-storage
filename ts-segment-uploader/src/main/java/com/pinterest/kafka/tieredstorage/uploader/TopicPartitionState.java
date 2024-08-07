package com.pinterest.kafka.tieredstorage.uploader;

/**
 * Wrapper class for the state of a topic partition
 */
public class TopicPartitionState {
    private int controller_epoch;
    private int leader;
    private int version;
    private int leader_epoch;
    private int[] isr;

    public int getController_epoch() {
        return controller_epoch;
    }

    public void setController_epoch(int controller_epoch) {
        this.controller_epoch = controller_epoch;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getLeader_epoch() {
        return leader_epoch;
    }

    public void setLeader_epoch(int leader_epoch) {
        this.leader_epoch = leader_epoch;
    }

    public int[] getIsr() {
        return isr;
    }

    public void setIsr(int[] isr) {
        this.isr = isr;
    }
}
