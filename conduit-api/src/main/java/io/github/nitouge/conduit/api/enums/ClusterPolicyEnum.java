package io.github.nitouge.conduit.api.enums;

public enum ClusterPolicyEnum {

    FAIL_OVER("failover"),

    FAIL_FAST("failfast"),

    FAIL_SAFE("failsafe"),

    FAIL_BACK("failback");

    private final String type;

    ClusterPolicyEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
