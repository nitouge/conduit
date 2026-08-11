package io.github.nitouge.conduit.api.enums;

public enum LoadbalancePolicyEnum {

    RANDOM("random"),

    ROUND_ROBIN("roundrobin"),

    CONSISTENT_HASH("consistenthash");

    private final String type;

    LoadbalancePolicyEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
