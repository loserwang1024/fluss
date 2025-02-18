package com.alibaba.fluss.authorize;

public class Resource {
    private final ResourceType type;
    private final String resourceName;

    public Resource(ResourceType type, String resourceName) {
        this.type = type;
        this.resourceName = resourceName;
    }
}
