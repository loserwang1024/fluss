package com.alibaba.fluss.authorize;

import com.alibaba.fluss.authenticate.FlussPrincipal;

public class Acl {
    private final FlussPrincipal principal;
    private final PermissionType permissionType;
    private final String host;
    private final OperationType operationType;

    public Acl(FlussPrincipal principal, PermissionType permissionType, String host, OperationType operationType) {
        this.principal = principal;
        this.permissionType = permissionType;
        this.host = host;
        this.operationType = operationType;
    }
}
