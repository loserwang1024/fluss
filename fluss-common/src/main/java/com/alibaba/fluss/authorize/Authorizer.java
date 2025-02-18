package com.alibaba.fluss.authorize;

import com.alibaba.fluss.authenticate.FlussPrincipal;
import com.alibaba.fluss.authenticate.Session;

import java.util.List;

public interface Authorizer {
    void start();
    void close();

    void authorize(Session session, Resource resource, OperationType operationType) throws Exception;

    void addAcls(Resource resource, List<Acl> acls) throws Exception;

    void removeAcls(ResourceType resource);

    List<Acl> getAcls(Resource resource);

    List<Acl> getAcls(FlussPrincipal principal);
}
