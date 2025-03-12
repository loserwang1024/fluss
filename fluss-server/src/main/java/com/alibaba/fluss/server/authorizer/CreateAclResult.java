/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.exception.ApiException;
import com.alibaba.fluss.security.acl.AclBinding;

import java.util.Optional;

public class CreateAclResult {
    private final AclBinding aclBinding;

    private final ApiException exception;

    public CreateAclResult(AclBinding aclBinding, ApiException exception) {
        this.aclBinding = aclBinding;
        this.exception = exception;
    }

    public static CreateAclResult success(AclBinding aclBinding) {
        return new CreateAclResult(aclBinding, null);
    }

    /** Returns any exception during create. If exception is empty, the request has succeeded. */
    public Optional<ApiException> exception() {
        return exception == null ? Optional.empty() : Optional.of(exception);
    }

    public AclBinding getAclBinding() {
        return aclBinding;
    }
}
