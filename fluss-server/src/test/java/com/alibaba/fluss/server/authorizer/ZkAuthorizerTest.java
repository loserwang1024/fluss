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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.ZooKeeperUtils;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ZkAuthorizer}. */
public class ZkAuthorizerTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZkAuthorizer authorizer;
    private ZkAuthorizer authorizer2;
    private ZooKeeperClient zooKeeperClient;
    private Configuration configuration;

    @BeforeEach
    void setUp() throws Exception {
        this.configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        zooKeeperClient = ZooKeeperUtils.startZookeeperClient(configuration, new NOPErrorHandler());
        authorizer = new ZkAuthorizer(configuration);
        authorizer2 = new ZkAuthorizer(configuration);
        authorizer.startup();
        authorizer2.startup();
    }

    @AfterEach
    void tearDown() {
        authorizer.close();
        authorizer2.close();
        zooKeeperClient.close();
    }

    @Test
    void testLoadCache() throws Exception {
        Resource resource1 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls1 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user1", "User"),
                                "host-1",
                                OperationType.READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource1, acls1);

        Resource resource2 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls2 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user2", "User"),
                                "host-1",
                                OperationType.READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource2, acls2);

        // delete acl change notifications to test initial load.
        zooKeeperClient.deleteAclChangeNotifications();
        authorizer2.startup();
        assertThat(listAcls(authorizer2, resource1)).isEqualTo(acls1);
        assertThat(listAcls(authorizer2, resource2)).isEqualTo(acls2);

        // test update cache later
        final Set<AccessControlEntry> acls3 =
                new HashSet<>(
                        Arrays.asList(
                                new AccessControlEntry(
                                        new FlussPrincipal("user2", "User"),
                                        "host-1",
                                        OperationType.READ,
                                        PermissionType.ANY),
                                new AccessControlEntry(
                                        new FlussPrincipal("user3", "User"),
                                        "host-2",
                                        OperationType.IDEMPOTENT_WRITE,
                                        PermissionType.ANY)));
        addAcls(authorizer, resource2, acls3);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, resource2)).isEqualTo(acls3);
                });
    }

    // Authorizing the empty resource is not supported because we create a znode with the resource
    // name.
    @Test
    void testEmptyAclThrowsException() {
        assertThatThrownBy(
                        () ->
                                addAcls(
                                        authorizer,
                                        Resource.database(""),
                                        Collections.singleton(
                                                new AccessControlEntry(
                                                        new FlussPrincipal("user1", "User"),
                                                        "host-1",
                                                        OperationType.READ,
                                                        PermissionType.ANY))))
                .hasMessageContaining("Failed to update ACLs for Resource{type=DATABASE, name=''}");
    }

    @Test
    void testLocalConcurrentModificationOfResourceAcls() {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 =
                new AccessControlEntry(user1, "host-1", OperationType.READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 =
                new AccessControlEntry(user2, "host-2", OperationType.READ, PermissionType.ANY);
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer, commonResource, Collections.singleton(acl2));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });
    }

    @Test
    void testDistributedConcurrentModificationOfResourceAcls() {
        Resource commonResource = Resource.database("test");
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 =
                new AccessControlEntry(user1, "host-1", OperationType.READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 =
                new AccessControlEntry(user2, "host-2", OperationType.READ, PermissionType.ANY);
        // Add on each instance
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer2, commonResource, Collections.singleton(acl2));

        FlussPrincipal user3 = new FlussPrincipal("user3", "User");
        AccessControlEntry acl3 =
                new AccessControlEntry(user3, "host-3", OperationType.READ, PermissionType.ANY);

        // Add on one instance and delete on another
        addAcls(authorizer, commonResource, Collections.singleton(acl3));
        removeAcls(authorizer2, commonResource, Collections.singleton(acl3));

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });
    }

    @Test
    void testHighConcurrencyModificationOfResourceAcls() throws Exception {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls =
                IntStream.range(0, 50)
                        .mapToObj(
                                i ->
                                        new AccessControlEntry(
                                                new FlussPrincipal(String.valueOf(i), "User"),
                                                "host-1",
                                                OperationType.READ,
                                                PermissionType.ANY))
                        .collect(Collectors.toSet());

        List<Runnable> concurrentFunctions =
                acls.stream()
                        .map(
                                acl ->
                                        (Runnable)
                                                () -> {
                                                    if (Integer.parseInt(
                                                                            acl.getPrincipal()
                                                                                    .getName())
                                                                    % 2
                                                            == 0) {
                                                        addAcls(
                                                                authorizer,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    } else {
                                                        addAcls(
                                                                authorizer2,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    }

                                                    if (Integer.parseInt(
                                                                            acl.getPrincipal()
                                                                                    .getName())
                                                                    % 10
                                                            == 0) {
                                                        removeAcls(
                                                                authorizer2,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    }
                                                })
                        .collect(Collectors.toList());

        Set<AccessControlEntry> expectedAcls =
                acls.stream()
                        .filter(acl -> Integer.parseInt(acl.getPrincipal().getName()) % 10 != 0)
                        .collect(Collectors.toSet());
        assertConcurrent(concurrentFunctions, 30 * 1000);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource)).isEqualTo(expectedAcls);
                });
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource)).isEqualTo(expectedAcls);
                });
    }

    // todo: 无法保证，好奇为啥kafka可以保证，不应该把
    @Test
    void testHighConcurrencyDeletionOfResourceAcls() {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        AccessControlEntry acl =
                new AccessControlEntry(
                        new FlussPrincipal("user1", "User"),
                        "host-1",
                        OperationType.READ,
                        PermissionType.ANY);
        List<Runnable> concurrentFunctions =
                IntStream.range(0, 50)
                        .mapToObj(
                                i ->
                                        (Runnable)
                                                () -> {
                                                    addAcls(
                                                            authorizer,
                                                            commonResource,
                                                            Collections.singleton(acl));
                                                    removeAcls(
                                                            authorizer2,
                                                            commonResource,
                                                            Collections.singleton(acl));
                                                })
                        .collect(Collectors.toList());
        assertConcurrent(concurrentFunctions, 30 * 1000);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(Collections.emptySet());
                });

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource))
                            .isEqualTo(Collections.emptySet());
                });
    }

    // todo : add acl作为base测试类

    void addAcls(Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBinding> aclBindings =
                entries.stream()
                        .map(entry -> new AclBinding(resource, entry))
                        .collect(Collectors.toList());
        authorizer
                .addAcls(aclBindings)
                .forEach(
                        result -> {
                            if (result.exception().isPresent()) {
                                throw result.exception().get();
                            }
                        });
    }

    Set<AccessControlEntry> listAcls(Authorizer authorizer, Resource resource) {
        AclBindingFilter aclBindingFilter =
                new AclBindingFilter(
                        new ResourceFilter(resource.getType(), resource.getName()),
                        AccessControlEntryFilter.ANY);
        Collection<AclBinding> aclBindings = authorizer.listAcls(aclBindingFilter);
        return aclBindings.stream()
                .map(AclBinding::getAccessControlEntry)
                .collect(Collectors.toSet());
    }

    void removeAcls(Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBindingFilter> aclBindings =
                entries.stream()
                        .map(
                                entry ->
                                        new AclBindingFilter(
                                                new ResourceFilter(
                                                        resource.getType(), resource.getName()),
                                                new AccessControlEntryFilter(
                                                        entry.getPrincipal(),
                                                        entry.getHost(),
                                                        entry.getOperationType(),
                                                        entry.getPermissionType())))
                        .collect(Collectors.toList());
        authorizer.dropAcls(aclBindings).stream()
                .forEach(
                        result -> {
                            if (result.exception().isPresent()) {
                                throw result.exception().get();
                            }
                        });
    }

    /**
     * Asserts that a list of tasks can be executed concurrently within a given timeout.
     *
     * @param tasks the list of tasks to execute
     * @param timeoutMs the timeout in milliseconds
     */
    private void assertConcurrent(List<Runnable> tasks, long timeoutMs) {
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

        executor.shutdown();
        try {
            boolean completed = executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            assertThat(completed).isTrue();
            for (Future<?> future : futures) {
                future.get(); // Ensure no exceptions were thrown
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Should support many concurrent calls"
                            + " - Exception during concurrent execution",
                    e);
        }
    }
}
