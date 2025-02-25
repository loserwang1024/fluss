/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.client.admin.internal.AdminApiDriver;
import com.alibaba.fluss.client.admin.internal.AdminApiFuture;
import com.alibaba.fluss.client.admin.internal.AdminApiHandler;
import com.alibaba.fluss.client.admin.internal.ListOffsetsHandler;
import com.alibaba.fluss.client.metadata.KvSnapshotMetadata;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.utils.ClientRpcMessageUtils;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * The default implementation of {@link Admin}.
 *
 * <p>This class is thread-safe. The API of this class is evolving, see {@link Admin} for details.
 */
public class FlussAdmin implements Admin {

    private final AdminGateway gateway;
    private final MetadataUpdater metadataUpdater;
    private final RpcClient client;

    public FlussAdmin(RpcClient client, MetadataUpdater metadataUpdater) {
        this.gateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, client, AdminGateway.class);
        this.metadataUpdater = metadataUpdater;
        this.client = client;
    }

    @Override
    public CompletableFuture<List<ServerNode>> getServerNodes() {
        CompletableFuture<List<ServerNode>> future = new CompletableFuture<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        List<ServerNode> serverNodeList = new ArrayList<>();
                        Cluster cluster =
                                sendMetadataRequestAndRebuildCluster(
                                        gateway,
                                        false,
                                        metadataUpdater.getCluster(),
                                        null,
                                        null,
                                        null);
                        serverNodeList.add(cluster.getCoordinatorServer());
                        serverNodeList.addAll(cluster.getAliveTabletServerList());
                        future.complete(serverNodeList);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath) {
        GetTableSchemaRequest request = new GetTableSchemaRequest();
        // requesting the latest schema of the given table by not setting schema id
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getTableSchema(request)
                .thenApply(
                        r ->
                                new SchemaInfo(
                                        Schema.fromJsonBytes(r.getSchemaJson()), r.getSchemaId()));
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath, int schemaId) {
        GetTableSchemaRequest request = new GetTableSchemaRequest();
        // requesting the latest schema of the given table by not setting schema id
        request.setSchemaId(schemaId)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getTableSchema(request)
                .thenApply(
                        r ->
                                new SchemaInfo(
                                        Schema.fromJsonBytes(r.getSchemaJson()), r.getSchemaId()));
    }

    @Override
    public CompletableFuture<Void> createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists) {
        TablePath.validateDatabaseName(databaseName);
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setDatabaseJson(databaseDescriptor.toJsonBytes())
                .setDatabaseName(databaseName)
                .setIgnoreIfExists(ignoreIfExists);
        return gateway.createDatabase(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<DatabaseInfo> getDatabaseInfo(String databaseName) {
        GetDatabaseInfoRequest request = new GetDatabaseInfoRequest();
        request.setDatabaseName(databaseName);
        return gateway.getDatabaseInfo(request)
                .thenApply(
                        r ->
                                new DatabaseInfo(
                                        databaseName,
                                        DatabaseDescriptor.fromJsonBytes(r.getDatabaseJson()),
                                        r.getCreatedTime(),
                                        r.getModifiedTime()));
    }

    @Override
    public CompletableFuture<Void> dropDatabase(
            String databaseName, boolean ignoreIfNotExists, boolean cascade) {
        DropDatabaseRequest request = new DropDatabaseRequest();

        request.setIgnoreIfNotExists(ignoreIfNotExists)
                .setCascade(cascade)
                .setDatabaseName(databaseName);
        return gateway.dropDatabase(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Boolean> databaseExists(String databaseName) {
        DatabaseExistsRequest request = new DatabaseExistsRequest();
        request.setDatabaseName(databaseName);
        return gateway.databaseExists(request).thenApply(DatabaseExistsResponse::isExists);
    }

    @Override
    public CompletableFuture<List<String>> listDatabases() {
        ListDatabasesRequest request = new ListDatabasesRequest();
        return gateway.listDatabases(request)
                .thenApply(ListDatabasesResponse::getDatabaseNamesList);
    }

    @Override
    public CompletableFuture<Void> createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists) {
        tablePath.validate();
        CreateTableRequest request = new CreateTableRequest();
        request.setTableJson(tableDescriptor.toJsonBytes())
                .setIgnoreIfExists(ignoreIfExists)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.createTable(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getTableInfo(request)
                .thenApply(
                        r ->
                                TableInfo.of(
                                        tablePath,
                                        r.getTableId(),
                                        r.getSchemaId(),
                                        TableDescriptor.fromJsonBytes(r.getTableJson()),
                                        r.getCreatedTime(),
                                        r.getModifiedTime()));
    }

    @Override
    public CompletableFuture<Void> dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
        DropTableRequest request = new DropTableRequest();
        request.setIgnoreIfNotExists(ignoreIfNotExists)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.dropTable(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Boolean> tableExists(TablePath tablePath) {
        TableExistsRequest request = new TableExistsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.tableExists(request).thenApply(TableExistsResponse::isExists);
    }

    @Override
    public CompletableFuture<List<String>> listTables(String databaseName) {
        ListTablesRequest request = new ListTablesRequest();
        request.setDatabaseName(databaseName);
        return gateway.listTables(request).thenApply(ListTablesResponse::getTableNamesList);
    }

    @Override
    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath) {
        ListPartitionInfosRequest request = new ListPartitionInfosRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.listPartitionInfos(request)
                .thenApply(ClientRpcMessageUtils::toPartitionInfos);
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath) {
        GetLatestKvSnapshotsRequest request = new GetLatestKvSnapshotsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getLatestKvSnapshots(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshots);
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(
            TablePath tablePath, String partitionName) {
        checkNotNull(partitionName, "partitionName");
        GetLatestKvSnapshotsRequest request = new GetLatestKvSnapshotsRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        request.setPartitionName(partitionName);
        return gateway.getLatestKvSnapshots(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshots);
    }

    @Override
    public CompletableFuture<KvSnapshotMetadata> getKvSnapshotMetadata(
            TableBucket bucket, long snapshotId) {
        GetKvSnapshotMetadataRequest request = new GetKvSnapshotMetadataRequest();
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }
        request.setTableId(bucket.getTableId())
                .setBucketId(bucket.getBucket())
                .setSnapshotId(snapshotId);
        return gateway.getKvSnapshotMetadata(request)
                .thenApply(ClientRpcMessageUtils::toKvSnapshotMetadata);
    }

    @Override
    public CompletableFuture<LakeSnapshot> getLatestLakeSnapshot(TablePath tablePath) {
        GetLatestLakeSnapshotRequest request = new GetLatestLakeSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        return gateway.getLatestLakeSnapshot(request)
                .thenApply(ClientRpcMessageUtils::toLakeTableSnapshotInfo);
    }

    @Override
    public ListOffsetsResult listOffsets(
            PhysicalTablePath physicalTablePath,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {

        Long partitionId = null;
        metadataUpdater.checkAndUpdateTableMetadata(
                Collections.singleton(physicalTablePath.getTablePath()));
        long tableId = metadataUpdater.getTableId(physicalTablePath.getTablePath());
        // if partition name is not null, we need to check and update partition metadata
        if (physicalTablePath.getPartitionName() != null) {
            metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
            partitionId = metadataUpdater.getPartitionIdOrElseThrow(physicalTablePath);
        }

        AdminApiFuture.SimpleAdminApiFuture<Integer, Long> future =
                ListOffsetsHandler.newFuture(buckets);
        AdminApiHandler<Integer, Long> handler =
                new ListOffsetsHandler(metadataUpdater, tableId, partitionId, offsetSpec, client);
        AdminApiDriver<Integer, Long> adminApiDriver = new AdminApiDriver<>(future, handler);
        CompletableFuture.runAsync(adminApiDriver::maybeSendRequests);
        return new ListOffsetsResult(future.all());
    }

    @Override
    public CompletableFuture<LakeStorageInfo> describeLakeStorage() {
        return gateway.describeLakeStorage(new DescribeLakeStorageRequest())
                .thenApply(ClientRpcMessageUtils::toLakeStorageInfo);
    }

    @Override
    public void close() {
        // nothing to do yet
    }
}
