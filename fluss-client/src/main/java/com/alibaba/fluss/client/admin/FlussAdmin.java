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

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.lake.LakeTableSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.PartitionSnapshotInfo;
import com.alibaba.fluss.client.utils.ClientRpcMessageUtils;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
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
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetPartitionSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetTableRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeListOffsetsRequest;
import static com.alibaba.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;

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
    public CompletableFuture<Cluster> getCluster(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitions,
            @Nullable Collection<Long> tablePartitionIds) {
        CompletableFuture<Cluster> future = new CompletableFuture<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        Cluster cluster =
                                sendMetadataRequestAndRebuildCluster(
                                        gateway,
                                        false,
                                        metadataUpdater.getCluster(),
                                        tablePaths,
                                        tablePartitions,
                                        tablePartitionIds);
                        future.complete(cluster);
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
    public CompletableFuture<Void> createDatabase(String databaseName, boolean ignoreIfExists) {
        TablePath.validateDatabaseName(databaseName);
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setDatabaseName(databaseName).setIgnoreIfExists(ignoreIfExists);
        return gateway.createDatabase(request).thenApply(r -> null);
    }

    @Override
    public CompletableFuture<Void> deleteDatabase(
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
    public CompletableFuture<TableInfo> getTable(TablePath tablePath) {
        GetTableRequest request = new GetTableRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getTable(request)
                .thenApply(
                        r ->
                                new TableInfo(
                                        tablePath,
                                        r.getTableId(),
                                        TableDescriptor.fromJsonBytes(r.getTableJson()),
                                        r.getSchemaId()));
    }

    @Override
    public CompletableFuture<Void> deleteTable(TablePath tablePath, boolean ignoreIfNotExists) {
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
    public CompletableFuture<KvSnapshotInfo> getKvSnapshot(TablePath tablePath) {
        GetKvSnapshotRequest request = new GetKvSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        return gateway.getKvSnapshot(request).thenApply(ClientRpcMessageUtils::toKvSnapshotInfo);
    }

    @Override
    public CompletableFuture<LakeTableSnapshotInfo> getLakeTableSnapshot(TablePath tablePath) {
        GetLakeTableSnapshotRequest request = new GetLakeTableSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        return gateway.getLakeTableSnapshot(request)
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
        Map<Integer, ListOffsetsRequest> requestMap =
                prepareListOffsetsRequests(
                        metadataUpdater, tableId, partitionId, buckets, offsetSpec);
        Map<Integer, CompletableFuture<Long>> bucketToOffsetMap = new ConcurrentHashMap<>();
        for (int bucket : buckets) {
            bucketToOffsetMap.put(bucket, new CompletableFuture<>());
        }

        sendListOffsetsRequest(metadataUpdater, client, requestMap, bucketToOffsetMap);
        return new ListOffsetsResult(bucketToOffsetMap);
    }

    @Override
    public CompletableFuture<PartitionSnapshotInfo> getPartitionSnapshot(
            TablePath tablePath, String partitionName) {
        GetPartitionSnapshotRequest request = new GetPartitionSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        request.setPartitionName(partitionName);
        return gateway.getPartitionSnapshot(request)
                .thenApply(ClientRpcMessageUtils::toPartitionSnapshotInfo);
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

    private static Map<Integer, ListOffsetsRequest> prepareListOffsetsRequests(
            MetadataUpdater metadataUpdater,
            long tableId,
            @Nullable Long partitionId,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {
        Map<Integer, List<Integer>> nodeForBucketList = new HashMap<>();
        for (Integer bucketId : buckets) {
            int leader = metadataUpdater.leaderFor(new TableBucket(tableId, partitionId, bucketId));
            nodeForBucketList.computeIfAbsent(leader, k -> new ArrayList<>()).add(bucketId);
        }

        Map<Integer, ListOffsetsRequest> listOffsetsRequests = new HashMap<>();
        nodeForBucketList.forEach(
                (leader, ids) ->
                        listOffsetsRequests.put(
                                leader,
                                makeListOffsetsRequest(tableId, partitionId, ids, offsetSpec)));
        return listOffsetsRequests;
    }

    private static void sendListOffsetsRequest(
            MetadataUpdater metadataUpdater,
            RpcClient client,
            Map<Integer, ListOffsetsRequest> leaderToRequestMap,
            Map<Integer, CompletableFuture<Long>> bucketToOffsetMap) {
        leaderToRequestMap.forEach(
                (leader, request) -> {
                    TabletServerGateway gateway =
                            GatewayClientProxy.createGatewayProxy(
                                    () -> metadataUpdater.getTabletServer(leader),
                                    client,
                                    TabletServerGateway.class);
                    gateway.listOffsets(request)
                            .thenAccept(
                                    r -> {
                                        for (PbListOffsetsRespForBucket resp :
                                                r.getBucketsRespsList()) {
                                            if (resp.hasErrorCode()) {
                                                bucketToOffsetMap
                                                        .get(resp.getBucketId())
                                                        .completeExceptionally(
                                                                ApiError.fromErrorMessage(resp)
                                                                        .exception());
                                            } else {
                                                bucketToOffsetMap
                                                        .get(resp.getBucketId())
                                                        .complete(resp.getOffset());
                                            }
                                        }
                                    });
                });
    }
}
