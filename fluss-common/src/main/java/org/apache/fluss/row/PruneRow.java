/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.SchemaUtil.getIndexMapping;

/**
 * A specific implementation of {@link ProjectedRow} which prunes the columns from the origin schema
 * to the target schema.
 */
public class PruneRow extends ProjectedRow {

    private final RowType originRowType;

    private PruneRow(RowType originRowType, int[] indexMapping) {
        super(indexMapping);
        this.originRowType = originRowType;
    }

    @Override
    public boolean isNullAt(int pos) {
        return indexMapping[pos] == UNEXIST_MAPPING || super.isNullAt(pos);
    }

    @Override
    public long getLong(int pos) {
        DataType originType = originRowType.getTypeAt(indexMapping[pos]);
        switch (originType.getTypeRoot()) {
            case TINYINT:
                return super.getByte(pos);
            case SMALLINT:
                return super.getShort(pos);
            case INTEGER:
                return super.getInt(pos);
            case BIGINT:
                return super.getLong(pos);
            default:
                throw new IllegalArgumentException("Unsupported type: " + originType);
        }
    }

    public static PruneRow from(Schema originSchema, Schema expectedSchema) {
        int[] indexMapping = getIndexMapping(originSchema, expectedSchema);
        return new PruneRow(originSchema.getRowType(), indexMapping);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PruneRow)) {
            return false;
        }
        PruneRow pruneRow = (PruneRow) o;
        return Objects.equals(originRowType, pruneRow.originRowType)
                && Arrays.equals(indexMapping, pruneRow.indexMapping)
                && Objects.equals(row, pruneRow.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originRowType);
    }
}
