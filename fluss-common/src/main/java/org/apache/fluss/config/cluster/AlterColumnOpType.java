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

package org.apache.fluss.config.cluster;

/** The type of alter column operation. */
public enum AlterColumnOpType {
    AddColumn(0),
    DropColumn(1),
    ModifyColumn(2);

    public final int value;

    AlterColumnOpType(int value) {
        this.value = value;
    }

    public static AlterColumnOpType from(int opType) {
        switch (opType) {
            case 0:
                return AddColumn;
            case 1:
                return DropColumn;
            case 2:
                return ModifyColumn;
            default:
                throw new IllegalArgumentException("Unsupported AlterColumnOpType: " + opType);
        }
    }

    public int value() {
        return this.value;
    }
}
