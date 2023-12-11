/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseStreamingUpdate extends MergingSnapshotProducer<StreamingUpdate>
    implements StreamingUpdate {
  private final List<Batch> batches = Lists.newArrayList();

  BaseStreamingUpdate(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected BaseStreamingUpdate self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    long startingSequenceNumber = base.nextSequenceNumber();
    batches.sort(Comparator.comparingInt(o -> o.ordinal));
    for (Batch batch : batches) {
      long dataSequenceNumber = startingSequenceNumber + batch.ordinal + 1;
      batch.newDataFiles.forEach(f -> add(f, dataSequenceNumber));
      batch.newDeleteFiles.forEach(f -> add(f, dataSequenceNumber));
    }
    return super.apply(base, snapshot);
  }

  @Override
  public StreamingUpdate addFile(DataFile dataFile, int batchOrdinal) {
    return StreamingUpdate.super.addFile(dataFile, batchOrdinal);
  }

  @Override
  public StreamingUpdate addFile(DeleteFile deleteFile, int batchOrdinal) {
    return StreamingUpdate.super.addFile(deleteFile, batchOrdinal);
  }

  @Override
  public BaseStreamingUpdate toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  private static class Batch {
    private final List<DataFile> newDataFiles = Lists.newArrayList();
    private final List<DeleteFile> newDeleteFiles = Lists.newArrayList();
    private final int ordinal;

    /**
     * Creates a new set of updates to a specific batch
     *
     * @param ordinal the batch ordinal
     */
    Batch(int ordinal) {
      this.ordinal = ordinal;
    }

    public List<DataFile> getNewDataFiles() {
      return newDataFiles;
    }

    public List<DeleteFile> getNewDeleteFiles() {
      return newDeleteFiles;
    }

    public int getOrdinal() {
      return ordinal;
    }
  }
}
