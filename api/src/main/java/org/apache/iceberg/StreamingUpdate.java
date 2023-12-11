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

import org.apache.iceberg.exceptions.ValidationException;

/**
 * API for appending sequential updates to a table
 *
 * <p>This API accumulates batches of file additions and deletions by order, produces a new {@link
 * Snapshot} of the changes where each batch is added to a new data sequence number, and commits
 * that snapshot as the current.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 * If any of the deleted files are no longer in the latest snapshot when reattempting, the commit
 * will throw a {@link ValidationException}.
 */
public interface StreamingUpdate extends SnapshotUpdate<StreamingUpdate> {
  /**
   * Remove a data file from the current table state.
   *
   * <p>This rewrite operation may change the size or layout of the data files. When applicable, it
   * is also recommended to discard already deleted records while rewriting data files. However, the
   * set of live data records must never change.
   *
   * @param dataFile a rewritten data file
   * @return this for method chaining
   */
  default StreamingUpdate deleteFile(DataFile dataFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  /**
   * Remove a delete file from the table state.
   *
   * <p>This rewrite operation may change the size or layout of the delete files. When applicable,
   * it is also recommended to discard delete records for files that are no longer part of the table
   * state. However, the set of applicable delete records must never change.
   *
   * @param deleteFile a rewritten delete file
   * @return this for method chaining
   */
  default StreamingUpdate deleteFile(DeleteFile deleteFile) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement deleteFile");
  }

  /**
   * Add a new data file to a specific. All files in this batch will receive the same data sequence
   * number.
   *
   * <p>This rewrite operation may change the size or layout of the data files. When applicable, it
   * is also recommended to discard already deleted records while rewriting data files. However, the
   * set of live data records must never change.
   *
   * @param dataFile a new data file
   * @param batchOrdinal The batch ordinal to associate with this data file
   * @return this for method chaining
   */
  default StreamingUpdate addFile(DataFile dataFile, int batchOrdinal) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }

  /**
   * Add a new delete file to a specific batch. All files in this batch will receive the same data
   * sequence number.
   *
   * <p>This rewrite operation may change the size or layout of the delete files. When applicable,
   * it is also recommended to discard delete records for files that are no longer part of the table
   * state. However, the set of applicable delete records must never change.
   *
   * @param deleteFile a new delete file
   * @param batchOrdinal The batch ordinal to associate with this data file
   * @return this for method chaining
   */
  default StreamingUpdate addFile(DeleteFile deleteFile, int batchOrdinal) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement addFile");
  }
}
