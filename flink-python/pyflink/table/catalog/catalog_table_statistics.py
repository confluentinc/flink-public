################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from py4j.java_gateway import java_import
from pyflink.java_gateway import get_gateway
from typing import Dict


class CatalogTableStatistics(object):
    """
    Statistics for a non-partitioned table or a partition of a partitioned table.
    """

    def __init__(self, row_count=None, field_count=None, total_size=None, raw_data_size=None,
                 properties=None, j_catalog_table_statistics=None):
        gateway = get_gateway()
        java_import(gateway.jvm, "org.apache.flink.table.catalog.stats.CatalogTableStatistics")
        if j_catalog_table_statistics is None:
            if properties is None:
                self._j_catalog_table_statistics = gateway.jvm.CatalogTableStatistics(
                    row_count, field_count, total_size, raw_data_size)
            else:
                self._j_catalog_table_statistics = gateway.jvm.CatalogTableStatistics(
                    row_count, field_count, total_size, raw_data_size, properties)
        else:
            self._j_catalog_table_statistics = j_catalog_table_statistics

    def get_row_count(self) -> int:
        """
        The number of rows in the table or partition.
        """
        return self._j_catalog_table_statistics.getRowCount()

    def get_field_count(self) -> int:
        """
        The number of files on disk.
        """
        return self._j_catalog_table_statistics.getFileCount()

    def get_total_size(self) -> int:
        """
        The total size in bytes.
        """
        return self._j_catalog_table_statistics.getTotalSize()

    def get_raw_data_size(self) -> int:
        """
        The raw data size (size when loaded in memory) in bytes.
        """
        return self._j_catalog_table_statistics.getRawDataSize()

    def get_properties(self) -> Dict[str, str]:
        return dict(self._j_catalog_table_statistics.getProperties())

    def copy(self) -> 'CatalogTableStatistics':
        """
        Create a deep copy of "this" instance.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog_table_statistics.copy())
