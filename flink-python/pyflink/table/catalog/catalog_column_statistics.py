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

class CatalogColumnStatistics(object):
    """
    Column statistics of a table or partition.
    """

    def __init__(self, column_statistics_data=None, properties=None,
                 j_catalog_column_statistics=None):
        if j_catalog_column_statistics is None:
            gateway = get_gateway()
            java_import(gateway.jvm, "org.apache.flink.table.catalog.stats.CatalogColumnStatistics")
            if properties is None:
                self._j_catalog_column_statistics = gateway.jvm.CatalogColumnStatistics(
                    column_statistics_data)
            else:
                self._j_catalog_column_statistics = gateway.jvm.CatalogColumnStatistics(
                    column_statistics_data, properties)
        else:
            self._j_catalog_column_statistics = j_catalog_column_statistics

    def get_column_statistics_data(self):
        return self._j_catalog_column_statistics.getColumnStatisticsData()

    def get_properties(self) -> Dict[str, str]:
        return dict(self._j_catalog_column_statistics.getProperties())

    def copy(self) -> 'CatalogColumnStatistics':
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog_column_statistics.copy())
