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

from pyflink.java_gateway import get_gateway
from typing import Dict

class CatalogPartitionSpec(object):
    """
    Represents a partition spec object in catalog.
    Partition columns and values are NOT of strict order, and they need to be re-arranged to the
    correct order by comparing with a list of strictly ordered partition keys.
    """

    def __init__(self, partition_spec):
        if isinstance(partition_spec, dict):
            gateway = get_gateway()
            self._j_catalog_partition_spec = gateway.jvm.CatalogPartitionSpec(partition_spec)
        else:
            self._j_catalog_partition_spec = partition_spec

    def __str__(self):
        return self._j_catalog_partition_spec.toString()

    def __hash__(self):
        return self._j_catalog_partition_spec.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_catalog_partition_spec.equals(
            other._j_catalog_partition_spec)

    def get_partition_spec(self) -> Dict[str, str]:
        """
        Get the partition spec as key-value map.

        :return: A map of partition spec keys and values.
        """
        return dict(self._j_catalog_partition_spec.getPartitionSpec())
