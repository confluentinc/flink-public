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
from typing import Dict, Optional


class CatalogPartition(object):
    """
    Represents a partition object in catalog.
    """

    def __init__(self, j_catalog_partition):
        self._j_catalog_partition = j_catalog_partition

    @staticmethod
    def create_instance(
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogPartition":
        """
        Creates an instance of CatalogPartition.

        :param properties: Property of the partition
        :param comment: Comment of the partition
        """
        assert properties is not None

        gateway = get_gateway()
        return CatalogPartition(
            gateway.jvm.org.apache.flink.table.catalog.CatalogPartitionImpl(
                properties, comment))

    @staticmethod
    def _get(j_catalog_partition):
        return CatalogPartition(j_catalog_partition)

    def get_properties(self) -> Dict[str, str]:
        """
        Get a map of properties associated with the partition.

        :return: A map of properties with the partition.
        """
        return dict(self._j_catalog_partition.getProperties())

    def copy(self) -> 'CatalogPartition':
        """
        Get a deep copy of the CatalogPartition instance.

        :return: A copy of CatalogPartition instance.
        """
        return CatalogPartition(self._j_catalog_partition.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the partition object.

        :return: An optional short description of partition object.
        """
        description = self._j_catalog_partition.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the partition object.

        :return: An optional long description of the partition object.
        """
        detailed_description = self._j_catalog_partition.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None

    def get_comment(self) -> str:
        """
        Get comment of the partition.

        :return: Comment of the partition.
        """
        return self._j_catalog_partition.getComment()
