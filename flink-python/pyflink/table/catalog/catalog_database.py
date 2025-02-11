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


class CatalogDatabase(object):
    """
    Represents a database object in a catalog.
    """

    def __init__(self, j_catalog_database):
        self._j_catalog_database = j_catalog_database

    @staticmethod
    def create_instance(
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogDatabase":
        """
        Creates an instance of CatalogDatabase.

        :param properties: Property of the database
        :param comment: Comment of the database
        """
        assert properties is not None

        gateway = get_gateway()
        return CatalogDatabase(gateway.jvm.org.apache.flink.table.catalog.CatalogDatabaseImpl(
            properties, comment))

    @staticmethod
    def _get(j_catalog_database):
        return CatalogDatabase(j_catalog_database)

    def get_properties(self) -> Dict[str, str]:
        """
        Get a map of properties associated with the database.
        """
        return dict(self._j_catalog_database.getProperties())

    def get_comment(self) -> str:
        """
        Get comment of the database.

        :return: Comment of the database.
        """
        return self._j_catalog_database.getComment()

    def copy(self) -> 'CatalogDatabase':
        """
        Get a deep copy of the CatalogDatabase instance.

        :return: A copy of CatalogDatabase instance.
        """
        return CatalogDatabase(self._j_catalog_database.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the database.

        :return: An optional short description of the database.
        """
        description = self._j_catalog_database.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the database.

        :return: An optional long description of the database.
        """
        detailed_description = self._j_catalog_database.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None
