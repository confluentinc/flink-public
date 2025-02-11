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
from pyflink.table.schema import Schema
from pyflink.table.table_schema import TableSchema
from typing import Dict, List, Optional


class CatalogBaseTable(object):
    """
    CatalogBaseTable is the common parent of table and view. It has a map of
    key-value pairs defining the properties of the table.
    """

    def __init__(self, j_catalog_base_table):
        self._j_catalog_base_table = j_catalog_base_table

    @staticmethod
    def create_table(
        schema: TableSchema,
        partition_keys: List[str] = [],
        properties: Dict[str, str] = {},
        comment: str = None
    ) -> "CatalogBaseTable":
        """
        Create an instance of CatalogBaseTable for the catalog table.

        :param schema: the table schema
        :param partition_keys: the partition keys, default empty
        :param properties: the properties of the catalog table
        :param comment: the comment of the catalog table
        """
        assert schema is not None
        assert partition_keys is not None
        assert properties is not None

        gateway = get_gateway()
        return CatalogBaseTable(
            gateway.jvm.org.apache.flink.table.catalog.CatalogTable.newBuilder()
            .schema(schema._j_table_schema.toSchema())
            .comment(comment)
            .partitionKeys(partition_keys)
            .options(properties)
            .build())

    @staticmethod
    def create_view(
        original_query: str,
        expanded_query: str,
        schema: TableSchema,
        properties: Dict[str, str],
        comment: str = None
    ) -> "CatalogBaseTable":
        """
        Create an instance of CatalogBaseTable for the catalog view.

        :param original_query: the original text of the view definition
        :param expanded_query: the expanded text of the original view definition, this is needed
                               because the context such as current DB is lost after the session,
                               in which view is defined, is gone. Expanded query text takes care
                               of the this, as an example.
        :param schema: the table schema
        :param properties: the properties of the catalog view
        :param comment: the comment of the catalog view
        """
        assert original_query is not None
        assert expanded_query is not None
        assert schema is not None
        assert properties is not None

        gateway = get_gateway()
        return CatalogBaseTable(
            gateway.jvm.org.apache.flink.table.catalog.CatalogViewImpl(
                original_query, expanded_query, schema._j_table_schema, properties, comment))

    @staticmethod
    def _get(j_catalog_base_table):
        return CatalogBaseTable(j_catalog_base_table)

    def get_options(self):
        """
        Returns a map of string-based options.

        In case of CatalogTable, these options may determine the kind of connector and its
        configuration for accessing the data in the external system.

        :return: Property map of the table/view.

        .. versionadded:: 1.11.0
        """
        return dict(self._j_catalog_base_table.getOptions())

    def get_unresolved_schema(self) -> Schema:
        """
        Returns the schema of the table or view.

        The schema can reference objects from other catalogs and will be resolved and validated by
        the framework when accessing the table or view.
        """
        return Schema(self._j_catalog_base_table.getUnresolvedSchema())

    def get_comment(self) -> str:
        """
        Get comment of the table or view.

        :return: Comment of the table/view.
        """
        return self._j_catalog_base_table.getComment()

    def copy(self) -> 'CatalogBaseTable':
        """
        Get a deep copy of the CatalogBaseTable instance.

        :return: An copy of the CatalogBaseTable instance.
        """
        return CatalogBaseTable(self._j_catalog_base_table.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the table or view.

        :return: An optional short description of the table/view.
        """
        description = self._j_catalog_base_table.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the table or view.

        :return: An optional long description of the table/view.
        """
        detailed_description = self._j_catalog_base_table.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None
