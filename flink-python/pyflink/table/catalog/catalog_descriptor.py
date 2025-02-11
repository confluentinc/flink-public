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

from pyflink.common.configuration import Configuration

class CatalogDescriptor:
    """
    Describes a catalog with the catalog name and configuration.
    A CatalogDescriptor is a template for creating a catalog instance. It closely resembles the
    "CREATE CATALOG" SQL DDL statement, containing catalog name and catalog configuration.
    """
    def __init__(self, j_catalog_descriptor):
        self._j_catalog_descriptor = j_catalog_descriptor

    @staticmethod
    def of(catalog_name: str, configuration: Configuration, comment: str = None):
        assert catalog_name is not None
        assert configuration is not None

        from pyflink.java_gateway import get_gateway
        gateway = get_gateway()

        j_catalog_descriptor = gateway.jvm.org.apache.flink.table.catalog.CatalogDescriptor.of(
            catalog_name, configuration._j_configuration, comment)
        return CatalogDescriptor(j_catalog_descriptor)
