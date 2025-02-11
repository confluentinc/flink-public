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
from typing import Dict

class CatalogModel(object):
    """
    Interface for a model in a catalog.
    """

    def __init__(self, j_catalog_model):
        self._j_catalog_model = j_catalog_model

    @staticmethod
    def create_model(
        input_schema: Schema,
        output_schema: Schema,
        options: Dict[str, str] = {},
        comment: str = None
    ) -> "CatalogModel":
        """
        Create an instance of CatalogModel for the catalog model.

        :param input_schema: the model input schema
        :param output_schema: the model output schema
        :param options: the properties of the catalog model
        :param comment: the comment of the catalog model
        """
        assert input_schema is not None
        assert output_schema is not None
        assert options is not None

        gateway = get_gateway()
        return CatalogModel(
            gateway.jvm.org.apache.flink.table.catalog.CatalogModel.of(
                input_schema._j_schema, output_schema._j_schema, options, comment))

    @staticmethod
    def _get(j_catalog_model):
        return CatalogModel(j_catalog_model)

    def copy(self) -> 'CatalogModel':
        """
        Create a deep copy of the model.

        :return: A deep copy of "this" instance.
        """
        return CatalogModel(self._j_catalog_model.copy())

    def get_comment(self) -> str:
        """
        Get comment of the model.

        :return: Comment of model.
        """
        return self._j_catalog_model.getComment()

    def get_options(self):
        """
        Returns a map of string-based options.

        :return: Property map of the model.

        .. versionadded:: 1.11.0
        """
        return dict(self._j_catalog_model.getOptions())
