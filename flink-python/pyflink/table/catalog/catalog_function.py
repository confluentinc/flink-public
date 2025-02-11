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

from typing import Optional


class CatalogFunction(object):
    """
    Interface for a function in a catalog.
    """

    def __init__(self, j_catalog_function):
        self._j_catalog_function = j_catalog_function

    @staticmethod
    def create_instance(
        class_name: str,
        function_language: str = 'Python'
    ) -> "CatalogFunction":
        """
        Creates an instance of CatalogDatabase.

        :param class_name: full qualified path of the class name
        :param function_language: language of the function, must be one of
                                  'Python', 'Java' or 'Scala'. (default Python)
        """
        assert class_name is not None

        gateway = get_gateway()
        FunctionLanguage = gateway.jvm.org.apache.flink.table.catalog.FunctionLanguage
        if function_language.lower() == 'python':
            function_language = FunctionLanguage.PYTHON
        elif function_language.lower() == 'java':
            function_language = FunctionLanguage.JAVA
        elif function_language.lower() == 'scala':
            function_language = FunctionLanguage.SCALA
        else:
            raise ValueError("function_language must be one of 'Python', 'Java' or 'Scala'")
        return CatalogFunction(
            gateway.jvm.org.apache.flink.table.catalog.CatalogFunctionImpl(
                class_name, function_language))

    @staticmethod
    def _get(j_catalog_function):
        return CatalogFunction(j_catalog_function)

    def get_class_name(self) -> str:
        """
        Get the full name of the class backing the function.

        :return: The full name of the class.
        """
        return self._j_catalog_function.getClassName()

    def copy(self) -> 'CatalogFunction':
        """
        Create a deep copy of the function.

        :return: A deep copy of "this" instance.
        """
        return CatalogFunction(self._j_catalog_function.copy())

    def get_description(self) -> Optional[str]:
        """
        Get a brief description of the function.

        :return: An optional short description of function.
        """
        description = self._j_catalog_function.getDescription()
        if description.isPresent():
            return description.get()
        else:
            return None

    def get_detailed_description(self) -> Optional[str]:
        """
        Get a detailed description of the function.

        :return: An optional long description of the function.
        """
        detailed_description = self._j_catalog_function.getDetailedDescription()
        if detailed_description.isPresent():
            return detailed_description.get()
        else:
            return None

    def get_function_language(self):
        """
        Get the language used for the function definition.

        :return: the language type of the function definition

        .. versionadded:: 1.10.0
        """
        return self._j_catalog_function.getFunctionLanguage()
