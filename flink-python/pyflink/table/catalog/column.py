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

from py4j.java_gateway import get_java_class

from pyflink.java_gateway import get_gateway
from pyflink.table.resolved_expression import ResolvedExpression
from pyflink.table.types import DataType, _to_java_data_type, _from_java_data_type
from typing import Optional
from abc import ABCMeta, abstractmethod


class Column(metaclass=ABCMeta):
    """
    Representation of a column in a :class:`~pyflink.table.ResolvedSchema`.

    A table column describes either a :class:`PhysicalColumn`, :class:`ComputedColumn`, or
    :class:`MetadataColumn`.

    Every column is fully resolved. The enclosed :class:`~pyflink.table.types.DataType`
    indicates whether the column is a time attribute and thus might differ from the original
    data type.
    """

    def __init__(self, j_column):
        self._j_column = j_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_column.equals(other._j_column)

    def __hash__(self):
        return self._j_column.hashCode()

    def __str__(self):
        return self._j_column.toString()

    @staticmethod
    def _from_j_column(j_column) -> Optional["Column"]:
        """
        Returns a non-abstract column, either a :class:`PhysicalColumn`, a :class:`ComputedColumn`,
        or a :class:`MetadataColumn` from an org.apache.flink.table.catalog.Column.
        """
        if j_column is None:
            return None
        gateway = get_gateway()
        JColumn = gateway.jvm.org.apache.flink.table.catalog.Column
        JPhysicalColumn = gateway.jvm.org.apache.flink.table.catalog.Column.PhysicalColumn
        JComputedColumn = gateway.jvm.org.apache.flink.table.catalog.Column.ComputedColumn
        JMetadataColumn = gateway.jvm.org.apache.flink.table.catalog.Column.MetadataColumn
        j_clz = j_column.getClass()

        if not get_java_class(JColumn).isAssignableFrom(j_clz):
            raise TypeError("The input %s is not an instance of Column." % j_column)

        if get_java_class(JPhysicalColumn).isAssignableFrom(j_column.getClass()):
            return PhysicalColumn(j_physical_column=j_column.getClass())
        elif get_java_class(JComputedColumn).isAssignableFrom(j_column.getClass()):
            return MetaDataColumn(j_metadata_column=j_column.getClass())
        elif get_java_class(JMetadataColumn).isAssignableFrom(j_column.getClass()):
            return MetaDataColumn(j_metadata_column=j_column.getClass())
        else:
            return None

    @staticmethod
    def physical(name: str, data_type: DataType) -> "PhysicalColumn":
        """
        Creates a regular table column that represents physical data.
        """
        gateway = get_gateway()
        j_data_type = _to_java_data_type(data_type)
        j_physical_column = gateway.jvm.org.apache.flink.table.catalog.Column.physical(
            name, j_data_type
        )
        return PhysicalColumn(j_physical_column)

    @staticmethod
    def computed(name: str, resolved_expression: ResolvedExpression) -> "ComputedColumn":
        """
        Creates a computed column that is computed from the given
        :class:`~pyflink.table.ResolvedExpression`.
        """
        gateway = get_gateway()
        j_resolved_expression = resolved_expression
        j_computed_column = gateway.jvm.org.apache.flink.table.catalog.Column.computed(
            name, j_resolved_expression
        )
        return ComputedColumn(j_computed_column)

    @staticmethod
    def metadata(
        name: str, data_type: DataType, metadata_key: Optional[str], is_virtual: bool
    ) -> "MetaDataColumn":
        """
        Creates a metadata column from metadata of the given column name or from metadata of the
        given key (if not null).

        Allows to specify whether the column is virtual or not.
        """
        gateway = get_gateway()
        j_data_type = _to_java_data_type(data_type)
        j_metadata_column = gateway.jvm.org.apache.flink.table.catalog.Column.metadata(
            name, j_data_type, metadata_key, is_virtual
        )
        return MetaDataColumn(j_metadata_column)

    @abstractmethod
    def with_comment(self, comment: Optional[str]):
        """
        Add the comment to the column and return the new object.
        """
        pass

    @abstractmethod
    def is_physical(self) -> bool:
        """
        Returns whether the given column is a physical column of a table; neither computed nor
        metadata.
        """
        pass

    @abstractmethod
    def is_persisted(self) -> bool:
        """
        Returns whether the given column is persisted in a sink operation.
        """
        pass

    def get_data_type(self) -> DataType:
        """
        Returns the data type of this column.
        """
        j_data_type = self._j_column.getDataType()
        return DataType(_from_java_data_type(j_data_type))

    def get_name(self):
        """
        Returns the name of this column.
        """
        return self._j_column.getName()

    def get_comment(self) -> Optional[str]:
        """
        Returns the comment of this column.
        """
        optional_result = self._j_column.getComment()
        return optional_result.get() if optional_result.isPresent() else None

    def as_summary_string(self) -> str:
        """
        Returns a string that summarizes this column for printing to a console.
        """
        return self._j_column.asSummaryString()

    @abstractmethod
    def explain_extras(self) -> Optional[str]:
        """
        Returns an explanation of specific column extras next to name and type.
        """
        pass

    @abstractmethod
    def copy(self, new_type: DataType) -> "Column":
        """
        Returns a copy of the column with a replaced :class:`~pyflink.table.types.DataType`.
        """
        pass

    @abstractmethod
    def rename(self, new_name: str) -> "Column":
        """
        Returns a copy of the column with a replaced name.
        """
        pass


class PhysicalColumn(Column):
    """
    Representation of a physical columns.
    """

    def __init__(self, j_physical_column):
        super().__init__(j_physical_column)
        self._j_physical_column = j_physical_column

    def with_comment(self, comment: str) -> "PhysicalColumn":
        return self._j_physical_column.withComment(comment)

    def is_physical(self) -> bool:
        return True

    def is_persisted(self) -> bool:
        return True

    def explain_extras(self) -> Optional[str]:
        return None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_physical_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_physical_column.rename(new_name)


class ComputedColumn(Column):
    """
    Representation of a computed column.
    """

    def __init__(self, j_computed_column):
        super().__init__(j_computed_column)
        self._j_computed_column = j_computed_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_computed_column.equals(
            other._j_computed_column
        )

    def __hash__(self):
        return self._j_computed_column.hashCode()

    def with_comment(self, comment: str) -> "PhysicalColumn":
        return self._j_computed_column.withComment(comment)

    def is_physical(self) -> bool:
        return False

    def is_persisted(self) -> bool:
        return False

    def get_expression(self) -> None:
        return self._j_computed_column.getExpression()

    def explain_extras(self) -> Optional[str]:
        optional_result = self._j_computed_column.explainExtras()
        return optional_result.get() if optional_result.isPresent() else None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_computed_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_computed_column.rename(new_name)


class MetaDataColumn(Column):
    """
    Representation of a metadata column.
    """

    def __init__(self, j_metadata_column):
        super().__init__(j_metadata_column)
        self._j_metadata_column = j_metadata_column

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_metadata_column.equals(
            other._j_metadata_column
        )

    def __hash__(self):
        return self._j_metadata_column.hashCode()

    def is_virtual(self) -> bool:
        return self._j_metadata_column.isVirtual()

    def get_metadata_key(self) -> Optional[str]:
        optional_result = self._j_metadata_column.getMetadataKey()
        return optional_result.get() if optional_result.isPresent() else None

    def with_comment(self, comment: str) -> "MetaDataColumn":
        return self._j_metadata_column.withComment(comment)

    def is_physical(self) -> bool:
        return False

    def is_persisted(self) -> bool:
        return self._j_metadata_column.isPersisted()

    def explain_extras(self) -> Optional[str]:
        optional_result = self._j_metadata_column.explainExtras()
        return optional_result.get() if optional_result.isPresent() else None

    def copy(self, new_data_type: DataType) -> Column:
        return self._j_metadata_column.copy(new_data_type)

    def rename(self, new_name: str) -> Column:
        return self._j_metadata_column.rename(new_name)
