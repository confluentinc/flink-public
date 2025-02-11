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

from enum import Enum
from pyflink.java_gateway import get_gateway
from typing import List
from abc import ABCMeta



class Constraint(metaclass=ABCMeta):
    """
    Integrity constraints, generally referred to simply as constraints, define the valid states of
    SQL-data by constraining the values in the base tables.
    """

    def __init__(self, j_constraint):
        self._j_constraint = j_constraint

    def get_name(self) -> str:
        """
        Returns the name of the constraint.
        """
        return self._j_constraint.getName()

    def is_enforced(self) -> bool:
        """
        Constraints can either be enforced or non-enforced. If a constraint is enforced it will be
        checked whenever any SQL statement is executed that results in data or schema changes. If
        the constraint is not enforced the owner of the data is responsible for ensuring data
        integrity.
        Flink will rely on the information as valid and might use it for query optimisations.
        """
        return self._j_constraint.isEnforced()

    def get_type(self) -> "ConstraintType":
        """
        Returns the type of the constraint, which could be `PRIMARY_KEY` or `UNIQUE_KEY`.
        """
        j_constraint_type = self._j_constraint.getType().name()
        return self.ConstraintType[j_constraint_type]

    def as_summary_string(self) -> str:
        """
        Prints the constraint in a readable way.
        """
        return self._j_constraint.asSummaryString()

    class ConstraintType(Enum):
        """
        Type of the constraint.

        Unique constraints:

        - UNIQUE - is satisfied if and only if there do not exist two rows that have same
         non-null values in the unique columns
        - PRIMARY KEY - additionally to UNIQUE constraint, it requires none of the values in
          specified columns be a null value. Moreover there can be only a single PRIMARY KEY
          defined for a Table.
        """

        PRIMARY_KEY = 0
        UNIQUE_KEY = 1


class UniqueConstraint(Constraint):
    """
    A unique key constraint. It can be declared also as a PRIMARY KEY.
    """

    def __init__(self, name: str = None, columns: List[str] = None, j_unique_constraint=None):
        """
        Creates a non enforced PRIMARY_KEY constraint.
        """
        if j_unique_constraint is None:
            gateway = get_gateway()
            self._j_unique_constraint = gateway.jvm.org.apache.flink.table.catalog.UniqueConstraint(
                name, columns
            )
            super().__init__(self._j_unique_constraint)
        else:
            self._j_unique_constraint = j_unique_constraint
            super().__init__(j_unique_constraint)

    def get_columns(self) -> List[str]:
        """
        List of column names for which the primary key was defined.
        """
        return self._j_unique_constraint.getColumns()

    def get_type_string(self) -> str:
        """
        Returns a string representation of the underlying constraint type.
        """
        return self._j_unique_constraint.getTypeString()
