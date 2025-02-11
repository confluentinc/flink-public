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


class ObjectPath(object):
    """
    A database name and object (table/view/function) name combo in a catalog.
    """

    def __init__(self, database_name=None, object_name=None, j_object_path=None):
        if j_object_path is None:
            gateway = get_gateway()
            self._j_object_path = gateway.jvm.ObjectPath(database_name, object_name)
        else:
            self._j_object_path = j_object_path

    def __str__(self):
        return self._j_object_path.toString()

    def __hash__(self):
        return self._j_object_path.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_object_path.equals(
            other._j_object_path)

    def get_database_name(self) -> str:
        return self._j_object_path.getDatabaseName()

    def get_object_name(self) -> str:
        return self._j_object_path.getObjectName()

    def get_full_name(self) -> str:
        return self._j_object_path.getFullName()

    @staticmethod
    def from_string(full_name: str) -> 'ObjectPath':
        gateway = get_gateway()
        return ObjectPath(j_object_path=gateway.jvm.ObjectPath.fromString(full_name))
