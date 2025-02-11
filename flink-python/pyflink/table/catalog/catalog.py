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
from typing import List
from .catalog_database import CatalogDatabase
from .catalog_base_table import CatalogBaseTable
from .catalog_partition_spec import CatalogPartitionSpec
from .catalog_partition import CatalogPartition
from .catalog_function import CatalogFunction
from .procedure import Procedure
from .catalog_model import CatalogModel
from .catalog_table_statistics import CatalogTableStatistics
from .catalog_column_statistics import CatalogColumnStatistics
from .object_path import ObjectPath

class Catalog(object):
    """
    Catalog is responsible for reading and writing metadata such as database/table/views/UDFs
    from a registered catalog. It connects a registered catalog and Flink's Table API.
    """

    def __init__(self, j_catalog):
        self._j_catalog = j_catalog

    def get_default_database(self) -> str:
        """
        Get the name of the default database for this catalog. The default database will be the
        current database for the catalog when user's session doesn't specify a current database.
        The value probably comes from configuration, will not change for the life time of the
        catalog instance.

        :return: The name of the current database.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.getDefaultDatabase()

    def list_databases(self) -> List[str]:
        """
        Get the names of all databases in this catalog.

        :return: A list of the names of all databases.
        :raise: CatalogException in case of any runtime exception.
        """
        return list(self._j_catalog.listDatabases())

    def get_database(self, database_name: str) -> 'CatalogDatabase':
        """
        Get a database from this catalog.

        :param database_name: Name of the database.
        :return: The requested database :class:`CatalogDatabase`.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return CatalogDatabase._get(self._j_catalog.getDatabase(database_name))

    def database_exists(self, database_name: str) -> bool:
        """
        Check if a database exists in this catalog.

        :param database_name: Name of the database.
        :return: true if the given database exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.databaseExists(database_name)

    def create_database(self, name: str, database: 'CatalogDatabase', ignore_if_exists: bool):
        """
        Create a database.

        :param name: Name of the database to be created.
        :param database: The :class:`CatalogDatabase` database definition.
        :param ignore_if_exists: Flag to specify behavior when a database with the given name
                                 already exists:
                                 if set to false, throw a DatabaseAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseAlreadyExistException if the given database already exists and
                ignoreIfExists is false.
        """
        self._j_catalog.createDatabase(name, database._j_catalog_database, ignore_if_exists)

    def drop_database(self, name: str, ignore_if_exists: bool):
        """
        Drop a database.

        :param name: Name of the database to be dropped.
        :param ignore_if_exists: Flag to specify behavior when the database does not exist:
                                 if set to false, throw an exception,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the given database does not exist.
        """
        self._j_catalog.dropDatabase(name, ignore_if_exists)

    def alter_database(self, name: str, new_database: 'CatalogDatabase',
                       ignore_if_not_exists: bool):
        """
        Modify an existing database.

        :param name: Name of the database to be modified.
        :param new_database: The new database :class:`CatalogDatabase` definition.
        :param ignore_if_not_exists: Flag to specify behavior when the given database does not
                                     exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the given database does not exist.
        """
        self._j_catalog.alterDatabase(name, new_database._j_catalog_database, ignore_if_not_exists)

    def list_tables(self, database_name: str) -> List[str]:
        """
        Get names of all tables and views under this database. An empty list is returned if none
        exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all tables and views in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listTables(database_name))

    def list_views(self, database_name: str) -> List[str]:
        """
        Get names of all views under this database. An empty list is returned if none exists.

        :param database_name: Name of the given database.
        :return: A list of the names of all views in the given database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listViews(database_name))

    def get_table(self, table_path: 'ObjectPath') -> 'CatalogBaseTable':
        """
        Get a CatalogTable or CatalogView identified by tablePath.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: The requested table or view :class:`CatalogBaseTable`.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the target does not exist.
        """
        return CatalogBaseTable._get(self._j_catalog.getTable(table_path._j_object_path))

    def table_exists(self, table_path: 'ObjectPath') -> bool:
        """
        Check if a table or view exists in this catalog.

        :param table_path: Path :class:`ObjectPath` of the table or view.
        :return: true if the given table exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.tableExists(table_path._j_object_path)

    def drop_table(self, table_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table or view does not exist.
        """
        self._j_catalog.dropTable(table_path._j_object_path, ignore_if_not_exists)

    def rename_table(self, table_path: 'ObjectPath', new_table_name: str,
                     ignore_if_not_exists: bool):
        """
        Rename an existing table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be renamed.
        :param new_table_name: The new name of the table or view.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist.
        """
        self._j_catalog.renameTable(table_path._j_object_path, new_table_name, ignore_if_not_exists)

    def create_table(self, table_path: 'ObjectPath', table: 'CatalogBaseTable',
                     ignore_if_exists: bool):
        """
        Create a new table or view.

        :param table_path: Path :class:`ObjectPath` of the table or view to be created.
        :param table: The table definition :class:`CatalogBaseTable`.
        :param ignore_if_exists: Flag to specify behavior when a table or view already exists at
                                 the given path:
                                 if set to false, it throws a TableAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database in tablePath doesn't exist.
                TableAlreadyExistException if table already exists and ignoreIfExists is false.
        """
        self._j_catalog.createTable(table_path._j_object_path, table._j_catalog_base_table,
                                    ignore_if_exists)

    def alter_table(self, table_path: 'ObjectPath', new_table: 'CatalogBaseTable',
                    ignore_if_not_exists):
        """
        Modify an existing table or view.
        Note that the new and old CatalogBaseTable must be of the same type. For example,
        this doesn't allow alter a regular table to partitioned table, or alter a view to a table,
        and vice versa.

        :param table_path: Path :class:`ObjectPath` of the table or view to be modified.
        :param new_table: The new table definition :class:`CatalogBaseTable`.
        :param ignore_if_not_exists: Flag to specify behavior when the table or view does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist.
        """
        self._j_catalog.alterTable(table_path._j_object_path, new_table._j_catalog_base_table,
                                   ignore_if_not_exists)

    def list_partitions(self,
                        table_path: 'ObjectPath',
                        partition_spec: 'CatalogPartitionSpec' = None)\
            -> List['CatalogPartitionSpec']:
        """
        Get CatalogPartitionSpec of all partitions of the table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: The partition spec :class:`CatalogPartitionSpec` to list.
        :return: A list of :class:`CatalogPartitionSpec` of the table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException thrown if the table does not exist in the catalog.
                TableNotPartitionedException thrown if the table is not partitioned.
        """
        if partition_spec is None:
            return [CatalogPartitionSpec(p) for p in self._j_catalog.listPartitions(
                table_path._j_object_path)]
        else:
            return [CatalogPartitionSpec(p) for p in self._j_catalog.listPartitions(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec)]

    def get_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogPartition':
        """
        Get a partition of the given table.
        The given partition spec keys and values need to be matched exactly for a result.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: The partition spec :class:`CatalogPartitionSpec` of partition to get.
        :return: The requested partition :class:`CatalogPartition`.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the partition doesn't exist.
        """
        return CatalogPartition._get(self._j_catalog.getPartition(
            table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def partition_exists(self, table_path: 'ObjectPath',
                         partition_spec: 'CatalogPartitionSpec') -> bool:
        """
        Check whether a partition exists or not.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               check.
        :return: true if the partition exists.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.partitionExists(
            table_path._j_object_path, partition_spec._j_catalog_partition_spec)

    def create_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                         partition: 'CatalogPartition', ignore_if_exists: bool):
        """
        Create a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param partition: The partition :class:`CatalogPartition` to add.
        :param ignore_if_exists: Flag to specify behavior if a table with the given name already
                                 exists:
                                 if set to false, it throws a TableAlreadyExistException,
                                 if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException thrown if the target table does not exist.
                TableNotPartitionedException thrown if the target table is not partitioned.
                PartitionSpecInvalidException thrown if the given partition spec is invalid.
                PartitionAlreadyExistsException thrown if the target partition already exists.
        """
        self._j_catalog.createPartition(table_path._j_object_path,
                                        partition_spec._j_catalog_partition_spec,
                                        partition._j_catalog_partition,
                                        ignore_if_exists)

    def drop_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                       ignore_if_not_exists: bool):
        """
        Drop a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               drop.
        :param ignore_if_not_exists: Flag to specify behavior if the database does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the target partition does not exist.
        """
        self._j_catalog.dropPartition(table_path._j_object_path,
                                      partition_spec._j_catalog_partition_spec,
                                      ignore_if_not_exists)

    def alter_partition(self, table_path: 'ObjectPath', partition_spec: 'CatalogPartitionSpec',
                        new_partition: 'CatalogPartition', ignore_if_not_exists: bool):
        """
        Alter a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition to
                               alter.
        :param new_partition: New partition :class:`CatalogPartition` to replace the old one.
        :param ignore_if_not_exists: Flag to specify behavior if the database does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException thrown if the target partition does not exist.
        """
        self._j_catalog.alterPartition(table_path._j_object_path,
                                       partition_spec._j_catalog_partition_spec,
                                       new_partition._j_catalog_partition,
                                       ignore_if_not_exists)

    def list_functions(self, database_name: str) -> List[str]:
        """
        List the names of all functions in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the functions in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listFunctions(database_name))

    def list_procedures(self, database_name: str) -> List[str]:
        """
        List the names of all procedures in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the procedures in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listProcedures(database_name))

    def get_function(self, function_path: 'ObjectPath') -> 'CatalogFunction':
        """
        Get the function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: The requested function :class:`CatalogFunction`.
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist in the catalog.
        """
        return CatalogFunction._get(self._j_catalog.getFunction(function_path._j_object_path))

    def get_procedure(self, procedure_path: 'ObjectPath') -> 'Procedure':
        """
        Get the procedure.

        :param procedure_path: Path :class:`ObjectPath` of the procedure.
        :return: The requested procedure :class:`Procedure`.
        :raise: CatalogException in case of any runtime exception.
                ProcedureNotExistException if the procedure does not exist in the catalog.
        """
        return Procedure._get(self._j_catalog.getProcedure(procedure_path._j_object_path))

    def function_exists(self, function_path: 'ObjectPath') -> bool:
        """
        Check whether a function exists or not.

        :param function_path: Path :class:`ObjectPath` of the function.
        :return: true if the function exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.functionExists(function_path._j_object_path)

    def create_function(self, function_path: 'ObjectPath', function: 'CatalogFunction',
                        ignore_if_exists: bool):
        """
        Create a function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :param function: The function :class:`CatalogFunction` to be created.
        :param ignore_if_exists: Flag to specify behavior if a function with the given name
                                 already exists:
                                 if set to false, it throws a FunctionAlreadyExistException,
                                 if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                FunctionAlreadyExistException if the function already exist.
                DatabaseNotExistException     if the given database does not exist.
        """
        self._j_catalog.createFunction(function_path._j_object_path,
                                       function._j_catalog_function,
                                       ignore_if_exists)

    def alter_function(self, function_path: 'ObjectPath', new_function: 'CatalogFunction',
                       ignore_if_not_exists: bool):
        """
        Modify an existing function.

        :param function_path: Path :class:`ObjectPath` of the function.
        :param new_function: The function :class:`CatalogFunction` to be modified.
        :param ignore_if_not_exists: Flag to specify behavior if the function does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist.
        """
        self._j_catalog.alterFunction(function_path._j_object_path,
                                      new_function._j_catalog_function,
                                      ignore_if_not_exists)

    def drop_function(self, function_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a function.

        :param function_path: Path :class:`ObjectPath` of the function to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior if the function does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                FunctionNotExistException if the function does not exist.
        """
        self._j_catalog.dropFunction(function_path._j_object_path, ignore_if_not_exists)

    def list_models(self, database_name: str) -> List[str]:
        """
        List the names of all models in the given database. An empty list is returned if none is
        registered.

        :param database_name: Name of the database.
        :return: A list of the names of the models in this database.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database does not exist.
        """
        return list(self._j_catalog.listModels(database_name))

    def get_model(self, model_path: 'ObjectPath') -> 'CatalogModel':
        """
        Get the model.

        :param model_path: Path :class:`ObjectPath` of the model.
        :return: The requested :class:`CatalogModel`.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist in the catalog.
        """
        return CatalogModel._get(self._j_catalog.getModel(model_path._j_object_path))

    def model_exists(self, model_path: 'ObjectPath') -> bool:
        """
        Check whether a model exists or not.

        :param model_path: Path :class:`ObjectPath` of the model.
        :return: true if the model exists in the catalog false otherwise.
        :raise: CatalogException in case of any runtime exception.
        """
        return self._j_catalog.modelExists(model_path._j_object_path)

    def drop_model(self, model_path: 'ObjectPath', ignore_if_not_exists: bool):
        """
        Drop a model.

        :param model_path: Path :class:`ObjectPath` of the model to be dropped.
        :param ignore_if_not_exists: Flag to specify behavior if the model does not exist:
                                     if set to false, throw an exception
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.dropModel(model_path._j_object_path, ignore_if_not_exists)

    def rename_model(self, model_path: 'ObjectPath', new_model_name: str,
                     ignore_if_not_exists: bool):
        """
        Rename an existing model.

        :param model_path: Path :class:`ObjectPath` of the model to be renamed.
        :param new_model_name: The new name of the model.
        :param ignore_if_not_exists: Flag to specify behavior when the model does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.renameModel(model_path._j_object_path, new_model_name, ignore_if_not_exists)

    def create_model(self, model_path: 'ObjectPath', model: 'CatalogModel',
                     ignore_if_exists: bool):
        """
        Create a new model.

        :param model_path: Path :class:`ObjectPath` of the model to be created.
        :param model: The model definition :class:`CatalogModel`.
        :param ignore_if_exists: Flag to specify behavior when a model already exists at
                                 the given path:
                                 if set to false, it throws a ModelAlreadyExistException,
                                 if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                DatabaseNotExistException if the database in tablePath doesn't exist.
                ModelAlreadyExistException if model already exists and ignoreIfExists is false.
        """
        self._j_catalog.createModel(model_path._j_object_path, model._j_catalog_model,
                                    ignore_if_exists)

    def alter_model(self, model_path: 'ObjectPath', new_model: 'CatalogModel',
                    ignore_if_not_exists):
        """
        Modify an existing model.

        :param model_path: Path :class:`ObjectPath` of the model to be modified.
        :param new_model: The new model definition :class:`CatalogModel`.
        :param ignore_if_not_exists: Flag to specify behavior when the model does not exist:
                                     if set to false, throw an exception,
                                     if set to true, do nothing.
        :raise: CatalogException in case of any runtime exception.
                ModelNotExistException if the model does not exist.
        """
        self._j_catalog.alterModel(model_path._j_object_path, new_model._j_catalog_model,
                                   ignore_if_not_exists)

    def get_table_statistics(self, table_path: 'ObjectPath') -> 'CatalogTableStatistics':
        """
        Get the statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :return: The statistics :class:`CatalogTableStatistics` of the given table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog.getTableStatistics(
                table_path._j_object_path))

    def get_table_column_statistics(self, table_path: 'ObjectPath') -> 'CatalogColumnStatistics':
        """
        Get the column statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :return: The column statistics :class:`CatalogColumnStatistics` of the given table.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog.getTableColumnStatistics(
                table_path._j_object_path))

    def get_partition_statistics(self,
                                 table_path: 'ObjectPath',
                                 partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogTableStatistics':
        """
        Get the statistics of a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :return: The statistics :class:`CatalogTableStatistics` of the given partition.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return CatalogTableStatistics(
            j_catalog_table_statistics=self._j_catalog.getPartitionStatistics(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def bulk_get_partition_statistics(self,
                                      table_path: 'ObjectPath',
                                      partition_specs: List['CatalogPartitionSpec']) \
            -> List['CatalogTableStatistics']:
        """
        Get a list of statistics of given partitions.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_specs: The list of :class:`CatalogPartitionSpec` of the given partitions.
        :return: The statistics list of :class:`CatalogTableStatistics` of the given partitions.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return [CatalogTableStatistics(j_catalog_table_statistics=p)
                for p in self._j_catalog.bulkGetPartitionStatistics(table_path._j_object_path,
                partition_specs)]

    def get_partition_column_statistics(self,
                                        table_path: 'ObjectPath',
                                        partition_spec: 'CatalogPartitionSpec') \
            -> 'CatalogColumnStatistics':
        """
        Get the column statistics of a partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :return: The column statistics :class:`CatalogColumnStatistics` of the given partition.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return CatalogColumnStatistics(
            j_catalog_column_statistics=self._j_catalog.getPartitionColumnStatistics(
                table_path._j_object_path, partition_spec._j_catalog_partition_spec))

    def bulk_get_partition_column_statistics(self,
                                             table_path: 'ObjectPath',
                                             partition_specs: List['CatalogPartitionSpec']) \
            -> List['CatalogColumnStatistics']:
        """
        Get a list of the column statistics for the given partitions.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_specs: The list of :class:`CatalogPartitionSpec` of the given partitions.
        :return: The statistics list of :class:`CatalogTableStatistics` of the given partitions.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        return [CatalogColumnStatistics(j_catalog_column_statistics=p)
                for p in self._j_catalog.bulkGetPartitionStatistics(
                table_path._j_object_path, partition_specs)]

    def alter_table_statistics(self,
                               table_path: 'ObjectPath',
                               table_statistics: 'CatalogTableStatistics',
                               ignore_if_not_exists: bool):
        """
        Update the statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param table_statistics: New statistics :class:`CatalogTableStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the table does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        self._j_catalog.alterTableStatistics(
            table_path._j_object_path,
            table_statistics._j_catalog_table_statistics,
            ignore_if_not_exists)

    def alter_table_column_statistics(self,
                                      table_path: 'ObjectPath',
                                      column_statistics: 'CatalogColumnStatistics',
                                      ignore_if_not_exists: bool):
        """
        Update the column statistics of a table.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param column_statistics: New column statistics :class:`CatalogColumnStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the column does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                TableNotExistException if the table does not exist in the catalog.
        """
        self._j_catalog.alterTableColumnStatistics(
            table_path._j_object_path,
            column_statistics._j_catalog_column_statistics,
            ignore_if_not_exists)

    def alter_partition_statistics(self,
                                   table_path: 'ObjectPath',
                                   partition_spec: 'CatalogPartitionSpec',
                                   partition_statistics: 'CatalogTableStatistics',
                                   ignore_if_not_exists: bool):
        """
        Update the statistics of a table partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param partition_statistics: New statistics :class:`CatalogTableStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the partition does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        self._j_catalog.alterPartitionStatistics(
            table_path._j_object_path,
            partition_spec._j_catalog_partition_spec,
            partition_statistics._j_catalog_table_statistics,
            ignore_if_not_exists)

    def alter_partition_column_statistics(self,
                                          table_path: 'ObjectPath',
                                          partition_spec: 'CatalogPartitionSpec',
                                          column_statistics: 'CatalogColumnStatistics',
                                          ignore_if_not_exists: bool):
        """
        Update the column statistics of a table partition.

        :param table_path: Path :class:`ObjectPath` of the table.
        :param partition_spec: Partition spec :class:`CatalogPartitionSpec` of the partition.
        :param column_statistics: New column statistics :class:`CatalogColumnStatistics` to update.
        :param ignore_if_not_exists: Flag to specify behavior if the partition does not exist:
                                     if set to false, throw an exception,
                                     if set to true, nothing happens.
        :raise: CatalogException in case of any runtime exception.
                PartitionNotExistException if the partition does not exist.
        """
        self._j_catalog.alterPartitionColumnStatistics(
            table_path._j_object_path,
            partition_spec._j_catalog_partition_spec,
            column_statistics._j_catalog_column_statistics,
            ignore_if_not_exists)

class HiveCatalog(Catalog):
    """
    A catalog implementation for Hive.
    """

    def __init__(self, catalog_name: str, default_database: str = None, hive_conf_dir: str = None,
                 hadoop_conf_dir: str = None, hive_version: str = None):
        assert catalog_name is not None

        gateway = get_gateway()

        j_hive_catalog = gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
            catalog_name, default_database, hive_conf_dir, hadoop_conf_dir, hive_version)
        super(HiveCatalog, self).__init__(j_hive_catalog)

class JdbcCatalog(Catalog):
    """
    A catalog implementation for Jdbc.
    """
    def __init__(self, catalog_name: str, default_database: str, username: str, pwd: str,
                 base_url: str):
        assert catalog_name is not None
        assert default_database is not None
        assert username is not None
        assert pwd is not None
        assert base_url is not None

        from pyflink.java_gateway import get_gateway
        gateway = get_gateway()

        j_jdbc_catalog = gateway.jvm.org.apache.flink.connector.jdbc.catalog.JdbcCatalog(
            catalog_name, default_database, username, pwd, base_url)
        super(JdbcCatalog, self).__init__(j_jdbc_catalog)
