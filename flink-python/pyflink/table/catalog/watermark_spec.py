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
from pyflink.table.resolved_expression import ResolvedExpression


class WatermarkSpec:
    """
    Representation of a watermark specification in :class:`~pyflink.table.ResolvedSchema`.

    It defines the rowtime attribute and a :class:`~pyflink.table.ResolvedExpression`
    for watermark generation.
    """

    def __init__(self, j_watermark_spec):
        self._j_watermark_spec = j_watermark_spec

    def __str__(self):
        return self._j_watermark_spec.toString()

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._j_watermark_spec.equals(
            other._j_watermark_spec
        )

    def __hash__(self):
        return self._j_watermark_spec.hashCode()

    @staticmethod
    def of(rowtime_attribute: str, watermark_expression: ResolvedExpression):
        """
        Creates a :class:`WatermarkSpec` from a given rowtime attribute and a watermark
        expression.
        """
        gateway = get_gateway()
        j_watermark_spec = gateway.jvm.org.apache.flink.table.catalog.WatermarkSpec.of(
            rowtime_attribute, watermark_expression._j_resolved_expr
        )
        return WatermarkSpec(j_watermark_spec)

    def get_rowtime_attribute(self) -> str:
        """
        Returns the name of a rowtime attribute.

        The referenced attribute must be present in the :class:`~pyflink.table.ResolvedSchema`
        and must be of :class:`~pyflink.table.types.TimestampType`
        """
        return self._j_watermark_spec.getRowtimeAttribute()

    def get_watermark_expression(self) -> ResolvedExpression:
        """
        Returns the :class:`~pyflink.table.ResolvedExpression` for watermark generation.
        """
        j_watermark_expression = self._j_watermark_spec.getWatermarkExpression()
        return ResolvedExpression(j_watermark_expression)

    def as_summary_string(self) -> str:
        """
        Prints the watermark spec in a readable way.
        """
        return self._j_watermark_spec.asSummaryString()
