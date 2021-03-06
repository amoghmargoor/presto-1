/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.connector.system.jdbc;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static io.prestosql.connector.system.SystemConnectorSessionUtil.toSession;
import static io.prestosql.connector.system.jdbc.FilterUtil.filter;
import static io.prestosql.connector.system.jdbc.FilterUtil.stringFilter;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listTableColumns;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class ColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "columns");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", createUnboundedVarcharType())
            .column("table_schem", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("column_name", createUnboundedVarcharType())
            .column("data_type", BIGINT)
            .column("type_name", createUnboundedVarcharType())
            .column("column_size", BIGINT)
            .column("buffer_length", BIGINT)
            .column("decimal_digits", BIGINT)
            .column("num_prec_radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", createUnboundedVarcharType())
            .column("column_def", createUnboundedVarcharType())
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", createUnboundedVarcharType())
            .column("scope_catalog", createUnboundedVarcharType())
            .column("scope_schema", createUnboundedVarcharType())
            .column("scope_table", createUnboundedVarcharType())
            .column("source_data_type", BIGINT)
            .column("is_autoincrement", createUnboundedVarcharType())
            .column("is_generatedcolumn", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public ColumnJdbcTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = toSession(transactionHandle, connectorSession);
        Optional<String> catalogFilter = stringFilter(constraint, 0);
        Optional<String> schemaFilter = stringFilter(constraint, 1);
        Optional<String> tableFilter = stringFilter(constraint, 2);

        Builder table = InMemoryRecordSet.builder(METADATA);
        for (String catalog : filter(listCatalogs(session, metadata, accessControl).keySet(), catalogFilter)) {
            QualifiedTablePrefix prefix = FilterUtil.tablePrefix(catalog, schemaFilter, tableFilter);
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : listTableColumns(session, metadata, accessControl, prefix).entrySet()) {
                addColumnRows(table, catalog, entry.getKey(), entry.getValue());
            }
        }
        return table.build().cursor();
    }

    private static void addColumnRows(Builder builder, String catalog, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        int ordinalPosition = 1;
        for (ColumnMetadata column : columns) {
            if (column.isHidden()) {
                continue;
            }
            builder.addRow(
                    catalog,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    column.getName(),
                    jdbcDataType(column.getType()),
                    column.getType().getDisplayName(),
                    columnSize(column.getType()),
                    0,
                    decimalDigits(column.getType()),
                    numPrecRadix(column.getType()),
                    DatabaseMetaData.columnNullableUnknown,
                    column.getComment(),
                    null,
                    null,
                    null,
                    charOctetLength(column.getType()),
                    ordinalPosition,
                    "",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            ordinalPosition++;
        }
    }

    static int jdbcDataType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        }
        if (type.equals(BIGINT)) {
            return Types.BIGINT;
        }
        if (type.equals(INTEGER)) {
            return Types.INTEGER;
        }
        if (type.equals(SMALLINT)) {
            return Types.SMALLINT;
        }
        if (type.equals(TINYINT)) {
            return Types.TINYINT;
        }
        if (type.equals(REAL)) {
            return Types.REAL;
        }
        if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        }
        if (type instanceof DecimalType) {
            return Types.DECIMAL;
        }
        if (isVarcharType(type)) {
            return Types.VARCHAR;
        }
        if (isCharType(type)) {
            return Types.CHAR;
        }
        if (type.equals(VARBINARY)) {
            return Types.VARBINARY;
        }
        if (type.equals(TIME)) {
            return Types.TIME;
        }
        if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Types.TIME_WITH_TIMEZONE;
        }
        if (type.equals(TIMESTAMP)) {
            return Types.TIMESTAMP;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (type.equals(DATE)) {
            return Types.DATE;
        }
        if (type instanceof ArrayType) {
            return Types.ARRAY;
        }
        return Types.JAVA_OBJECT;
    }

    static Integer columnSize(Type type)
    {
        if (type.equals(BIGINT)) {
            return 19;  // 2**63-1
        }
        if (type.equals(INTEGER)) {
            return 10;  // 2**31-1
        }
        if (type.equals(SMALLINT)) {
            return 5;   // 2**15-1
        }
        if (type.equals(TINYINT)) {
            return 3;   // 2**7-1
        }
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getPrecision();
        }
        if (type.equals(REAL)) {
            return 24; // IEEE 754
        }
        if (type.equals(DOUBLE)) {
            return 53; // IEEE 754
        }
        if (isVarcharType(type)) {
            return ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (isCharType(type)) {
            return ((CharType) type).getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        if (type.equals(TIME)) {
            return 8; // 00:00:00
        }
        if (type.equals(TIME_WITH_TIME_ZONE)) {
            return 8 + 6; // 00:00:00+00:00
        }
        if (type.equals(DATE)) {
            return 14; // +5881580-07-11 (2**31-1 days)
        }
        if (type.equals(TIMESTAMP)) {
            return 15 + 8;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return 15 + 8 + 6;
        }
        return null;
    }

    // DECIMAL_DIGITS is the number of fractional digits
    private static Integer decimalDigits(Type type)
    {
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getScale();
        }
        return null;
    }

    private static Integer charOctetLength(Type type)
    {
        if (isVarcharType(type)) {
            return ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (isCharType(type)) {
            return ((CharType) type).getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        return null;
    }

    static Integer numPrecRadix(Type type)
    {
        if (type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                (type instanceof DecimalType)) {
            return 10;
        }
        if (type.equals(REAL) || type.equals(DOUBLE)) {
            return 2;
        }
        return null;
    }
}
