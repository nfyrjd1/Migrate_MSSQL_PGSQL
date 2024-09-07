using MigrateDB.Classes.DB.Connection;
using Npgsql;
using NpgsqlTypes;

namespace MigrateDB.Classes.DB
{
    public class Table
    {
        public required string Name { get; set; }
        public Column? IdentityColumn { get; set; }
        public required List<Column> Columns { get; set; }
        public required List<ForeignKey> ForeignKeys { get; set; }
    }

    public class Column
    {
        public required string Name { get; set; }
        public required NpgsqlDbType DataType { get; set; }
        public string? SequenceName { get; set; } = null;
        public bool IsIdentityColumn {
            get {
                return this.SequenceName != null;
            }
        }
    }

    public class ForeignKey
    {
        public required string Name { get; set; }
        public required string ColumnName { get; set; }
        public required string ReferencedTable { get; set; }
        public required string ReferencedColumn { get; set; }
    }

    public class TableStructure(DbManager dbManager)
    {
        private readonly DbManager dbManager = dbManager;

        public async Task<List<Table>> GetAllTablesStructure()
        {
            PGSQLConnection connection = (PGSQLConnection)await dbManager.GetConnection();

            List<Table> tables = GetTables(connection);
            tables = SetTablesForeignKeys(connection, tables);

            connection.Close();
            return tables;
        }

        private List<Table> GetTables(PGSQLConnection connection)
        {
            NpgsqlCommand command = new(@"
                SELECT 
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    CASE 
                        WHEN c.column_default LIKE 'nextval(%' THEN 
                            regexp_replace(c.column_default, 'nextval\(\''?(.+?)\''?\s*::regclass\)', '\1')
                        ELSE NULL 
                    END AS sequence_name
                FROM 
                    information_schema.columns AS c
                WHERE 
                    c.table_schema = 'public'
                ORDER BY 
                    c.table_name, 
                    c.ordinal_position;
            ", connection.Connection);
            NpgsqlDataReader reader = command.ExecuteReader();

            Dictionary<string, List<Column>> columns = [];

            while (reader.Read())
            {
                string tableName = reader.GetString(reader.GetOrdinal("table_name"));
                if (!columns.ContainsKey(tableName))
                {
                    columns.Add(tableName, []);
                }

                int sequenceColumn = reader.GetOrdinal("sequence_name");
                columns[tableName].Add(new Column()
                {
                    Name = reader.GetString(reader.GetOrdinal("column_name")),
                    DataType = GetColumnDataType(reader.GetString(reader.GetOrdinal("data_type"))),
                    SequenceName = reader.IsDBNull(sequenceColumn) ? null : reader.GetString(sequenceColumn)
                });
            }
            reader.Close();

            List<Table> tables = [];
            foreach (KeyValuePair<string, List<Column>> keyValuePair in columns)
            {
                Column? identityColumn = keyValuePair.Value.Find((Column column) => column.SequenceName != null);
                tables.Add(new Table
                {
                    Name = keyValuePair.Key,
                    IdentityColumn = identityColumn,
                    Columns = keyValuePair.Value,
                    ForeignKeys = []
                });
            }

            return tables;
        }

        private static NpgsqlDbType GetColumnDataType(string rawDataType)
        {
            switch (rawDataType)
            {
                case "character varying":
                case "text":
                    {
                        return NpgsqlDbType.Text;
                    }
                case "timestamp without time zone":
                    {
                        return NpgsqlDbType.Timestamp;
                    }
                case "integer":
                    {
                        return NpgsqlDbType.Integer;
                    }
                case "smallint":
                    {
                        return NpgsqlDbType.Smallint;
                    }
                case "boolean":
                    {
                        return NpgsqlDbType.Boolean;
                    }
                case "double precision":
                    {
                        return NpgsqlDbType.Double;
                    }
                case "numeric":
                    {
                        return NpgsqlDbType.Numeric;
                    }
                case "bigint":
                    {
                        return NpgsqlDbType.Bigint;
                    }
            }

            return NpgsqlDbType.Unknown;
        }

        private static List<Table> SetTablesForeignKeys(PGSQLConnection connection, List<Table> tables)
        {
            string query = $@"SELECT 
                                conname AS fk_name, 
                                conrelid::regclass::text AS table_name, 
                                a.attname AS column_name,
                                confrelid::regclass::text AS foreign_table_name,
                                af.attname AS foreign_column_name
                            FROM pg_constraint AS c
                                JOIN pg_attribute AS a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
                                JOIN pg_attribute AS af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
                            WHERE c.contype = 'f';";
            NpgsqlCommand command = new(query, connection.Connection);
            NpgsqlDataReader reader = command.ExecuteReader();

            Dictionary<string, List<ForeignKey>> foreignKeys = [];

            while (reader.Read())
            {
                string tableName = reader.GetString(reader.GetOrdinal("table_name")).Replace("\"", "");
                string constraintName = reader.GetString(reader.GetOrdinal("fk_name"));
                string columnName = reader.GetString(reader.GetOrdinal("column_name"));
                string referencedTable = reader.GetString(reader.GetOrdinal("foreign_table_name"));
                string referencedColumn = reader.GetString(reader.GetOrdinal("foreign_column_name"));

                // Create foreign key object
                ForeignKey foreignKey = new()
                {
                    Name = constraintName,
                    ColumnName = columnName,
                    ReferencedTable = referencedTable,
                    ReferencedColumn = referencedColumn
                };

                if (!foreignKeys.ContainsKey(tableName))
                {
                    foreignKeys.Add(tableName, []);
                }

                foreignKeys[tableName].Add(foreignKey);
            }

            reader.Close();

            foreach (Table table in tables)
            {
                table.ForeignKeys = foreignKeys.ContainsKey(table.Name) ? foreignKeys[table.Name] : [];
            }
            return tables;
        }
    }
}
