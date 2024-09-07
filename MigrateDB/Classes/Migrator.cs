using Microsoft.Data.SqlClient;
using MigrateDB.Classes.DB;
using MigrateDB.Classes.DB.Connection;
using Npgsql;
using NpgsqlTypes;
using System.Data;

namespace MigrateDB.Classes
{
    public delegate void OutputFunction(string line);

    public class Migrator(Settings settings, OutputFunction outputFunction)
    {
        private readonly List<string> IgnoreTables = settings.IgnoreTables;

        private readonly DbManager MSSQL_DbManager = new(DB.DbType.MSSQL, settings.SourceConnectionString, settings.MaxConnections);

        private readonly DbManager PGSQL_DbManager = new(DB.DbType.PGSQL, settings.TargetConnectionString, settings.MaxConnections);

        private readonly OutputFunction outputFunction = outputFunction;

        List<Table> tables = [];

        List<string> completedTables = [];

        public async Task Migrate()
        {
            TableStructure tableStructure = new(this.PGSQL_DbManager);
            this.tables = await tableStructure.GetAllTablesStructure();
            this.completedTables = FileManager.Read(FileManager.FileType.Tables).Split("\n").ToList();

            await ExecuteScriptBefore();

            AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.SuppressInsecureTLSWarning", true);
            IEnumerable<Task> tasks = tables.Select(TransferDataAsync);
            await Task.WhenAll(tasks);

            await ExecuteScriptAfter();

            FileManager.Delete(FileManager.FileType.Tables);
        }

        async Task TransferDataAsync(Table table)
        {
            string tableName = table.Name;

            if (this.IgnoreTables.Contains(tableName) || this.completedTables.Contains(tableName))
            {
                return;
            }

            try
            {
                MSSQLConnection msSqlConnection = (MSSQLConnection) await MSSQL_DbManager.GetConnection();
                PGSQLConnection pgSqlConnection = (PGSQLConnection) await PGSQL_DbManager.GetConnection();

                this.outputFunction($"Начало переноса данных в таблицу: {tableName}. {this.PGSQL_DbManager.GetBusyState()}");

                string truncate = $"truncate \"{table.Name}\" cascade;";
                await this.ExecuteScript(pgSqlConnection, truncate, false);

                string columns = string.Join(", ", table.Columns.Select(column => $"\"{column.Name}\""));
                string selectQuery = $"SELECT {columns} FROM \"{tableName}\"";
                SqlCommand sqlCommand = new(selectQuery, msSqlConnection.Connection)
                {
                    CommandTimeout = 900
                };

                SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection);
                NpgsqlBinaryImporter writer = pgSqlConnection.Connection.BeginBinaryImport($"COPY \"{tableName}\" ({columns}) FROM STDIN (FORMAT BINARY)");
                writer.Timeout = TimeSpan.FromSeconds(900);

                long lastId = 0;
                while (await sqlDataReader.ReadAsync())
                {
                    object[] values = new object[sqlDataReader.FieldCount];
                    sqlDataReader.GetValues(values);

                    await writer.StartRowAsync(CancellationToken.None);
                    for (int i = 0; i < table.Columns.Count; i++)
                    {
                        if (table.Columns[i].IsIdentityColumn)
                        {
                            lastId = long.Parse(values[i].ToString());
                        }

                        if (table.Columns[i].DataType != NpgsqlDbType.Boolean && (values[i].ToString() == "True" || values[i].ToString() == "False"))
                        {
                            values[i] = values[i].ToString() == "True" ? 1 : 0;
                        }

                        writer.Write(values[i], table.Columns[i].DataType);
                    }
                }

                await sqlDataReader.CloseAsync();
                await writer.CompleteAsync();
                await writer.CloseAsync();

                if (table.IdentityColumn != null)
                {
                    lastId += 1;
                    string resetIdentity = $"ALTER SEQUENCE \"{table.IdentityColumn.SequenceName}\" RESTART WITH {lastId};";
                    await this.ExecuteScript(pgSqlConnection, resetIdentity, false);
                }

                await FileManager.WriteLineAsync(FileManager.FileType.Tables, tableName);
                this.completedTables.Add(tableName);

                msSqlConnection.Close();
                pgSqlConnection.Close();

                this.outputFunction($"Вставка для таблицы {tableName} - закончена. {this.PGSQL_DbManager.GetBusyState()}");
            }
            catch (PostgresException px)
            {
                this.outputFunction(@$"Таблица {tableName}.
                    Severity: {px.Data["Severity"]}
                    SqlState: {px.Data["SqlState"]}
                    MessageText: {px.Message}
                    Where: {px.Data["Where"]}, line {px.Data["Line"]}, column {px.Data["Column"]}
                    File: {px.Data["File"]}
                    Line: {px.Data["Line"]}
                    px: {px}
                ");
                throw;
            }
            catch (SqlException se)
            {
                this.outputFunction($"Таблица {tableName}. Message: {se.Message}");
                throw;
            }
            catch (Exception ex)
            {
                this.outputFunction($"Таблица {tableName}. Message: {ex.Message}");
                throw;
            }
        }

        async Task ExecuteScriptBefore()
        {
            string script = string.Join(
                "",
                tables.Select(t => string.Join(
                "\r\n",
                    t.ForeignKeys.Select(f => $"ALTER TABLE \"{t.Name}\" DROP CONSTRAINT IF EXISTS \"{f.Name}\";"))
                )
            ).Trim();

            if (string.IsNullOrEmpty(script))
            {
                return;
            }

            string afterScript = string.Join(
                "",
                tables.Select(t => string.Join(
                    "\r\n",
                    t.ForeignKeys.Select(f => $"ALTER TABLE \"{t.Name}\" ADD CONSTRAINT \"{f.Name}\" FOREIGN KEY ({f.ColumnName}) REFERENCES {f.ReferencedTable} ({f.ReferencedColumn});"))
                )
            );
            FileManager.Write(FileManager.FileType.AfterScript, afterScript);

            PGSQLConnection connection = (PGSQLConnection) await this.PGSQL_DbManager.GetConnection();
            await ExecuteScript(connection, script, true, "Подготовка БД.", "Скрипт подготовки к миграции успешно выполнен.");
        }

        async Task ExecuteScriptAfter()
        {
            string script = FileManager.Read(FileManager.FileType.AfterScript);
            if (string.IsNullOrEmpty(script))
            {
                return;
            }

            PGSQLConnection connection = (PGSQLConnection) await this.PGSQL_DbManager.GetConnection();
            await ExecuteScript(connection, script, true, "Восстановление настроек БД.", "Сброшенные настройки БД восстановлены.");

            FileManager.Delete(FileManager.FileType.AfterScript);
        }

        async Task ExecuteScript(PGSQLConnection connection, string script, bool closeConnection = true, string? messageBefore = null, string? messageAfter = null)
        {
            try
            {
                NpgsqlCommand command = new(script, connection.Connection)
                {
                    CommandTimeout = 900
                };

                if (!string.IsNullOrEmpty(messageBefore))
                {
                    this.outputFunction(messageBefore);
                }
                
                await command.ExecuteNonQueryAsync();

                if (!string.IsNullOrEmpty(messageAfter))
                {
                    this.outputFunction(messageAfter);
                }

                if (closeConnection == true)
                {
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                this.outputFunction($"Ошибка выполнения скрипта: {ex.Message}.");
                throw;
            }
        }
    }
}
