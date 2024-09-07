using MigrateDB.Classes.DB.Connection;

namespace MigrateDB.Classes.DB
{
    public class DbManager(DbType dbType, string connectionString, int maxConnections)
    {
        private readonly DbType dbType = dbType;

        private readonly string connectionString = connectionString;

        private readonly int maxConnections = maxConnections;

        private readonly SemaphoreSlim semaphore = new(maxConnections, maxConnections);

        public string GetBusyState()
        {
            return $"Занято подключений: {this.semaphore.CurrentCount} / {this.maxConnections}";
        }

        public async Task<DbConnection> GetConnection()
        {
            await semaphore.WaitAsync();

            if (dbType == DbType.MSSQL)
            {
                return new MSSQLConnection(connectionString, OnConnectionClose);
            }

            return new PGSQLConnection(connectionString, OnConnectionClose);
        }

        private void OnConnectionClose()
        {
            semaphore.Release();
        }
    }
}
