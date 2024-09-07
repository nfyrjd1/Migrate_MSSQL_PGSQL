using Npgsql;

namespace MigrateDB.Classes.DB.Connection
{
    internal class PGSQLConnection : DbConnection
    {
        public new NpgsqlConnection Connection { get; protected set; }

        public PGSQLConnection(string connectionString, OnConnectionClose onConnectionClose) : base(connectionString, onConnectionClose)
        {
            Connection = new NpgsqlConnection(connectionString);
            Connection.Open();
        }

        public void Close()
        {
            Connection.Close();
            this.onConnectionClose();
        }
    }
}
