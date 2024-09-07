using Microsoft.Data.SqlClient;

namespace MigrateDB.Classes.DB.Connection
{
    internal class MSSQLConnection : DbConnection
    {
        public new SqlConnection Connection { get; protected set; }

        public MSSQLConnection(string connectionString, OnConnectionClose onConnectionClose) : base(connectionString, onConnectionClose)
        {
            Connection = new SqlConnection(connectionString);
            Connection.Open();
        }

        public void Close()
        {
            Connection.Close();
            this.onConnectionClose();
        }
    }
}
