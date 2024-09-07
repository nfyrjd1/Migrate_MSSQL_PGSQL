
using System.Data;

namespace MigrateDB.Classes.DB.Connection
{
    public delegate void OnConnectionClose();

    abstract public class DbConnection(string connectionString, OnConnectionClose onConnectionClose)
    {
        protected OnConnectionClose onConnectionClose = onConnectionClose;

        public IDbConnection Connection { get; protected set; }
    }
}
