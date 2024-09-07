
namespace MigrateDB.Classes
{
    public class Settings
    {
        public string SourceConnectionString { get; private set; } = "";
        public string TargetConnectionString { get; private set; } = "";

        public int MaxConnections { get; private set; } = 100;

        public List<string> IgnoreTables { get; private set; } = [];

        public static Settings ParseFromFile()
        {
            if (!FileManager.IsExists(FileManager.FileType.Settings))
            {
                CreateSettingsFile();
                throw new Exception("Не найден файл настроек");
            }

            string text = FileManager.Read(FileManager.FileType.Settings);
            List<string> lines = text.Split('\n').ToList();

            Settings settings = new();
            foreach (string line in lines)
            {
                List<string> parts = line.Split('=', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList(); // Разбиваем по первому равно
                if (parts.Count < 2)
                {
                    throw new Exception($"Не удалось прочитать настройки: {line}");
                }

                string key = parts[0];
                string value = parts[1];

                switch (key.ToLower())
                {
                    case "source":
                        settings.SourceConnectionString= value;
                        break;
                    case "target":
                        settings.TargetConnectionString = value;
                        break;
                    case "maxconnections":
                        if (int.TryParse(value, out int maxConnections))
                        {
                            settings.MaxConnections = maxConnections;
                        }
                        break;
                    case "ignoretables":
                        settings.IgnoreTables = value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
                        break;
                }
            }

            return settings;
        }

        static void CreateSettingsFile() {
            List<string> lines = [];

            lines.Add("# Строка подключения к базе mssql, откуда тянем данные");
            lines.Add("source=Data Source=***;Initial Catalog=***;User ID=***;Password=***;TrustServerCertificate=true;Encrypt=false;");

            lines.Add("");
            lines.Add("# Строка подключения к базе pgsql, куда тянем данные");
            lines.Add("target=Host=***;Port=***;Username=***;Password=***;Database=***;");

            lines.Add("");
            lines.Add("# Максимальное количество подключений");
            lines.Add("maxConnections=100");

            lines.Add("");
            lines.Add("# Таблицы, которые нужно проигнорировать при переносе (через запятую)");
            lines.Add("ignoreTables=migrations, user_session");

            string settings = string.Join("\n", lines);
            FileManager.Write(FileManager.FileType.Settings, settings);
        }
    }
}
