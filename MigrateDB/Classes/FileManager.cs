namespace MigrateDB.Classes
{
    public static class FileManager
    {
        const string AFTER_SCRIPT_FILE_PATH = "./after_script.txt";
        const string TABLES_FILE_PATH = "./tables.txt";
        const string SETTINGS_FILE_PATH = "./settings.txt";

        public enum FileType {
            AfterScript,
            Tables,
            Settings,
        };

        static Task? CurrentWrite = null;

        public static string GetFilePath(FileType type) {
            switch (type)
            {
                case FileType.AfterScript:
                    {
                        return AFTER_SCRIPT_FILE_PATH;
                    }
                case FileType.Tables:
                    {
                        return TABLES_FILE_PATH;
                    }
                case FileType.Settings:
                    {
                        return SETTINGS_FILE_PATH;
                    }
            }

            throw new NotImplementedException();
        }

        public static bool IsExists(FileType type)
        {
            return File.Exists(GetFilePath(type));
        }

        public static string Read(FileType type)
        {
            if (!IsExists(type))
            {
                return "";
            }

            try
            {
                return string.Join("\n",
                    File.ReadAllLines(GetFilePath(type))
                        .Select(line => line.Trim())
                        .Where(line => !string.IsNullOrEmpty(line) && !line.StartsWith('#'))
                );
            }
            catch (Exception ex)
            {
                throw new Exception($"Произошла ошибка при чтении файла: {ex.Message}");
            }
        }

        public static void Write(FileType type, string text)
        {
            try
            {
                File.WriteAllText(GetFilePath(type), text);
            }
            catch (Exception ex)
            {
                // Обработка ошибок
                throw new Exception($"Произошла ошибка при записи файла: {ex.Message}");
            }
        }

        public static async Task WriteLineAsync(FileType type, string line)
        {
            if (CurrentWrite != null && !CurrentWrite.IsCompleted)
            {
                await CurrentWrite;
            }

            CurrentWrite = WriteLineHelper(type, line);
        }

        public static async Task WriteLineHelper(FileType type, string line)
        {
            // Открываем файл и используем поток в режиме добавления
            using FileStream stream = new(GetFilePath(type), FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
            using StreamWriter writer = new(stream);
            // Асинхронная запись строки
            await writer.WriteLineAsync(line);
            await writer.FlushAsync(); // Сбрасываем буфер в файл
        }

        public static void Delete(FileType type)
        {
            try
            {
                if (IsExists(type))
                {
                    File.Delete(GetFilePath(type));
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Произошла ошибка при удалении файла: {ex.Message}");
            }
        }
    }
}
