using MigrateDB.Classes;

public class Program
{
    static void Main(string[] args)
    {
        try
        {
            WriteLine("Начало миграции");

            Settings settings = Settings.ParseFromFile();
            Migrator migrator = new(settings, WriteLine);
            migrator.Migrate().Wait();

            WriteLine("Миграция завершена");
        }
        catch (Exception ex)
        {
            WriteLine(ex.Message);
        } 
        finally
        {
            Console.WriteLine("\nНажмите Enter чтобы выйти");
            Console.ReadLine();
        }
    }

    public static void WriteLine(string line)
    {
        line = $"{DateTime.Now.ToString()}: {line}";
        Console.WriteLine(line);
        FileManager.WriteLineAsync(FileManager.FileType.Logs, line).Wait();
    }
}