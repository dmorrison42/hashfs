using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

namespace hashfs
{
    class Program
    {

        private static SHA256 Sha256 = SHA256.Create();

        private static string GetHash(string filename)
        {
            var hash = GetHashSha256(filename);
            if (hash == null) return null;
            return BitConverter
                .ToString(hash)
                .Replace("-", string.Empty);
        }

        private static byte[] GetHashSha256(string filename)
        {
            try
            {
                using (FileStream stream = File.OpenRead(filename))
                {
                    return Sha256.ComputeHash(stream);
                }
            }
            catch
            {
                return null;
            }
        }

        static JObject ToJson(string path)
        {
            var obj = JObject.FromObject(new
            {
                name = ".",
                children = new JArray(),
            });

            using var con = new SQLiteConnection($@"URI=file:{path}");
            con.Open();

            using var cmd = new SQLiteCommand(con);

            cmd.CommandText = @"SELECT * FROM files";

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var filePath = reader.GetString(0);
                var segments = filePath.Split(new char[] { '\\', '/' });
                var siblings = (JArray)obj["children"];

                foreach (var segment in segments.Take(segments.Length - 1))
                {
                    while (true)
                    {
                        var next = siblings.FirstOrDefault(s => s["name"].ToObject<string>() == segment);
                        if (next == null)
                        {
                            siblings.Add(JObject.FromObject(new
                            {
                                name = segment,
                                children = new JArray(),
                            }));
                        }
                        else
                        {
                            siblings = (JArray)next["children"];
                            break;
                        }
                    }
                }
                siblings.Add(JObject.FromObject(new
                {
                    // Modified = reader.GetString(2),
                    name = segments.Last(),
                    value = reader.GetInt64(1),
                }));
            }

            return obj;
        }

        static void Main(string[] args)
        {
            var database = @".\hashes.db";
            var path = ".";

            if (args.Length >= 1 && args[0] == "--tojson")
            {
                if (args.Length >= 2)
                {
                    database = args[1];
                }
                Console.WriteLine(ToJson(database).ToString());
                return;
            }

            if (args.Length >= 1)
            {
                path = args[0];
            }

            if (args.Length >= 2)
            {
                database = args[1];
            }

            var cs = $@"URI=file:{database}";

            using var con = new SQLiteConnection(cs);
            con.Open();
            using var cmd = new SQLiteCommand(con);
            if (args.Length == 1)
            {
                path = args[0];
            }
            cmd.CommandText = @"CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, size INT, modified TEXT, hash TEXT)";
            cmd.ExecuteNonQuery();
            var hungItems = new List<(string Path, Task Task)>();

            long fileCount = 0;
            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                var task = Task.Run(() =>
                {
                    var info = new System.IO.FileInfo(filePath);
                    var length = info.Length;
                    var modified = info.LastWriteTime.ToString("o");

                    cmd.CommandText = @"SELECT * FROM files WHERE path=@path";
                    cmd.Parameters.AddWithValue("@path", filePath);

                    using var reader = cmd.ExecuteReader();
                    reader.Read();

                    if (fileCount++ % 100 == 0)
                    {
                        Console.WriteLine(fileCount - 1);
                    }

                    if (!reader.HasRows || filePath != reader.GetString(0) || length != reader.GetInt64(1) || modified != reader.GetString(2))
                    {
                        reader.Close();
                        var hash = GetHash(filePath);
                        cmd.CommandText = "INSERT OR REPLACE INTO files(path, size, modified, hash) VALUES(@path, @size, @modified, @hash)";
                        cmd.Parameters.AddWithValue("@path", filePath);
                        cmd.Parameters.AddWithValue("@size", length);
                        cmd.Parameters.AddWithValue("@modified", modified);
                        cmd.Parameters.AddWithValue("@hash", hash);
                        cmd.ExecuteNonQuery();
                    };
                });

                task.Wait(1 * 60 * 1000);
                if (!task.IsCompleted)
                {
                    hungItems.Add((filePath, task));
                }

                for (var i = hungItems.Count - 1; i >= 0; i--)
                {
                    var item = hungItems[i];
                    if (item.Task.IsCompleted)
                    {
                        hungItems.RemoveAt(i);
                    }
                    else
                    {
                        Console.WriteLine($"HUNG: {item.Path}");
                    }
                }
            }
        }
    }
}
