using System.Threading;
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

        static void InitializeDatabase(SQLiteConnection con)
        {
            using var cmd = new SQLiteCommand(con);
            cmd.CommandText = @"CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, size INT, modified TEXT, hash TEXT)";
            cmd.ExecuteNonQuery();
        }

        static Task<bool> ProcessPathAsync(SQLiteConnection con, string filePath)
        {
            return Task.Run<bool>(() =>
            {
                var info = new System.IO.FileInfo(filePath);
                var length = info.Length;
                var modified = info.LastWriteTime.ToString("o");

                using var readCommand = new SQLiteCommand(@"SELECT * FROM files WHERE path=@path", con);
                readCommand.Parameters.AddWithValue("@path", filePath);
                using var reader = readCommand.ExecuteReader();
                reader.Read();


                if (reader.HasRows && filePath == reader.GetString(0) && length == reader.GetInt64(1) && modified == reader.GetString(2))
                {
                    return false;
                }

                var hash = GetHash(filePath);
                using var cmd = new SQLiteCommand(
                    "INSERT OR REPLACE INTO files(path, size, modified, hash) VALUES(@path, @size, @modified, @hash)", con);
                cmd.Parameters.AddWithValue("@path", filePath);
                cmd.Parameters.AddWithValue("@size", length);
                cmd.Parameters.AddWithValue("@modified", modified);
                cmd.Parameters.AddWithValue("@hash", hash);
                cmd.ExecuteNonQuery();
                return true;
            });
        }

        static void AddHashes(SQLiteConnection con, string path)
        {
            var waitTime = 60 * 1000;
            var runningItems = new List<(string Path, Task Task)>();
            string hungMessage = null;

            Task.Run(() =>
            {
                while (true)
                {
                    Console.Write(hungMessage);
                    Thread.Sleep(waitTime);
                }
            });

            long fileCount = 0;
            long hashCount = 0;
            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                var task = ProcessPathAsync(con, filePath);
                task.Wait(waitTime);
                runningItems.Add((filePath, task));
                runningItems = runningItems.Where(i =>
                {
                    if (!task.IsCompleted) return false;
                    if (task.Result) hashCount++;
                    return true;
                }).ToList();

                hungMessage = string.Join("", runningItems.Select(i => i.Path + "\n"));

                for (var i = runningItems.Count - 1; i >= 0; i--)
                {
                    var item = runningItems[i];
                    if (item.Task.IsCompleted)
                    {
                        runningItems.RemoveAt(i);
                    }
                    else
                    {
                        Console.WriteLine($"HUNG: {item.Path}");
                    }
                }

                if (++fileCount % 100 == 0)
                {
                    Console.WriteLine($"{fileCount}:{hashCount}");
                }
            }
        }

        static void RemoveMissing(SQLiteConnection con)
        {
            using var cmd = new SQLiteCommand(con);

            cmd.CommandText = @"SELECT path FROM files";

            using var reader = cmd.ExecuteReader();
            long entries = 0;
            while (reader.Read())
            {
                if (++entries % 100 == 0)
                {
                    Console.WriteLine(entries);
                }
                var filePath = reader.GetString(0);
                if (!File.Exists(filePath))
                {
                    var rmCmd = new SQLiteCommand("DELETE FROM files WHERE path = @path", con);
                    rmCmd.Parameters.AddWithValue("@path", filePath);
                    rmCmd.ExecuteNonQuery();
                }
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

            if (args.Length >= 1) path = args[0];
            if (args.Length >= 2) database = args[1];

            var cs = $@"URI=file:{database}";

            using var con = new SQLiteConnection(cs);
            con.Open();
            InitializeDatabase(con);
            AddHashes(con, path);
            RemoveMissing(con);
        }
    }
}
