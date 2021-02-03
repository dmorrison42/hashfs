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

        enum ProcessResult
        {
            Cached,
            NewlyHashed,
            RehashedDueToSize,
            RehashedDueToModifiedDate,
        }

        static Task<ProcessResult> ProcessPathAsync(SQLiteConnection con, string filePath)
        {
            return Task.Run<ProcessResult>(() =>
            {
                var info = new System.IO.FileInfo(filePath);
                var length = info.Length;
                var modified = info.LastWriteTime.ToString("o");

                using var readCommand = new SQLiteCommand(@"SELECT * FROM files WHERE path=@path", con);
                readCommand.Parameters.AddWithValue("@path", filePath);
                using var reader = readCommand.ExecuteReader();
                reader.Read();

                var sameLength = false;
                var sameDate = false;
                if (reader.HasRows)
                {
                    sameLength = length == (int)reader["size"];
                    sameDate = modified == (string)reader["modified"];
                }

                if (reader.HasRows && sameLength && sameDate) return ProcessResult.Cached;

                var hash = GetHash(filePath);
                using var cmd = new SQLiteCommand(
                    "INSERT OR REPLACE INTO files(path, size, modified, hash) VALUES(@path, @size, @modified, @hash)", con);
                cmd.Parameters.AddWithValue("@path", filePath);
                cmd.Parameters.AddWithValue("@size", length);
                cmd.Parameters.AddWithValue("@modified", modified);
                cmd.Parameters.AddWithValue("@hash", hash);
                cmd.ExecuteNonQuery();

                if (!sameLength) return ProcessResult.RehashedDueToSize;
                if (!sameDate) return ProcessResult.RehashedDueToModifiedDate;
                return ProcessResult.NewlyHashed;
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
            var hashTypes = new long[] { 0, 0, 0, 0 };
            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                var task = ProcessPathAsync(con, filePath);
                runningItems.Add((filePath, task));
                if (runningItems.Count > 3)
                {
                    Task.WaitAny(runningItems.Select(r => r.Task).ToArray(), waitTime);
                }
                runningItems = runningItems.Where(i =>
                {
                    if (!task.IsCompleted) return false;
                    hashTypes[(int)task.Result] += 1;
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
                    Console.WriteLine($"{fileCount}:{string.Join(",", hashTypes)}");
                }
            }
            Console.WriteLine("Waiting at the end");
            Task.WaitAll(runningItems.Select(r => r.Task).ToArray());
        }

        static void RemoveMissing(SQLiteConnection con)
        {
            Task<bool> CleanPathAsync(string path)
            {
                return Task.Run(() =>
                {
                    if (File.Exists(path)) return false;

                    var rmCmd = new SQLiteCommand("DELETE FROM files WHERE path = @path", con);
                    rmCmd.Parameters.AddWithValue("@path", path);
                    rmCmd.ExecuteNonQuery();
                    return true;
                });
            }

            var waitTime = 60 * 1000;
            var runningItems = new List<(string Path, Task Task)>();
            using var cmd = new SQLiteCommand(con);

            cmd.CommandText = @"SELECT path FROM files";

            using var reader = cmd.ExecuteReader();
            long entries = 0;
            while (reader.Read())
            {
                var filePath = reader.GetString(0);
                runningItems.Add((filePath, CleanPathAsync(filePath)));
                if (runningItems.Count > 3)
                {
                    Task.WaitAny(runningItems.Select(r => r.Task).ToArray(), waitTime);
                }

                for (var i = runningItems.Count - 1; i >= 0; i--)
                {
                    var item = runningItems[i];
                    if (item.Task.IsCompleted)
                    {
                        runningItems.RemoveAt(i);
                    }
                }

                if (++entries % 100 == 0)
                {
                    Console.WriteLine(entries);
                }
            }
            Task.WaitAll(runningItems.Select(r => r.Task).ToArray());
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
            Console.WriteLine("HashFS v0.2");

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
            Console.WriteLine("Hasing completed! Removing non-existing files from the database");
            RemoveMissing(con);
        }
    }
}
