using System.Diagnostics;
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

        enum ProcessResult
        {
            Cached,
            NewlyHashed,
            RehashedDueToSize,
            RehashedDueToModifiedDate,
            ZeroLength,
        }

        static void InitializeDatabase(SQLiteConnection con)
        {
            using var cmd = new SQLiteCommand(con);
            cmd.CommandText = @"CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, size INT, modified TEXT, hash TEXT)";
            cmd.ExecuteNonQuery();
        }

        static IDictionary<string, (long Size, string Modified)> ReadDatabase(SQLiteConnection con)
        {
            var watch = Stopwatch.StartNew();
            var result = new Dictionary<string, (long, string)>();
            using var cmd = new SQLiteCommand(@"SELECT path, size, modified FROM files", con);


            using var reader = cmd.ExecuteReader();
            long entries = 0;
            while (reader.Read())
            {
                result[(string)reader["path"]] = ((long)reader.GetInt64(1), (string)reader["modified"]);
                entries++;
                if (watch.Elapsed.TotalSeconds > 10)
                {
                    Console.WriteLine($"Read: {Math.Floor(entries / 1000d) * 1000}");
                    watch.Restart();
                }
            }
            Console.WriteLine($"Read: {result.Count}");
            return result;
        }

        static Task<ProcessResult> ProcessPathAsync(SQLiteConnection con, string filePath, IDictionary<string, (long Size, string Modified)> cache)
        {
            return Task.Run<ProcessResult>(() =>
            {
                var info = new System.IO.FileInfo(filePath);
                var length = info.Length;
                var modified = info.LastWriteTime.ToString("o");

                void InsertHash(string hash) {
                    using var cmd = new SQLiteCommand(
                        "INSERT OR REPLACE INTO files(path, size, modified, hash) VALUES(@path, @size, @modified, @hash)", con);
                    cmd.Parameters.AddWithValue("@path", filePath);
                    cmd.Parameters.AddWithValue("@size", length);
                    cmd.Parameters.AddWithValue("@modified", modified);
                    cmd.Parameters.AddWithValue("@hash", hash);
                    cmd.ExecuteNonQuery();
                }

                if (length == 0) {
                    InsertHash("");
                    return ProcessResult.ZeroLength;
                }

                var sameLength = false;
                var sameDate = false;

                if (cache.ContainsKey(filePath))
                {
                    var cachedInfo = cache[filePath];
                    // TODO: Make this impure magic clearer
                    cache.Remove(filePath);
                    sameLength = length == cachedInfo.Size;
                    sameDate = modified == cachedInfo.Modified;
                    if (sameLength && sameDate) return ProcessResult.Cached;
                    Console.WriteLine($"Cache Miss {filePath} ({sameLength}, {sameDate}) {cachedInfo.Modified}  {cachedInfo.Size}");
                }

                InsertHash(GetHash(filePath));

                if (!sameLength) return ProcessResult.RehashedDueToSize;
                if (!sameDate) return ProcessResult.RehashedDueToModifiedDate;
                return ProcessResult.NewlyHashed;
            });
        }

        static void AddHashes(SQLiteConnection con, string path, IDictionary<string, (long Size, string Modified)> cache)
        {
            var watch = Stopwatch.StartNew();
            var waitTime = 60 * 1000;
            var runningItems = new List<(string Path, Task<ProcessResult> Task)>();
            string hungMessage = null;

            Task.Run(() =>
            {
                while (true)
                {
                    if (hungMessage != null)
                    {
                        Console.WriteLine(runningItems.Count());
                        Console.Write(hungMessage);
                    }
                    Thread.Sleep(waitTime);
                }
            });

            long fileCount = 0;
            var hashTypes = new long[] { 0, 0, 0, 0, 0 };
            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                runningItems.Add((filePath, ProcessPathAsync(con, filePath, cache)));
                if (runningItems.Count > 3)
                {
                    Task.WaitAny(runningItems.Select(r => r.Task).ToArray(), waitTime);
                }

                for (var i = runningItems.Count - 1; i >= 0; i--)
                {
                    var item = runningItems[i];
                    if (item.Task.IsCompleted)
                    {
                        hashTypes[(int)item.Task.Result] += 1;
                        runningItems.RemoveAt(i);
                    }
                }

                hungMessage = string.Join("", runningItems.Select(i => $"Running Item: {i.Path}\n"));

                fileCount++;
                if (watch.Elapsed.TotalSeconds > 10)
                {
                    Console.WriteLine($"Processed: {fileCount}: " + string.Join(" ", hashTypes.Select(i => i.ToString())));
                    watch.Restart();
                }
            }
            Console.WriteLine("Processing Final Files");
            Task.WaitAll(runningItems.Select(r => r.Task).ToArray());
            // TODO: This number may be off a little
            Console.WriteLine($"Processed(ish): {fileCount}");
        }

        static void RemovePaths(SQLiteConnection con, IReadOnlyList<string> paths)
        {
            var watch = Stopwatch.StartNew();
            Console.WriteLine($"Removing: {paths.Count}");
            long entries = 0;
            foreach (var path in paths)
            {
                var rmCmd = new SQLiteCommand("DELETE FROM files WHERE path = @path", con);
                rmCmd.Parameters.AddWithValue("@path", path);
                rmCmd.ExecuteNonQuery();

                entries++;
                if (watch.Elapsed.TotalSeconds > 10)
                {
                    Console.WriteLine($"Removed: {Math.Floor(entries / 100d) * 100} / {paths.Count}");
                    watch.Restart();
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
            Console.WriteLine("HashFS v0.3.2");

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
            var cache = ReadDatabase(con);
            // Impure shenanigans, need to move this to a class
            AddHashes(con, path, cache);
            RemovePaths(con, cache.Keys.ToArray());
        }
    }
}
