using System.Diagnostics;
using System.Threading;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
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

        static void InitializeDatabase(SqliteConnection con)
        {
            using var cmd = new SqliteCommand(
                @"CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, size INT, modified TEXT, hash TEXT)",
                con);
            cmd.ExecuteNonQuery();
        }

        static IDictionary<string, (long Size, string Modified)> ReadDatabase(SqliteConnection con)
        {
            var watch = Stopwatch.StartNew();
            var result = new Dictionary<string, (long, string)>();
            using var cmd = new SqliteCommand(@"SELECT path, size, modified FROM files", con);


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

        static ConcurrentDictionary<string, (long Size, string Modified)> Cache;
        static Task<ProcessResult> ProcessPathAsync(SqliteConnection con, string filePath)
        {
            return Task.Run<ProcessResult>(() =>
            {
                var fileInfoTask = Task.Run(() => new System.IO.FileInfo(filePath));
                if (!fileInfoTask.Wait(30 * 1000))
                {
                    Console.WriteLine($"Stuck Getting File Info: {filePath}");
                }
                var info = fileInfoTask.Result;
                var length = info.Length;
                var modified = info.LastWriteTime.ToString("o");

                void InsertHash(string hash)
                {
                    using var cmd = new SqliteCommand(
                        "INSERT OR REPLACE INTO files(path, size, modified, hash) VALUES(@path, @size, @modified, @hash)", con);
                    cmd.Parameters.AddWithValue("@path", filePath);
                    cmd.Parameters.AddWithValue("@size", length);
                    cmd.Parameters.AddWithValue("@modified", modified);
                    cmd.Parameters.AddWithValue("@hash", (object)hash ?? DBNull.Value);
                    cmd.ExecuteNonQuery();
                }

                // Don't trigger length or date failures unless it's cached
                var sameLength = true;
                var sameDate = true;

                if (Cache.Remove(filePath, out var cachedInfo))
                {
                    sameLength = length == cachedInfo.Size;
                    sameDate = modified == cachedInfo.Modified;
                    if (sameLength && sameDate) return ProcessResult.Cached;
                }

                if (length == 0)
                {
                    InsertHash("");
                    return ProcessResult.ZeroLength;
                }

                InsertHash(GetHash(filePath));

                if (!sameLength) return ProcessResult.RehashedDueToSize;
                if (!sameDate) return ProcessResult.RehashedDueToModifiedDate;
                return ProcessResult.NewlyHashed;
            });
        }

        static void AddHashes(SqliteConnection con, string path)
        {
            var watch = Stopwatch.StartNew();
            var waitTime = 60 * 1000;
            var maxWaitTime = 5 * 60 * 1000;
            var runningItems = new List<(string Path, Stopwatch Stopwatch, Task Task)>();

            Task.Run(() =>
            {
                while (true)
                {
                    var items = runningItems.ToArray();
                    foreach (var item in items)
                    {
                        if (item.Stopwatch.Elapsed.TotalMilliseconds > waitTime)
                        {
                            Console.WriteLine($"Running ({item.Stopwatch.Elapsed}): {item.Path}");
                        }
                    }
                    Thread.Sleep(waitTime);
                }
            });

            long fileCount = 0;
            var hashTypes = new long[] { 0, 0, 0, 0, 0 };
            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                while (true)
                {
                    var active = runningItems
                        .Where(i => i.Stopwatch.Elapsed.TotalMilliseconds < maxWaitTime)
                        .Select(i => i.Task)
                        .Where(t => !t.IsCompleted)
                        .ToArray();
                    if (active.Length <= 5)
                    {
                        break;
                    }
                    if (Task.WaitAny(active, waitTime) == -1)
                    {
                        var workers = active.Where(t => !t.IsCompleted).Count();
                        if (workers > 1)
                        {
                            Console.WriteLine($"Timed out waiting for worker ({runningItems.Count} workers).");
                        }
                    }
                }

                runningItems.Add((filePath, Stopwatch.StartNew(), Task.Run(async () =>
                {
                    var result = await ProcessPathAsync(con, filePath);
                    lock (hashTypes)
                    {
                        hashTypes[(int)result] += 1;
                        fileCount++;
                    }
                })));

                for (var i = runningItems.Count - 1; i >= 0; i--)
                {
                    var item = runningItems[i];
                    if (item.Task.IsCompleted)
                    {
                        // Times estimated, should be within seconds
                        item.Stopwatch.Stop();
                        if (item.Stopwatch.Elapsed.TotalMilliseconds > waitTime)
                        {
                            Console.WriteLine($"Finished ({item.Stopwatch.Elapsed}): {item.Path}");
                        }
                        runningItems.RemoveAt(i);
                    }
                }

                if (watch.Elapsed.TotalSeconds > 10)
                {
                    Console.WriteLine($"Processed: {fileCount}: " + string.Join(" ", hashTypes.Select(i => i.ToString())));
                    watch.Restart();
                }
            }
            Console.WriteLine("Processing Final Files");
            Task.WaitAll(runningItems.Select(r => r.Task).ToArray());
            Console.WriteLine($"Processed: {fileCount}: " + string.Join(" ", hashTypes.Select(i => i.ToString())));
        }

        static void RemovePaths(SqliteConnection con, IReadOnlyList<string> paths)
        {
            var watch = Stopwatch.StartNew();
            Console.WriteLine($"Removing: {paths.Count}");
            long entries = 0;
            foreach (var path in paths)
            {
                var rmCmd = new SqliteCommand("DELETE FROM files WHERE path = @path", con);
                rmCmd.Parameters.AddWithValue("@path", path);
                rmCmd.ExecuteNonQuery();

                entries++;
                if (watch.Elapsed.TotalSeconds > 10)
                {
                    Console.WriteLine($"Removed: {entries} / {paths.Count}");
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

            using var con = new SqliteConnection($@"URI=file:{path}");
            con.Open();

            using var cmd = new SqliteCommand(@"SELECT * FROM files", con);

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

            // Don't log out version if we're trying to make parsable json
            Console.WriteLine("HashFS v0.3.9");

            if (args.Length >= 1) path = args[0];
            if (args.Length >= 2) database = args[1];

            var cs = $@"Data Source=file:{database}";

            using var con = new SqliteConnection(cs);
            Console.WriteLine($"System SQLite version: {con.ServerVersion}");
            con.Open();
            InitializeDatabase(con);
            Cache = new ConcurrentDictionary<string, (long, string)>(ReadDatabase(con));
            AddHashes(con, path);
            RemovePaths(con, Cache.Keys.ToArray());
        }
    }
}
