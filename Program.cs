using System;
using System.Data.SQLite;
using System.IO;
using System.Security.Cryptography;

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

        static void Main(string[] args)
        {
            var database = @".\hashes.db";
            var path = ".";
            if (args.Length == 1)
            {
                path = args[0];
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

            foreach (var filePath in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                var info = new System.IO.FileInfo(filePath);
                var length = info.Length;
                var modified = info.LastWriteTime.ToString("o");

                cmd.CommandText = @"SELECT * FROM files WHERE path=@path";
                cmd.Parameters.AddWithValue("@path", filePath);

                using var reader = cmd.ExecuteReader();
                reader.Read();

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
                    System.Console.WriteLine($"{hash}, {modified}, {length}, {filePath}");
                }
            };
        }
    }
}
