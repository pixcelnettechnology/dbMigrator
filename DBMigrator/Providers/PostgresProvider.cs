using DBMigrator.Models;
using Npgsql;

namespace DBMigrator.Providers
{
    public class PostgresProvider : IDbProvider
    {
        public virtual string Name => "Postgres";

        private static string QuoteIdent(string id) => "\"" + id.Replace("\"", "\"\"") + "\"";

        public virtual async Task<List<TableSummary>> GetTablesAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);

            var sql = @"
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       COALESCE(s.n_live_tup,0)::bigint AS estimated_row_count,
       pg_total_relation_size(c.oid) AS size_bytes,
       COUNT(a.attname) AS column_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
WHERE c.relkind = 'r'
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
GROUP BY n.nspname, c.relname, s.n_live_tup, c.oid
ORDER BY pg_total_relation_size(c.oid) DESC;";

            await using var cmd = new NpgsqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary
                {
                    Schema = rdr.GetString(0),
                    Name = rdr.GetString(1),
                    EstimatedRowCount = rdr.IsDBNull(2) ? 0 : rdr.GetInt64(2),
                    SizeBytes = rdr.IsDBNull(3) ? 0 : rdr.GetInt64(3),
                    ColumnCount = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4)
                });
            }
            return list;
        }

        public async Task<List<string>> GetViewsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"SELECT table_schema, table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog','information_schema','pg_toast') ORDER BY table_schema,table_name;";
            await using var cmd = new NpgsqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetViewDefinitionAsync(string connectionString, string schema, string viewName, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"SELECT pg_get_viewdef(c.oid, true) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind='v' AND n.nspname=@s AND c.relname=@v LIMIT 1;";
            await using var cmd = new NpgsqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("s", schema);
            cmd.Parameters.AddWithValue("v", viewName);
            var def = await cmd.ExecuteScalarAsync(ct);
            if (def == null) return $"-- view {schema}.{viewName} not found";
            return $"CREATE OR REPLACE VIEW {QuoteIdent(schema)}.{QuoteIdent(viewName)} AS\n{def};";
        }

        public async Task<List<string>> GetFunctionsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) as args
FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND p.prokind IN ('f','p')
ORDER BY n.nspname, p.proname;";
            await using var cmd = new NpgsqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}({rdr.GetString(2)})");
            return list;
        }

        public async Task<string> GetFunctionDefinitionAsync(string connectionString, string functionSignatureOrName, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);

            // parse schema.function(args) or schema.function
            var schema = "public";
            var fn = functionSignatureOrName;
            string? args = null;
            if (functionSignatureOrName.Contains('.'))
            {
                var idx = functionSignatureOrName.IndexOf('.');
                schema = functionSignatureOrName.Substring(0, idx);
                fn = functionSignatureOrName.Substring(idx + 1);
            }
            if (fn.Contains('('))
            {
                var idx = fn.IndexOf('(');
                args = fn.Substring(idx + 1, fn.Length - idx - 2);
                fn = fn.Substring(0, idx);
            }

            var sql = args == null
                ? "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname=@s AND p.proname=@f LIMIT 1;"
                : "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname=@s AND p.proname=@f AND pg_get_function_identity_arguments(p.oid)=@args LIMIT 1;";

            await using var cmd = new NpgsqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("s", schema);
            cmd.Parameters.AddWithValue("f", fn);
            if (args != null) cmd.Parameters.AddWithValue("args", args);
            var def = await cmd.ExecuteScalarAsync(ct);
            return def?.ToString() ?? $"-- function {functionSignatureOrName} not found";
        }

        public async Task<List<string>> GetProceduresAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) as args
FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.prokind = 'p' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
ORDER BY n.nspname, p.proname;";
            await using var cmd = new NpgsqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}({rdr.GetString(2)})");
            return list;
        }

        public async Task<string> GetProcedureDefinitionAsync(string connectionString, string procSignatureOrName, CancellationToken ct = default)
        {
            // reuse function getter but filtered for prokind='p'
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);

            var schema = "public";
            var proc = procSignatureOrName;
            string? args = null;
            if (procSignatureOrName.Contains('.'))
            {
                var idx = procSignatureOrName.IndexOf('.');
                schema = procSignatureOrName.Substring(0, idx);
                proc = procSignatureOrName.Substring(idx + 1);
            }
            if (proc.Contains('('))
            {
                var idx = proc.IndexOf('(');
                args = proc.Substring(idx + 1, proc.Length - idx - 2);
                proc = proc.Substring(0, idx);
            }

            var sql = args == null
                ? "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname=@s AND p.proname=@f AND p.prokind='p' LIMIT 1;"
                : "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname=@s AND p.proname=@f AND pg_get_function_identity_arguments(p.oid)=@args AND p.prokind='p' LIMIT 1;";

            await using var cmd = new NpgsqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("s", schema);
            cmd.Parameters.AddWithValue("f", proc);
            if (args != null) cmd.Parameters.AddWithValue("args", args);
            var def = await cmd.ExecuteScalarAsync(ct);
            return def?.ToString() ?? $"-- procedure {procSignatureOrName} not found";
        }

        public async Task AnalyzeAllAsync(string connectionString, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            // try VACUUM ANALYZE (may require privileges)
            await using var cmd = new NpgsqlCommand("VACUUM ANALYZE;", cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task AnalyzeTableAsync(string connectionString, string schema, string table, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = $"ANALYZE {QuoteIdent(schema)}.{QuoteIdent(table)};";
            await using var cmd = new NpgsqlCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string connectionString, string schema, string table, int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            // Build a SELECT which casts internal problematic types to text
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);

            // get columns & types
            var colCmd = cn.CreateCommand();
            colCmd.CommandText = @"
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema=@s AND table_name=@t
ORDER BY ordinal_position";
            colCmd.Parameters.AddWithValue("s", schema);
            colCmd.Parameters.AddWithValue("t", table);

            var cols = new List<string>();
            var colNames = new List<string>();
            await using (var rdr = await colCmd.ExecuteReaderAsync(ct))
            {
                while (await rdr.ReadAsync(ct))
                {
                    var col = rdr.GetString(0);
                    var dt = rdr.GetString(1);
                    colNames.Add(col);

                    // cast problematic types to text
                    if (dt is "aclitem" or "pg_node_tree" or "regproc" or "regprocedure")
                        cols.Add($"{QuoteIdent(col)}::text AS {QuoteIdent(col)}");
                    else
                        cols.Add($"{QuoteIdent(col)}");
                }
            }

            var select = $"SELECT {string.Join(", ", cols)} FROM {QuoteIdent(schema)}.{QuoteIdent(table)}";
            await using var cmd = new NpgsqlCommand(select, cn);
            await using var rdr2 = await cmd.ExecuteReaderAsync(ct);

            var names = Enumerable.Range(0, rdr2.FieldCount).Select(i => rdr2.GetName(i)).ToArray();
            while (await rdr2.ReadAsync(ct))
            {
                var row = new object?[rdr2.FieldCount];
                for (int i = 0; i < rdr2.FieldCount; i++)
                {
                    if (await rdr2.IsDBNullAsync(i, ct)) { row[i] = null; continue; }
                    var v = rdr2.GetValue(i);
                    if (v is DateTime dt)
                    {
                        if (dt.Year < 1 || dt.Year > 9999) row[i] = null;
                        else row[i] = dt;
                    }
                    else row[i] = v;
                }
                yield return (names, row);
            }
        }

        public async Task<int> InsertRowsAsync(string connectionString, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default)
        {
            // Use COPY (binary) for efficient insert
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);

            var columnList = string.Join(", ", columns.Select(c => QuoteIdent(c)));
            var copySql = $"COPY {QuoteIdent(schema)}.{QuoteIdent(table)} ({columnList}) FROM STDIN (FORMAT BINARY)";

            await using var writer = cn.BeginBinaryImport(copySql);
            var count = 0;
            foreach (var r in rows)
            {
                await writer.StartRowAsync(ct);
                for (int i = 0; i < columns.Length; i++)
                {
                    var val = r[i] ?? DBNull.Value;
                    await writer.WriteAsync(val, string.Empty, ct);
                }
                count++;
            }
            await writer.CompleteAsync(ct);
            return count;
        }

        public async Task ExecuteNonQueryAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new NpgsqlConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, cn);
            var obj = await cmd.ExecuteScalarAsync(ct);
            return obj?.ToString();
        }
    }
    public class AmazonAuroraPostgresProvider : PostgresProvider { public override string Name => "AmazonAuroraPostgres"; }
    public class HyperscalePostgresProvider : PostgresProvider { public override string Name => "Hyperscale"; }
}