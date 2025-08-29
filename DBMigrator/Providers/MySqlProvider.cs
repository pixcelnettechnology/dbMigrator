using System.Data;
using DBMigrator.Models;
using MySqlConnector;

namespace DBMigrator.Providers
{
    public class MySqlProvider : IDbProvider
    {
        public virtual string Name => "MySql";
        protected static string Q(string id) => $"`{id.Replace("`", "``")}`";

        public async Task<List<TableSummary>> GetTablesAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);

            // Current schema
            string dbName;
            await using (var cmdDb = new MySqlCommand("SELECT DATABASE()", cn))
                dbName = (await cmdDb.ExecuteScalarAsync(ct))?.ToString() ?? "";

            // metadata (size from information_schema; rows from TABLES.TABLE_ROWS estimate; columns count from COLUMNS)
            var sql = @"
SELECT t.TABLE_SCHEMA, t.TABLE_NAME, 
       IFNULL(t.TABLE_ROWS,0) AS est_rows,
       IFNULL(SUM(s.DATA_LENGTH + s.INDEX_LENGTH),0) AS size_bytes,
       (SELECT COUNT(*) FROM information_schema.COLUMNS c 
         WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME) AS col_count
FROM information_schema.TABLES t
LEFT JOIN information_schema.TABLES s 
  ON s.TABLE_SCHEMA=t.TABLE_SCHEMA AND s.TABLE_NAME=t.TABLE_NAME
WHERE t.TABLE_TYPE='BASE TABLE' AND t.TABLE_SCHEMA=@db
GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_ROWS
ORDER BY size_bytes DESC;";
            await using var cmd = new MySqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("@db", dbName);
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

        public async Task<List<string>> GetViewsAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            string db;
            await using (var cmdDb = new MySqlCommand("SELECT DATABASE()", cn))
                db = (await cmdDb.ExecuteScalarAsync(ct))?.ToString() ?? "";
            var sql = @"SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA=@db ORDER BY 1,2";
            await using var cmd = new MySqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("@db", db);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetViewDefinitionAsync(string cs, string schema, string viewName, CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            var sql = @"SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA=@s AND TABLE_NAME=@v";
            await using var cmd = new MySqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("@s", schema);
            cmd.Parameters.AddWithValue("@v", viewName);
            var def = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return def == null ? $"-- view {schema}.{viewName} not found" : $"CREATE OR REPLACE VIEW {Q(schema)}.{Q(viewName)} AS\n{def};";
        }

        public async Task<List<string>> GetFunctionsAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            string db;
            await using (var cmdDb = new MySqlCommand("SELECT DATABASE()", cn))
                db = (await cmdDb.ExecuteScalarAsync(ct))?.ToString() ?? "";
            var sql = @"SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='FUNCTION' AND ROUTINE_SCHEMA=@db ORDER BY 1,2";
            await using var cmd = new MySqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("@db", db);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}()");
            return list;
        }

        public async Task<string> GetFunctionDefinitionAsync(string cs, string functionSignatureOrName, CancellationToken ct = default)
        {
            // MySQL does not have a single-function DDL getter; use SHOW CREATE FUNCTION
            var (schema, name) = Split(functionSignatureOrName);
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            await using var use = new MySqlCommand($"USE {Q(schema)};", cn);
            await use.ExecuteNonQueryAsync(ct);
            await using var cmd = new MySqlCommand($"SHOW CREATE FUNCTION {Q(name)};", cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            if (await rdr.ReadAsync(ct))
            {
                var ddl = rdr.GetString("Create Function");
                return ddl + ";";
            }
            return $"-- function {schema}.{name} not found";
        }

        public async Task<List<string>> GetProceduresAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            string db;
            await using (var cmdDb = new MySqlCommand("SELECT DATABASE()", cn))
                db = (await cmdDb.ExecuteScalarAsync(ct))?.ToString() ?? "";
            var sql = @"SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA=@db ORDER BY 1,2";
            await using var cmd = new MySqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("@db", db);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}()");
            return list;
        }

        public async Task<string> GetProcedureDefinitionAsync(string cs, string procSignatureOrName, CancellationToken ct = default)
        {
            var (schema, name) = Split(procSignatureOrName);
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            await using var use = new MySqlCommand($"USE {Q(schema)};", cn);
            await use.ExecuteNonQueryAsync(ct);
            await using var cmd = new MySqlCommand($"SHOW CREATE PROCEDURE {Q(name)};", cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            if (await rdr.ReadAsync(ct))
            {
                var ddl = rdr.GetString("Create Procedure");
                return ddl + ";";
            }
            return $"-- procedure {schema}.{name} not found";
        }

        // MySQL has ANALYZE TABLE; it updates stats
        public async Task AnalyzeAllAsync(string cs, CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            string db;
            await using (var cmdDb = new MySqlCommand("SELECT DATABASE()", cn))
                db = (await cmdDb.ExecuteScalarAsync(ct))?.ToString() ?? "";
            // run ANALYZE on all tables
            var tables = new List<(string Schema, string Name)>();
            await using (var cmd = new MySqlCommand("SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA=@db", cn))
            {
                cmd.Parameters.AddWithValue("@db", db);
                await using var rdr = await cmd.ExecuteReaderAsync(ct);
                while (await rdr.ReadAsync(ct)) tables.Add((rdr.GetString(0), rdr.GetString(1)));
            }
            foreach (var t in tables)
                await new MySqlCommand($"ANALYZE TABLE {Q(t.Schema)}.{Q(t.Name)}", cn).ExecuteNonQueryAsync(ct);
        }

        public async Task AnalyzeTableAsync(string cs, string schema, string table, CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            await new MySqlCommand($"ANALYZE TABLE {Q(schema)}.{Q(table)}", cn).ExecuteNonQueryAsync(ct);
        }

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string cs, string schema, string table, int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            var cmd = new MySqlCommand($"SELECT * FROM {Q(schema)}.{Q(table)}", cn);
            await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
            var cols = Enumerable.Range(0, rdr.FieldCount).Select(i => rdr.GetName(i)).ToArray();
            while (await rdr.ReadAsync(ct))
            {
                var row = new object?[cols.Length];
                for (int i = 0; i < cols.Length; i++) row[i] = await rdr.IsDBNullAsync(i, ct) ? null : rdr.GetValue(i);
                yield return (cols, row);
            }
        }

        public async Task<int> InsertRowsAsync(string cs, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default)
        {
            int total = 0;
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);

            // Efficient multi-row insert batches
            const int MaxRowsPerBatch = 1000;
            var colsList = string.Join(",", columns.Select(Q));
            var rowsEnum = rows.ToList();

            for (int offset = 0; offset < rowsEnum.Count; offset += MaxRowsPerBatch)
            {
                var chunk = rowsEnum.Skip(offset).Take(MaxRowsPerBatch).ToList();
                if (chunk.Count == 0) break;

                var valuesParts = new List<string>();
                var parameters = new List<MySqlParameter>();
                int pIndex = 0;
                foreach (var r in chunk)
                {
                    var ps = new List<string>();
                    for (int i = 0; i < columns.Length; i++)
                    {
                        var p = new MySqlParameter($"@p{pIndex++}", r[i] ?? DBNull.Value);
                        parameters.Add(p);
                        ps.Add(p.ParameterName);
                    }
                    valuesParts.Add("(" + string.Join(",", ps) + ")");
                }

                var sql = $"INSERT INTO {Q(schema)}.{Q(table)} ({colsList}) VALUES {string.Join(",", valuesParts)};";
                await using var cmd = new MySqlCommand(sql, cn);
                cmd.Parameters.AddRange(parameters.ToArray());
                total += await cmd.ExecuteNonQueryAsync(ct);
            }
            return total;
        }

        public async Task ExecuteNonQueryAsync(string cs, string sql, CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            await using var cmd = new MySqlCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string cs, string sql, CancellationToken ct = default)
        {
            await using var cn = new MySqlConnection(cs);
            await cn.OpenAsync(ct);
            await using var cmd = new MySqlCommand(sql, cn);
            var obj = await cmd.ExecuteScalarAsync(ct);
            return obj?.ToString();
        }

        private static (string Schema, string Name) Split(string input)
        {
            var parts = input.Split('.', 2);
            return parts.Length == 2 ? (parts[0], parts[1]) : ("", parts[0]);
        }
    }

    // These are protocol-compatible with MySQL:
    public class MariaDbProvider : MySqlProvider { public override string Name => "MariaDB"; }
    public class PerconaProvider : MySqlProvider { public override string Name => "Percona"; }
    public class TiDbProvider : MySqlProvider { public override string Name => "TiDB"; }
    public class AmazonAuroraMySqlProvider : MySqlProvider { public override string Name => "AmazonAuroraMySql"; }
}