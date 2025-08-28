using System.Data.Odbc;
using DBMigrator.Models;

namespace DBMigrator.Providers
{
    public class Db2OdbcProvider : IDbProvider
    {
        public string Name => "Db2";

        private static string Q(string id) => "\"" + id.Replace("\"","\"\"") + "\"";

        public async Task<List<TableSummary>> GetTablesAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);

            // CARD (row estimate) & COLCOUNT available in SYSCAT.TABLES
            var sql = @"
SELECT TABSCHEMA, TABNAME, COALESCE(CARD,0), COALESCE(COLCOUNT,0)
FROM SYSCAT.TABLES
WHERE TYPE='T' AND TABSCHEMA NOT IN ('SYSIBM','SYSCAT','SYSSTAT','SYSFUN','SYSPROC','SYSTOOLS')
ORDER BY TABSCHEMA, TABNAME";
            using var cmd = new OdbcCommand(sql, cn);
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary
                {
                    Schema = rdr.GetString(0),
                    Name = rdr.GetString(1),
                    EstimatedRowCount = Convert.ToInt64(rdr.GetValue(2)),
                    SizeBytes = 0, // computing size is involved; leave 0 or add later
                    ColumnCount = Convert.ToInt32(rdr.GetValue(3))
                });
            }
            return list;
        }

        public Task<List<string>> GetViewsAsync(string cs, CancellationToken ct = default)
            => GetNames(cs, ct, "SELECT VIEWSCHEMA, VIEWNAME FROM SYSCAT.VIEWS ORDER BY 1,2");

        public async Task<string> GetViewDefinitionAsync(string cs, string schema, string viewName, CancellationToken ct = default)
        {
            using var cn = new OdbcConnection(cs); await cn.OpenAsync(ct);
            var sql = "SELECT TEXT FROM SYSCAT.VIEWS WHERE VIEWSCHEMA=? AND VIEWNAME=?";
            using var cmd = new OdbcCommand(sql, cn);
            cmd.Parameters.AddWithValue("@p1", schema);
            cmd.Parameters.AddWithValue("@p2", viewName);
            var txt = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return txt == null ? $"-- VIEW {schema}.{viewName} not found" : $"CREATE VIEW {Q(schema)}.{Q(viewName)} AS\n{txt}";
        }

        public Task<List<string>> GetFunctionsAsync(string cs, CancellationToken ct = default)
            => GetNames(cs, ct, "SELECT ROUTINESCHEMA, ROUTINENAME FROM SYSCAT.ROUTINES WHERE ROUTINETYPE='F' ORDER BY 1,2");

        public async Task<string> GetFunctionDefinitionAsync(string cs, string functionSignatureOrName, CancellationToken ct = default)
        {
            var (s, n) = Split(functionSignatureOrName);
            using var cn = new OdbcConnection(cs); await cn.OpenAsync(ct);
            var sql = "SELECT TEXT FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA=? AND ROUTINENAME=? AND ROUTINETYPE='F'";
            using var cmd = new OdbcCommand(sql, cn);
            cmd.Parameters.AddWithValue("@p1", s);
            cmd.Parameters.AddWithValue("@p2", n);
            var txt = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return txt ?? $"-- FUNCTION {s}.{n} not found";
        }

        public Task<List<string>> GetProceduresAsync(string cs, CancellationToken ct = default)
            => GetNames(cs, ct, "SELECT ROUTINESCHEMA, ROUTINENAME FROM SYSCAT.ROUTINES WHERE ROUTINETYPE='P' ORDER BY 1,2");

        public async Task<string> GetProcedureDefinitionAsync(string cs, string procSignatureOrName, CancellationToken ct = default)
        {
            var (s, n) = Split(procSignatureOrName);
            using var cn = new OdbcConnection(cs); await cn.OpenAsync(ct);
            var sql = "SELECT TEXT FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA=? AND ROUTINENAME=? AND ROUTINETYPE='P'";
            using var cmd = new OdbcCommand(sql, cn);
            cmd.Parameters.AddWithValue("@p1", s);
            cmd.Parameters.AddWithValue("@p2", n);
            var txt = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return txt ?? $"-- PROCEDURE {s}.{n} not found";
        }

        public Task AnalyzeAllAsync(string cs, CancellationToken ct = default) => Task.CompletedTask;
        public Task AnalyzeTableAsync(string cs, string schema, string table, CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(
            string cs, string schema, string table, int batchSize,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);
            using var cmd = new OdbcCommand($"SELECT * FROM {Q(schema)}.{Q(table)}", cn);
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            var cols = Enumerable.Range(0, rdr.FieldCount).Select(i => rdr.GetName(i)).ToArray();
            while (await rdr.ReadAsync(ct))
            {
                var row = new object?[cols.Length];
                for (int i = 0; i < cols.Length; i++) row[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
                yield return (cols, row);
            }
        }

        public async Task<int> InsertRowsAsync(string cs, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default)
        {
            int total = 0;
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);
            foreach (var r in rows)
            {
                var colList = string.Join(",", columns.Select(Q));
                var qMarks = string.Join(",", Enumerable.Range(0, columns.Length).Select(_ => "?"));
                using var cmd = new OdbcCommand($"INSERT INTO {Q(schema)}.{Q(table)} ({colList}) VALUES ({qMarks})", cn);
                for (int i = 0; i < columns.Length; i++) cmd.Parameters.AddWithValue($"@p{i}", r[i] ?? DBNull.Value);
                total += await cmd.ExecuteNonQueryAsync(ct);
            }
            return total;
        }

        public async Task ExecuteNonQueryAsync(string cs, string sql, CancellationToken ct = default)
        {
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);
            using var cmd = new OdbcCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string cs, string sql, CancellationToken ct = default)
        {
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);
            using var cmd = new OdbcCommand(sql, cn);
            var o = await cmd.ExecuteScalarAsync(ct);
            return o?.ToString();
        }

        private static async Task<List<string>> GetNames(string cs, CancellationToken ct, string sql)
        {
            var list = new List<string>();
            using var cn = new OdbcConnection(cs); await cn.OpenAsync(ct);
            using var cmd = new OdbcCommand(sql, cn);
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        private static (string Schema,string Name) Split(string input)
        {
            var parts = input.Split('.', 2);
            return parts.Length == 2 ? (parts[0], parts[1]) : ("", parts[0]);
        }
    }
}