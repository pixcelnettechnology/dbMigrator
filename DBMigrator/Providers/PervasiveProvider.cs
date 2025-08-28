using System.Data.Odbc;
using DBMigrator.Models;

namespace DBMigrator.Providers
{
    public class PervasiveOdbcProvider : IDbProvider
    {
        public string Name => "Pervasive";

        public async Task<List<TableSummary>> GetTablesAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            using var cn = new OdbcConnection(cs); // DSN or full ODBC conn string
            await cn.OpenAsync(ct);

            // Try generic INFORMATION_SCHEMA; if unavailable, you’ll need product-specific catalogs.
            var cmd = new OdbcCommand("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", cn);
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary { Schema = rdr.GetString(0), Name = rdr.GetString(1), EstimatedRowCount = 0, SizeBytes = 0, ColumnCount = 0 });
            }
            return list;
        }

        public Task<List<string>> GetViewsAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetViewDefinitionAsync(string cs, string schema, string viewName, CancellationToken ct = default) => Task.FromResult("-- not implemented");
        public Task<List<string>> GetFunctionsAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetFunctionDefinitionAsync(string cs, string functionSignatureOrName, CancellationToken ct = default) => Task.FromResult("-- not implemented");
        public Task<List<string>> GetProceduresAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetProcedureDefinitionAsync(string cs, string procSignatureOrName, CancellationToken ct = default) => Task.FromResult("-- not implemented");

        public Task AnalyzeAllAsync(string cs, CancellationToken ct = default) => Task.CompletedTask;
        public Task AnalyzeTableAsync(string cs, string schema, string table, CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string cs, string schema, string table, int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            using var cn = new OdbcConnection(cs);
            await cn.OpenAsync(ct);
            using var cmd = new OdbcCommand($"SELECT * FROM [{schema}].[{table}]", cn);
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
                var colList = string.Join(",", columns.Select(c => $"[{c}]"));
                var paramList = string.Join(",", Enumerable.Range(0, columns.Length).Select(i => $"?"));
                using var cmd = new OdbcCommand($"INSERT INTO [{schema}].[{table}] ({colList}) VALUES ({paramList})", cn);
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
    }
}