using System.Data;
using DBMigrator.Models;
using Microsoft.Data.SqlClient;

namespace DBMigrator.Providers
{
    public class SqlServerProvider : IDbProvider
    {
        public virtual string Name => "SqlServer";

        private static string QuoteIdent(string id) => "[" + id.Replace("]", "]]") + "]";

        public async Task<List<TableSummary>> GetTablesAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);

            var sql = @"
SELECT s.name AS schema_name,
       t.name AS table_name,
       ISNULL(p.rows,0) AS estimated_row_count,
       ISNULL((SELECT SUM(a.total_pages) FROM sys.allocation_units a JOIN sys.partitions p2 ON a.container_id = p2.partition_id WHERE p2.object_id = t.object_id),0) * 8 * 1024 AS size_bytes,
       (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS column_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE t.is_ms_shipped = 0
GROUP BY s.name, t.name, p.rows, t.object_id
ORDER BY size_bytes DESC;";

            await using var cmd = new SqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary
                {
                    Schema = rdr.GetString(0),
                    Name = rdr.GetString(1),
                    EstimatedRowCount = rdr.IsDBNull(2) ? 0 : Convert.ToInt64(rdr[2]),
                    SizeBytes = rdr.IsDBNull(3) ? 0 : Convert.ToInt64(rdr[3]),
                    ColumnCount = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4)
                });
            }
            return list;
        }

        public async Task<List<string>> GetViewsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"SELECT s.name, v.name FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id ORDER BY s.name, v.name;";
            await using var cmd = new SqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetViewDefinitionAsync(string connectionString, string schema, string viewName, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"SELECT OBJECT_DEFINITION(OBJECT_ID(@fullname))";
            await using var cmd = new SqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("fullname", $"{schema}.{viewName}");
            var def = await cmd.ExecuteScalarAsync(ct);
            if (def == null) return $"-- view {schema}.{viewName} not found";
            return def.ToString()!;
        }

        public async Task<List<string>> GetFunctionsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"
SELECT s.name, o.name FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id WHERE o.type IN ('FN','IF','TF') ORDER BY s.name,o.name;";
            await using var cmd = new SqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetFunctionDefinitionAsync(string connectionString, string functionSignatureOrName, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = "SELECT m.definition FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.schema_id = SCHEMA_ID(PARSENAME(@fullname,2)) AND o.name = PARSENAME(@fullname,1) AND o.type IN ('FN','IF','TF')";
            await using var cmd = new SqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("fullname", functionSignatureOrName);
            var def = await cmd.ExecuteScalarAsync(ct);
            return def?.ToString() ?? $"-- function {functionSignatureOrName} not found";
        }

        public async Task<List<string>> GetProceduresAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"SELECT s.name, p.name FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id ORDER BY s.name, p.name;";
            await using var cmd = new SqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct)) list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetProcedureDefinitionAsync(string connectionString, string procSignatureOrName, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = "SELECT m.definition FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.schema_id = SCHEMA_ID(PARSENAME(@fullname,2)) AND o.name = PARSENAME(@fullname,1) AND o.type = 'P'";
            await using var cmd = new SqlCommand(sql, cn);
            cmd.Parameters.AddWithValue("fullname", procSignatureOrName);
            var def = await cmd.ExecuteScalarAsync(ct);
            return def?.ToString() ?? $"-- procedure {procSignatureOrName} not found";
        }

        public async Task AnalyzeAllAsync(string connectionString, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            // update statistics
            await using var cmd = new SqlCommand("EXEC sp_updatestats;", cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public Task AnalyzeTableAsync(string connectionString, string schema, string table, CancellationToken ct = default)
        {
            // SQL Server does not provide per-table ANALYZE like Postgres; use sp_updatestats or UPDATE STATISTICS
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string connectionString, string schema, string table, int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = $"SELECT * FROM {QuoteIdent(schema)}.{QuoteIdent(table)}";
            await using var cmd = new SqlCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            var names = Enumerable.Range(0, rdr.FieldCount).Select(i => rdr.GetName(i)).ToArray();
            while (await rdr.ReadAsync(ct))
            {
                var row = new object?[rdr.FieldCount];
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    row[i] = await rdr.IsDBNullAsync(i, ct) ? null : rdr.GetValue(i);
                }
                yield return (names, row);
            }
        }

        public async Task<int> InsertRowsAsync(string connectionString, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default)
        {
            // Efficient bulk insert using SqlBulkCopy
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            var dt = new DataTable();
            foreach (var c in columns) dt.Columns.Add(c, typeof(object));
            foreach (var r in rows)
                dt.Rows.Add(r.Select(x => x ?? DBNull.Value).ToArray());

            using var bulk = new SqlBulkCopy(cn)
            {
                DestinationTableName = $"{QuoteIdent(schema)}.{QuoteIdent(table)}",
                BatchSize = dt.Rows.Count
            };
            foreach (DataColumn dc in dt.Columns) bulk.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
            await bulk.WriteToServerAsync(dt, ct);
            return dt.Rows.Count;
        }

        public async Task ExecuteNonQueryAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new SqlConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, cn);
            var obj = await cmd.ExecuteScalarAsync(ct);
            return obj?.ToString();
        }
    }

    public class AzureSQLProvider : SqlServerProvider { public override string Name => "AzureSQL"; }
}