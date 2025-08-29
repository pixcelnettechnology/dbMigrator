using DBMigrator.Models;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;

namespace DBMigrator.Providers
{
    public class GoogleCloudSpannerProvider : IDbProvider
    {
        public string Name => "GoogleCloudSpanner";

        public async Task<List<TableSummary>> GetTablesAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            using var conn = new SpannerConnection(cs);
            await conn.OpenAsync(ct);

            // Spanner INFORMATION_SCHEMA
            var cmd = conn.CreateSelectCommand(@"
SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
       (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME) AS col_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_TYPE='BASE TABLE'");
            using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary
                {
                    Schema = rdr.GetFieldValue<string>(0),
                    Name = rdr.GetFieldValue<string>(1),
                    EstimatedRowCount = 0, // no cheap estimate; use COUNT(*) if you must (expensive)
                    SizeBytes = 0,
                    ColumnCount = Convert.ToInt32(rdr.GetFieldValue<long>(2))
                });
            }
            return list;
        }

        public Task<List<string>> GetViewsAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetViewDefinitionAsync(string cs, string schema, string viewName, CancellationToken ct = default) => Task.FromResult("-- views not supported");
        public Task<List<string>> GetFunctionsAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetFunctionDefinitionAsync(string cs, string functionSignatureOrName, CancellationToken ct = default) => Task.FromResult("-- functions not supported");
        public Task<List<string>> GetProceduresAsync(string cs, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<string> GetProcedureDefinitionAsync(string cs, string procSignatureOrName, CancellationToken ct = default) => Task.FromResult("-- procedures not supported");

        public Task AnalyzeAllAsync(string cs, CancellationToken ct = default) => Task.CompletedTask;
        public Task AnalyzeTableAsync(string cs, string schema, string table, CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string cs, string schema, string table, int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            using var conn = new SpannerConnection(cs);
            await conn.OpenAsync(ct);
            var cmd = conn.CreateSelectCommand($"SELECT * FROM `{table}`"); // Spanner ignores schema name in most cases
            using var rdr = await cmd.ExecuteReaderAsync(ct);

            var cols = Enumerable.Range(0, rdr.FieldCount).Select(i => rdr.GetName(i)).ToArray();
            while (await rdr.ReadAsync(ct))
            {
                var row = new object?[cols.Length];
                for (int i = 0; i < cols.Length; i++) row[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
                yield return (cols, row);
            }
        }


        public async Task<int> InsertRowsAsync(
            string cs, string schema, string table, string[] columns, IEnumerable<object?[]> rows,
            CancellationToken ct = default)
        {
            int total = 0;
            using var conn = new SpannerConnection(cs);
            await conn.OpenAsync(ct);

            // Build static parts once
            var colList = string.Join(", ", columns.Select(c => $"`{c}`"));
            var paramList = string.Join(", ", Enumerable.Range(0, columns.Length).Select(i => $"@p{i}"));
            var sql = $"INSERT INTO `{table}` ({colList}) VALUES ({paramList})";

            using var tx = await conn.BeginTransactionAsync(ct);
            try
            {
                foreach (var row in rows)
                {
                    using var cmd = conn.CreateDmlCommand(sql);
                    cmd.Transaction = tx;

                    for (int i = 0; i < columns.Length; i++)
                    {
                        var value = i < row.Length ? row[i] : null;
                        var (spType, spValue) = ToSpannerParam(value);
                        var p = cmd.Parameters.Add($"p{i}", spType);
                        // Spanner uses null (not DBNull)
                        p.Value = spValue;
                    }

                    total += await cmd.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
            }
            catch
            {
                await tx.RollbackAsync(ct);
                throw;
            }

            return total;
        }

        /// <summary>
        /// Map a .NET object to (SpannerDbType, Value).
        /// </summary>
       private static (SpannerDbType Type, object? Value) ToSpannerParam(object? v)
{
    if (v is null || v is DBNull) return (SpannerDbType.String, null);

    switch (v)
    {
        case sbyte or byte or short or ushort or int or uint or long:
            return (SpannerDbType.Int64, Convert.ToInt64(v));

        case float or double:
            return (SpannerDbType.Float64, Convert.ToDouble(v));

        case decimal dec:
            // Choose Throw or Truncate depending on migration safety
            return (SpannerDbType.Numeric, SpannerNumeric.FromDecimal(dec, LossOfPrecisionHandling.Truncate));

        case bool b:
            return (SpannerDbType.Bool, b);

        case DateTime dt:
            var hasTime = dt.TimeOfDay != TimeSpan.Zero;
            if (!hasTime)
                return (SpannerDbType.Date, dt.Date);
            var utc = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return (SpannerDbType.Timestamp, utc);

        case byte[] bytes:
            return (SpannerDbType.Bytes, bytes);

        case Guid g:
            return (SpannerDbType.String, g.ToString());

        default:
            return (SpannerDbType.String, v.ToString());
    }
}
        public async Task ExecuteNonQueryAsync(string cs, string sql, CancellationToken ct = default)
        {
            using var conn = new SpannerConnection(cs);
            await conn.OpenAsync(ct);
            using var cmd = conn.CreateDdlCommand(sql);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string cs, string sql, CancellationToken ct = default)
        {
            using var conn = new SpannerConnection(cs);
            await conn.OpenAsync(ct);
            using var cmd = conn.CreateSelectCommand(sql);
            var o = await cmd.ExecuteScalarAsync(ct);
            return o?.ToString();
        }
    }
}