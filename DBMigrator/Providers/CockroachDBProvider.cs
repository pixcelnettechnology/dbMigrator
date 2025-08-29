using DBMigrator.Models;
using Npgsql;

namespace DBMigrator.Providers
{
    public class CockroachDbProvider : PostgresProvider
    {
        public new string Name => "CockroachDB";

        // Override GetTablesAsync to avoid pg_total_relation_size (not supported in older CRDB)
        public override async Task<List<TableSummary>> GetTablesAsync(string cs, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            await using var cn = new NpgsqlConnection(cs);
            await cn.OpenAsync(ct);

            // Use information_schema for columns; estimated rows from crdb_internal if available
            var sql = @"
SELECT t.table_schema, t.table_name,
       0::BIGINT AS estimated_rows,  -- could be improved via crdb_internal.table_row_statistics
       0::BIGINT AS size_bytes,
       (SELECT COUNT(*) FROM information_schema.columns c 
         WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) AS col_count
FROM information_schema.tables t
WHERE t.table_type='BASE TABLE' AND t.table_schema NOT IN ('pg_catalog','information_schema')
ORDER BY t.table_schema, t.table_name;";
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
    }
}