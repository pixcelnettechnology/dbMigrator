using System.Data;
using DBMigrator.Models;
using Oracle.ManagedDataAccess.Client;

namespace DBMigrator.Providers
{
    public class OracleProvider : IDbProvider
    {
        public string Name => "Oracle";

        private static string Q(string ident) => $"\"{ident.Replace("\"", "\"\"")}\"";

        // ---------- TABLE METADATA ----------
        public async Task<List<TableSummary>> GetTablesAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<TableSummary>();
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);

            // Uses ALL_* (requires privileges). If your user is schema-scoped, switch to USER_* and owner = USER.
            var sql = @"
SELECT
    t.OWNER,
    t.TABLE_NAME,
    NVL(t.NUM_ROWS, 0) AS NUM_ROWS,  -- populated by DBMS_STATS / ANALYZE
    (SELECT NVL(SUM(BYTES),0)
       FROM ALL_SEGMENTS s
      WHERE s.OWNER = t.OWNER
        AND s.SEGMENT_NAME = t.TABLE_NAME
        AND s.SEGMENT_TYPE IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION')) AS SIZE_BYTES,
    (SELECT COUNT(*) FROM ALL_TAB_COLUMNS c
      WHERE c.OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME) AS COLUMN_COUNT
FROM ALL_TABLES t
WHERE t.OWNER NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','ORDSYS','ORDDATA','LBACSYS','OUTLN')
ORDER BY SIZE_BYTES DESC NULLS LAST, t.OWNER, t.TABLE_NAME";

            await using var cmd = new OracleCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add(new TableSummary
                {
                    Schema = rdr.GetString(0),
                    Name = rdr.GetString(1),
                    EstimatedRowCount = rdr.IsDBNull(2) ? 0 : rdr.GetInt64(2), // NUM_ROWS is NUMBER; provider maps to decimal/int64 safely
                    SizeBytes = rdr.IsDBNull(3) ? 0 : Convert.ToInt64(rdr.GetDecimal(3)),
                    ColumnCount = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4)
                });
            }
            return list;
        }

        // ---------- VIEWS / FUNCTIONS / PROCEDURES ----------
        public async Task<List<string>> GetViewsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);

            var sql = @"
SELECT OWNER, VIEW_NAME
FROM ALL_VIEWS
WHERE OWNER NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','ORDSYS','ORDDATA','LBACSYS','OUTLN')
ORDER BY OWNER, VIEW_NAME";
            await using var cmd = new OracleCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
            {
                list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            }
            return list;
        }

        public async Task<string> GetViewDefinitionAsync(string connectionString, string schema, string viewName, CancellationToken ct = default)
        {
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            // Requires EXECUTE on DBMS_METADATA. Consider: GRANT EXECUTE ON DBMS_METADATA TO <user>;
            const string sql = "SELECT DBMS_METADATA.GET_DDL('VIEW', :name, :owner) FROM DUAL";
            await using var cmd = new OracleCommand(sql, cn);
            cmd.BindByName = true;
            cmd.Parameters.Add(new OracleParameter("name", OracleDbType.Varchar2, viewName, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("owner", OracleDbType.Varchar2, schema, ParameterDirection.Input));
            var ddl = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return ddl ?? $"-- VIEW {schema}.{viewName} not found or not accessible";
        }

        public async Task<List<string>> GetFunctionsAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"
SELECT OWNER, OBJECT_NAME
FROM ALL_OBJECTS
WHERE OBJECT_TYPE = 'FUNCTION'
  AND OWNER NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','ORDSYS','ORDDATA','LBACSYS','OUTLN')
ORDER BY OWNER, OBJECT_NAME";
            await using var cmd = new OracleCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
                list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetFunctionDefinitionAsync(string connectionString, string functionSignatureOrName, CancellationToken ct = default)
        {
            var (schema, name) = SplitSchemaName(functionSignatureOrName);
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            const string sql = "SELECT DBMS_METADATA.GET_DDL('FUNCTION', :name, :owner) FROM DUAL";
            await using var cmd = new OracleCommand(sql, cn);
            cmd.BindByName = true;
            cmd.Parameters.Add("name", OracleDbType.Varchar2, name, ParameterDirection.Input);
            cmd.Parameters.Add("owner", OracleDbType.Varchar2, schema, ParameterDirection.Input);
            var ddl = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return ddl ?? $"-- FUNCTION {schema}.{name} not found or not accessible";
        }

        public async Task<List<string>> GetProceduresAsync(string connectionString, CancellationToken ct = default)
        {
            var list = new List<string>();
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            var sql = @"
SELECT OWNER, OBJECT_NAME
FROM ALL_OBJECTS
WHERE OBJECT_TYPE = 'PROCEDURE'
  AND OWNER NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','ORDSYS','ORDDATA','LBACSYS','OUTLN')
ORDER BY OWNER, OBJECT_NAME";
            await using var cmd = new OracleCommand(sql, cn);
            await using var rdr = await cmd.ExecuteReaderAsync(ct);
            while (await rdr.ReadAsync(ct))
                list.Add($"{rdr.GetString(0)}.{rdr.GetString(1)}");
            return list;
        }

        public async Task<string> GetProcedureDefinitionAsync(string connectionString, string procSignatureOrName, CancellationToken ct = default)
        {
            var (schema, name) = SplitSchemaName(procSignatureOrName);
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            const string sql = "SELECT DBMS_METADATA.GET_DDL('PROCEDURE', :name, :owner) FROM DUAL";
            await using var cmd = new OracleCommand(sql, cn);
            cmd.BindByName = true;
            cmd.Parameters.Add("name", OracleDbType.Varchar2, name, ParameterDirection.Input);
            cmd.Parameters.Add("owner", OracleDbType.Varchar2, schema, ParameterDirection.Input);
            var ddl = (await cmd.ExecuteScalarAsync(ct))?.ToString();
            return ddl ?? $"-- PROCEDURE {schema}.{name} not found or not accessible";
        }

        // ---------- ANALYZE / STATS ----------
        public async Task AnalyzeAllAsync(string connectionString, CancellationToken ct = default)
        {
            // Gather stats for all objects in the current user’s default schema.
            // To target a specific schema with privileges: DBMS_STATS.GATHER_SCHEMA_STATS('<SCHEMA>')
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            string currentUser;
            await using (var who = new OracleCommand("SELECT USER FROM DUAL", cn))
                currentUser = (await who.ExecuteScalarAsync(ct))?.ToString() ?? "USER";

            var plsql = "BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(ownname => :own, options => 'GATHER AUTO'); END;";
            await using var cmd = new OracleCommand(plsql, cn);
            cmd.BindByName = true;
            cmd.Parameters.Add("own", OracleDbType.Varchar2, currentUser, ParameterDirection.Input);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task AnalyzeTableAsync(string connectionString, string schema, string table, CancellationToken ct = default)
        {
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            var plsql = "BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => :own, tabname => :tab, cascade => TRUE); END;";
            await using var cmd = new OracleCommand(plsql, cn);
            cmd.BindByName = true;
            cmd.Parameters.Add("own", OracleDbType.Varchar2, schema, ParameterDirection.Input);
            cmd.Parameters.Add("tab", OracleDbType.Varchar2, table, ParameterDirection.Input);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // ---------- DATA IO ----------
        public async IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(
            string connectionString, string schema, string table, int batchSize,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);

            // Ensure consistent column order
            var colsCmd = new OracleCommand(@"
SELECT COLUMN_NAME
FROM ALL_TAB_COLUMNS
WHERE OWNER = :own AND TABLE_NAME = :tab
ORDER BY COLUMN_ID", cn);
            colsCmd.BindByName = true;
            colsCmd.Parameters.Add("own", OracleDbType.Varchar2, schema, ParameterDirection.Input);
            colsCmd.Parameters.Add("tab", OracleDbType.Varchar2, table, ParameterDirection.Input);

            var cols = new List<string>();
            await using (var rc = await colsCmd.ExecuteReaderAsync(ct))
                while (await rc.ReadAsync(ct)) cols.Add(rc.GetString(0));

            var select = $"SELECT {string.Join(", ", cols.Select(c => Q(c)))} FROM {Q(schema)}.{Q(table)}";
            await using var cmd = new OracleCommand(select, cn);
            cmd.InitialLONGFetchSize = -1; // fetch entire LONG if present
            await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

            var colNames = cols.ToArray();
            while (await rdr.ReadAsync(ct))
            {
                var row = new object?[colNames.Length];
                for (int i = 0; i < colNames.Length; i++)
                {
                    if (await rdr.IsDBNullAsync(i, ct)) { row[i] = null; continue; }

                    try
                    {
                        var v = rdr.GetValue(i);
                        if (v is DateTime dt)
                        {
                            // clamp invalid .NET DateTime ranges (Oracle supports BCE etc.)
                            row[i] = (dt.Year < 1 || dt.Year > 9999) ? null : v;
                        }
                        else row[i] = v;
                    }
                    catch
                    {
                        // Fallback to string if a type fails to convert
                        row[i] = rdr.GetValue(i)?.ToString();
                    }
                }
                yield return (colNames, row);
            }
        }

        public async Task<int> InsertRowsAsync(string connectionString, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default)
        {
            // Use Array Binding for high-throughput inserts
            var batch = rows.ToList();
            if (batch.Count == 0) return 0;

            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);

            var colList = string.Join(", ", columns.Select(Q));
            var valList = string.Join(", ", Enumerable.Range(0, columns.Length).Select(i => $":p{i}"));
            var sql = $"INSERT INTO {Q(schema)}.{Q(table)} ({colList}) VALUES ({valList})";

            await using var cmd = new OracleCommand(sql, cn) { BindByName = true };
            cmd.ArrayBindCount = batch.Count;

            for (int i = 0; i < columns.Length; i++)
            {
                var arr = new object?[batch.Count];
                for (int r = 0; r < batch.Count; r++)
                    arr[r] = batch[r][i] ?? DBNull.Value;

                var p = new OracleParameter($":p{i}", arr);
                cmd.Parameters.Add(p);
            }

            var affected = await cmd.ExecuteNonQueryAsync(ct); // returns total rows affected
            return affected;
        }

        // ---------- HELPERS ----------
        public async Task ExecuteNonQueryAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new OracleCommand(sql, cn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task<string?> ExecuteScalarAsync(string connectionString, string sql, CancellationToken ct = default)
        {
            await using var cn = new OracleConnection(connectionString);
            await cn.OpenAsync(ct);
            await using var cmd = new OracleCommand(sql, cn);
            var obj = await cmd.ExecuteScalarAsync(ct);
            return obj?.ToString();
        }

        private static (string Schema, string Name) SplitSchemaName(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return ("", "");
            var parts = input.Split('.', 2);
            return parts.Length == 2 ? (parts[0], parts[1]) : ("", parts[0]);
        }
    }
}