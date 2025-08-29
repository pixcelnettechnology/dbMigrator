using System.Collections.Concurrent;
using System.Text;
using DBMigrator.Models;
using DBMigrator.Providers;

namespace DBMigrator.Services
{
    public class MigrationService
    {
        private readonly ProviderFactory _factory;
        private readonly ILogger<MigrationService> _log;

        // Tune this if you want more/less parallelism across tables.
        private const int DEFAULT_TABLE_DOP = 2;

        public MigrationService(ProviderFactory factory, ILogger<MigrationService> logger)
        {
            _factory = factory;
            _log = logger;
        }

        private IDbProvider P(string name) => _factory.Create(name);

        // ---------- PREVIEW (unchanged except for optional fast analyze) ----------

        public async Task<MigrationSummary> PreviewAsync(MigrationRequest req, CancellationToken ct = default)
        {
            var src = P(req.SourceProvider);
            var tgt = P(req.TargetProvider);

            var summary = new MigrationSummary
            {
                SourceProvider = req.SourceProvider,
                TargetProvider = req.TargetProvider
            };

            try
            {
                var selected = (req.TablesToMigrate ?? new List<string>())
                               .Select(s => s.Trim())
                               .Where(s => !string.IsNullOrWhiteSpace(s))
                               .ToList();

                if (req.RunAnalyze)
                {
                    try { await src.AnalyzeAllAsync(req.SourceConnectionString, ct); }
                    catch (Exception ex) { _log.LogWarning(ex, "AnalyzeAllAsync failed; continuing."); }
                }

                var allSrcTables = await src.GetTablesAsync(req.SourceConnectionString, ct);
                summary.SourceTables = selected.Count == 0
                    ? allSrcTables
                    : allSrcTables.Where(t => selected.Contains($"{t.Schema}.{t.Name}", StringComparer.OrdinalIgnoreCase)
                                           || selected.Contains(t.Name, StringComparer.OrdinalIgnoreCase)).ToList();

                var allTgtTables = await tgt.GetTablesAsync(req.TargetConnectionString, ct);
                summary.TargetTables = selected.Count == 0
                    ? allTgtTables
                    : allTgtTables.Where(t => selected.Contains($"{t.Schema}.{t.Name}", StringComparer.OrdinalIgnoreCase)
                                           || selected.Contains(t.Name, StringComparer.OrdinalIgnoreCase)).ToList();

                if (req.ExactRowCounts && summary.SourceTables.Count > 0)
                    await FillExactRowCountsAsync(req, src, summary.SourceTables, ct);

                if (req.IncludeViews) summary.Views = await src.GetViewsAsync(req.SourceConnectionString, ct);
                if (req.IncludeFunctions) summary.Functions = await src.GetFunctionsAsync(req.SourceConnectionString, ct);
                if (req.IncludeProcedures) summary.Procedures = await src.GetProceduresAsync(req.SourceConnectionString, ct);

                foreach (var s in summary.SourceTables)
                {
                    var match = summary.TargetTables.FirstOrDefault(t => t.Schema.Equals(s.Schema, StringComparison.OrdinalIgnoreCase)
                                                                       && t.Name.Equals(s.Name, StringComparison.OrdinalIgnoreCase));
                    if (match == null) summary.Warnings.Add($"Table {s.Schema}.{s.Name} will be created on target.");
                    else if (match.ColumnCount != s.ColumnCount) summary.Warnings.Add($"Column count mismatch for {s.Schema}.{s.Name} (src:{s.ColumnCount} vs tgt:{match.ColumnCount}).");
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Preview failed");
                summary.Warnings.Add($"Preview error: {ex.Message}");
            }

            return summary;
        }

        // ---------- MIGRATION: FAST PATHS (parallel + batched) ----------

        public async Task<MigrationReport> MigrateAsync(MigrationRequest req, CancellationToken ct = default)
        {
            var report = new MigrationReport
            {
                StartedAt = DateTime.UtcNow,
                SourceProvider = req.SourceProvider,
                TargetProvider = req.TargetProvider
            };

            var src = P(req.SourceProvider);
            var tgt = P(req.TargetProvider);

            try
            {
                var srcTablesAll = await src.GetTablesAsync(req.SourceConnectionString, ct);
                var selected = (req.TablesToMigrate ?? new List<string>())
                               .Select(s => s.Trim())
                               .Where(s => !string.IsNullOrWhiteSpace(s))
                               .ToList();

                var tables = (selected.Count == 0 ? srcTablesAll.Select(t => $"{t.Schema}.{t.Name}") : selected).ToList();

                if (req.RunAnalyze)
                {
                    try { await src.AnalyzeAllAsync(req.SourceConnectionString, ct); }
                    catch (Exception ex) { _log.LogWarning(ex, "AnalyzeAllAsync failed; will continue."); }
                }

                // Parallel table migration for throughput
                var dop = Math.Max(1, DEFAULT_TABLE_DOP);
                var tableQueue = new ConcurrentQueue<string>(tables);
                var tableResults = new ConcurrentBag<MigrationResult>();
                var errors = new ConcurrentBag<string>();

                var workers = Enumerable.Range(0, dop).Select(async _ =>
                {
                    while (tableQueue.TryDequeue(out var full))
                    {
                        var (schema, table) = SplitSchemaTable(full, req.SourceProvider);
                        try
                        {
                            await EnsureSchemaAsync(req.TargetProvider, tgt, req.TargetConnectionString, schema, ct);
                            await EnsureTableAsync(src, tgt, req, schema, table, ct);

                            var copied = await CopyTableFastAsync(src, tgt, req, schema, table, ct);

                            tableResults.Add(new MigrationResult
                            {
                                ObjectName = $"{schema}.{table}",
                                RowsCopied = copied,
                                Success = true
                            });
                        }
                        catch (Exception ex)
                        {
                            _log.LogError(ex, "Failed migrating table {Schema}.{Table}", schema, table);
                            tableResults.Add(new MigrationResult
                            {
                                ObjectName = $"{schema}.{table}",
                                Success = false,
                                ErrorMessage = ex.Message
                            });
                            errors.Add($"TABLE {schema}.{table}: {ex.Message}");
                        }
                    }
                }).ToArray();

                await Task.WhenAll(workers);
                report.Results.AddRange(tableResults.OrderBy(r => r.ObjectName));
                report.Errors.AddRange(errors);

                // Views
                if (req.IncludeViews)
                {
                    var views = await src.GetViewsAsync(req.SourceConnectionString, ct);
                    foreach (var v in views)
                    {
                        try
                        {
                            var (schema, name) = SplitSchemaTable(v, req.SourceProvider);
                            var ddl = await src.GetViewDefinitionAsync(req.SourceConnectionString, schema, name, ct);
                            await tgt.ExecuteNonQueryAsync(req.TargetConnectionString, ddl, ct);
                            report.Results.Add(new MigrationResult { ObjectName = $"VIEW:{v}", Success = true });
                        }
                        catch (Exception ex)
                        {
                            report.Results.Add(new MigrationResult { ObjectName = $"VIEW:{v}", Success = false, ErrorMessage = ex.Message });
                            report.Errors.Add($"VIEW {v}: {ex.Message}");
                        }
                    }
                }

                // Functions
                if (req.IncludeFunctions)
                {
                    var funcs = await src.GetFunctionsAsync(req.SourceConnectionString, ct);
                    foreach (var f in funcs)
                    {
                        try
                        {
                            var def = await src.GetFunctionDefinitionAsync(req.SourceConnectionString, f, ct);
                            await tgt.ExecuteNonQueryAsync(req.TargetConnectionString, def, ct);
                            report.Results.Add(new MigrationResult { ObjectName = $"FUNCTION:{f}", Success = true });
                        }
                        catch (Exception ex)
                        {
                            report.Results.Add(new MigrationResult { ObjectName = $"FUNCTION:{f}", Success = false, ErrorMessage = ex.Message });
                            report.Errors.Add($"FUNCTION {f}: {ex.Message}");
                        }
                    }
                }

                // Procedures
                if (req.IncludeProcedures)
                {
                    var procs = await src.GetProceduresAsync(req.SourceConnectionString, ct);
                    foreach (var p in procs)
                    {
                        try
                        {
                            var def = await src.GetProcedureDefinitionAsync(req.SourceConnectionString, p, ct);
                            await tgt.ExecuteNonQueryAsync(req.TargetConnectionString, def, ct);
                            report.Results.Add(new MigrationResult { ObjectName = $"PROCEDURE:{p}", Success = true });
                        }
                        catch (Exception ex)
                        {
                            report.Results.Add(new MigrationResult { ObjectName = $"PROCEDURE:{p}", Success = false, ErrorMessage = ex.Message });
                            report.Errors.Add($"PROCEDURE {p}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Migration pipeline error");
                report.Errors.Add($"Migration pipeline error: {ex.Message}");
            }
            finally
            {
                report.CompletedAt = DateTime.UtcNow;
            }

            return report;
        }

        // ---------- FAST TABLE COPY ----------

        private async Task<long> CopyTableFastAsync(IDbProvider src, IDbProvider tgt, MigrationRequest req, string schema, string table, CancellationToken ct)
        {
            // Large batch size exploits provider fast paths (COPY, SqlBulkCopy, array binding, etc.)
            var batchSize = Math.Max(1000, req.BatchSize <= 0 ? 10_000 : req.BatchSize);

            long copied = 0;
            string[]? columns = null;
            var buffer = new List<object?[]>(capacity: batchSize);

            await foreach (var (Cols, Row) in src.ReadTableRowsAsync(req.SourceConnectionString, schema, table, batchSize, ct))
            {
                columns ??= Cols;
                buffer.Add(Row);

                if (buffer.Count >= batchSize)
                {
                    copied += await tgt.InsertRowsAsync(req.TargetConnectionString, schema, table, columns!, buffer, ct);
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0 && columns is not null)
                copied += await tgt.InsertRowsAsync(req.TargetConnectionString, schema, table, columns, buffer, ct);

            return copied;
        }

        // ---------- HELPERS (schema/table, DDL, counts) ----------

        private static (string Schema, string Table) SplitSchemaTable(string input, string provider)
        {
            if (string.IsNullOrWhiteSpace(input)) return ("public", input);
            var s = input.Trim();
            if (s.Contains('.'))
            {
                var parts = s.Split('.', 2);
                return (parts[0].Trim(), parts[1].Trim());
            }
            var def = provider switch
            {
                "SqlServer" or "AzureSQL" => "dbo",
                _ => "public"
            };
            return (def, s);
        }

        private async Task FillExactRowCountsAsync(MigrationRequest req, IDbProvider src, List<TableSummary> tables, CancellationToken ct)
        {
            foreach (var t in tables)
            {
                try
                {
                    var sql = req.SourceProvider switch
                    {
                        "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres"
                            => $"SELECT COUNT(*)::bigint FROM \"{t.Schema}\".\"{t.Name}\"",
                        "SqlServer" or "AzureSQL"
                            => $"SELECT COUNT_BIG(*) FROM [{t.Schema}].[{t.Name}]",
                        "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql"
                            => $"SELECT COUNT(*) FROM `{t.Schema}`.`{t.Name}`",
                        "Oracle"
                            => $"SELECT COUNT(*) FROM \"{t.Schema}\".\"{t.Name}\"",
                        "Db2"
                            => $"SELECT COUNT(*) FROM \"{t.Schema}\".\"{t.Name}\"",
                        "Pervasive"
                            => $"SELECT COUNT(*) FROM [{t.Schema}].[{t.Name}]",
                        "GoogleCloudSpanner"
                            => $"SELECT COUNT(*) FROM `{t.Name}`",
                        _ => null
                    };
                    if (string.IsNullOrWhiteSpace(sql)) continue;
                    var s = await src.ExecuteScalarAsync(req.SourceConnectionString, sql!, ct);
                    if (long.TryParse(s, out var exact)) t.EstimatedRowCount = exact;
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Exact count failed for {Schema}.{Table}", t.Schema, t.Name);
                }
            }
        }

        private async Task EnsureSchemaAsync(string targetProvider, IDbProvider tgt, string targetCs, string schema, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(schema)) return;

            try
            {
                string sql = targetProvider switch
                {
                    "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres"
                        => $"CREATE SCHEMA IF NOT EXISTS \"{schema}\";",
                    "SqlServer" or "AzureSQL"
                        => @$"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')
                              EXEC('CREATE SCHEMA [{schema}]');",
                    "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql"
                        => $"CREATE SCHEMA IF NOT EXISTS `{schema}`;",
                    _ => string.Empty
                };
                if (!string.IsNullOrWhiteSpace(sql))
                    await tgt.ExecuteNonQueryAsync(targetCs, sql, ct);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "EnsureSchema failed for {Provider} schema {Schema}", targetProvider, schema);
            }
        }

        private async Task EnsureTableAsync(IDbProvider src, IDbProvider tgt, MigrationRequest req, string schema, string table, CancellationToken ct)
        {
            var tgtTables = await tgt.GetTablesAsync(req.TargetConnectionString, ct);
            var exists = tgtTables.Any(t => t.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase)
                                         && t.Name.Equals(table, StringComparison.OrdinalIgnoreCase));

            if (exists && !req.DropDestinationIfExists)
                return;

            if (exists && req.DropDestinationIfExists)
            {
                try
                {
                    var dropSql = req.TargetProvider switch
                    {
                        "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres"
                            => $"DROP TABLE IF EXISTS \"{schema}\".\"{table}\" CASCADE;",
                        "SqlServer" or "AzureSQL"
                            => $"IF OBJECT_ID(N'[{schema}].[{table}]', 'U') IS NOT NULL DROP TABLE [{schema}].[{table}];",
                        "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql"
                            => $"DROP TABLE IF EXISTS `{schema}`.`{table}`;",
                        "Oracle"
                            => $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"{schema}\".\"{table}\" CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                        "Db2"
                            => $"BEGIN ATOMIC DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; EXECUTE IMMEDIATE 'DROP TABLE \"{schema}\".\"{table}\"'; END",
                        "Pervasive"
                            => $"DROP TABLE [{schema}].[{table}]",
                        "GoogleCloudSpanner"
                            => $"DROP TABLE `{table}`",
                        _ => null
                    };
                    if (!string.IsNullOrWhiteSpace(dropSql))
                        await tgt.ExecuteNonQueryAsync(req.TargetConnectionString, dropSql!, ct);
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Drop existing table failed for {Schema}.{Table}", schema, table);
                }
            }

            var ddl = await BuildCreateTableDdlAsync(src, req.SourceProvider, req.SourceConnectionString,
                                                     req.TargetProvider, schema, table, ct);
            await tgt.ExecuteNonQueryAsync(req.TargetConnectionString, ddl, ct);
        }

        // ---------- Basic DDL Synthesis (unchanged; type mapping kept lean for speed) ----------

        private async Task<string> BuildCreateTableDdlAsync(
            IDbProvider sourceProvider, string sourceName, string sourceCs,
            string targetName, string schema, string table, CancellationToken ct)
        {
            var cols = await GetSourceColumnsAsync(sourceProvider, sourceName, sourceCs, schema, table, ct);
            if (cols.Count == 0) throw new InvalidOperationException($"No columns discovered for {schema}.{table} on source.");

            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {Qualify(targetName, schema, table)} (");
            for (int i = 0; i < cols.Count; i++)
            {
                var c = cols[i];
                var mappedType = MapType(c, targetName);
                var nullSql = c.IsNullable ? " NULL" : " NOT NULL";
                sb.Append($"  {Quote(targetName, c.Name)} {mappedType}{nullSql}");
                if (i < cols.Count - 1) sb.Append(",");
                sb.AppendLine();
            }
            sb.AppendLine(");");
            return sb.ToString();
        }

        private static string Qualify(string provider, string schema, string table) => provider switch
        {
            "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres" or "Oracle" or "Db2"
                => $"\"{schema}\".\"{table}\"",
            "SqlServer" or "AzureSQL"
                => $"[{schema}].[{table}]",
            "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql"
                => $"`{schema}`.`{table}`",
            "Pervasive"
                => $"[{schema}].[{table}]",
            "GoogleCloudSpanner"
                => $"`{table}`",
            _ => $"\"{schema}\".\"{table}\""
        };

        private static string Quote(string provider, string ident) => provider switch
        {
            "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres" or "Oracle" or "Db2"
                => $"\"{ident.Replace("\"", "\"\"")}\"",
            "SqlServer" or "AzureSQL"
                => $"[{ident.Replace("]", "]]")}]",
            "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql"
                => $"`{ident.Replace("`", "``")}`",
            "Pervasive"
                => $"[{ident.Replace("]", "]]")}]",
            "GoogleCloudSpanner"
                => $"`{ident}`",
            _ => ident
        };

        private static string MapType(SourceColumn c, string target)
        {
            var t = c.DataType.ToLowerInvariant();
            bool isChar = t.Contains("char");
            bool isVarChar = t.Contains("varchar");
            bool isText = t.Contains("text") || t.Contains("clob") || t.Contains("longtext") || t.Contains("ntext");
            bool isBool = t is "boolean" or "bool" or "bit";
            bool isInt = t.Contains("int") && !t.Contains("interval");
            bool isBigInt = t.Contains("bigint");
            bool isSmallInt = t.Contains("smallint");
            bool isTinyInt = t.Contains("tinyint");
            bool isDecimal = t.Contains("numeric") || t.Contains("decimal") || t.Contains("number");
            bool isFloat = t.Contains("float") || t.Contains("double") || t.Contains("real");
            bool isDate = t == "date";
            bool isTime = t.StartsWith("time");
            bool isTimestamp = t.Contains("timestamp") || t.Contains("datetime");
            bool isBinary = t.Contains("binary") || t.Contains("varbinary") || t.Contains("blob") || t.Contains("bytea");
            bool isUuid = t.Contains("uuid") || t.Contains("uniqueidentifier");
            bool isJson = t.Contains("json");

            string len = c.Length > 0 ? c.Length.ToString() : "";
            string prec = c.Precision > 0 ? c.Precision.ToString() : "";
            string scale = c.Scale > 0 ? c.Scale.ToString() : "";

            return target switch
            {
                "SqlServer" or "AzureSQL" => MapToSqlServer(),
                "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres" => MapToPostgres(),
                "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql" => MapToMySql(),
                "Oracle" => MapToOracle(),
                "Db2" => MapToDb2(),
                "Pervasive" => MapToSqlServer(),
                "GoogleCloudSpanner" => MapToSpanner(),
                _ => MapToPostgres()
            };

            string MapToSqlServer()
            {
                if (isBool) return "bit";
                if (isUuid) return "uniqueidentifier";
                if (isBigInt) return "bigint";
                if (isInt) return "int";
                if (isSmallInt) return "smallint";
                if (isTinyInt) return "tinyint";
                if (isDecimal) return (c.Precision, c.Scale) switch { ( > 0, >= 0) => $"decimal({prec},{(string.IsNullOrEmpty(scale) ? "0" : scale)})", _ => "decimal(38,9)" };
                if (isFloat) return "float";
                if (isDate) return "date";
                if (isTimestamp) return "datetime2";
                if (isTime) return "time";
                if (isBinary) return "varbinary(max)";
                if (isText) return "nvarchar(max)";
                if (isVarChar || isChar) return $"nvarchar({(string.IsNullOrEmpty(len) ? "255" : len)})";
                if (isJson) return "nvarchar(max)";
                return "nvarchar(max)";
            }
            string MapToPostgres()
            {
                if (isBool) return "boolean";
                if (isUuid) return "uuid";
                if (isBigInt) return "bigint";
                if (isInt) return "integer";
                if (isSmallInt) return "smallint";
                if (isDecimal) return (c.Precision, c.Scale) switch { ( > 0, >= 0) => $"numeric({prec},{(string.IsNullOrEmpty(scale) ? "0" : scale)})", _ => "numeric" };
                if (isFloat) return "double precision";
                if (isDate) return "date";
                if (isTimestamp) return "timestamp";
                if (isTime) return "time";
                if (isBinary) return "bytea";
                if (isText) return "text";
                if (isVarChar || isChar) return $"varchar({(string.IsNullOrEmpty(len) ? "255" : len)})";
                if (isJson) return "jsonb";
                return "text";
            }
            string MapToMySql()
            {
                if (isBool) return "tinyint(1)";
                if (isUuid) return "char(36)";
                if (isBigInt) return "bigint";
                if (isInt) return "int";
                if (isSmallInt) return "smallint";
                if (isDecimal) return (c.Precision, c.Scale) switch { ( > 0, >= 0) => $"decimal({prec},{(string.IsNullOrEmpty(scale) ? "0" : scale)})", _ => "decimal(38,9)" };
                if (isFloat) return "double";
                if (isDate) return "date";
                if (isTimestamp) return "datetime(6)";
                if (isTime) return "time";
                if (isBinary) return "longblob";
                if (isText) return "longtext";
                if (isVarChar || isChar) return $"varchar({(string.IsNullOrEmpty(len) ? "255" : len)})";
                if (isJson) return "json";
                return "longtext";
            }
            string MapToOracle()
            {
                if (isBool) return "NUMBER(1)";
                if (isUuid) return "VARCHAR2(36)";
                if (isBigInt) return "NUMBER(19)";
                if (isInt) return "NUMBER(10)";
                if (isSmallInt) return "NUMBER(5)";
                if (isDecimal) return (c.Precision, c.Scale) switch { ( > 0, >= 0) => $"NUMBER({prec},{(string.IsNullOrEmpty(scale) ? "0" : scale)})", _ => "NUMBER" };
                if (isFloat) return "BINARY_DOUBLE";
                if (isDate) return "DATE";
                if (isTimestamp) return "TIMESTAMP";
                if (isTime) return "VARCHAR2(16)";
                if (isBinary) return "BLOB";
                if (isText) return "CLOB";
                if (isVarChar || isChar) return $"VARCHAR2({(string.IsNullOrEmpty(len) ? "255" : len)})";
                if (isJson) return "CLOB";
                return "CLOB";
            }
            string MapToDb2()
            {
                if (isBool) return "SMALLINT";
                if (isUuid) return "CHAR(36)";
                if (isBigInt) return "BIGINT";
                if (isInt) return "INTEGER";
                if (isSmallInt) return "SMALLINT";
                if (isDecimal) return (c.Precision, c.Scale) switch { ( > 0, >= 0) => $"DECIMAL({prec},{(string.IsNullOrEmpty(scale) ? "0" : scale)})", _ => "DECIMAL(38,9)" };
                if (isFloat) return "DOUBLE";
                if (isDate) return "DATE";
                if (isTimestamp) return "TIMESTAMP";
                if (isTime) return "TIME";
                if (isBinary) return "BLOB";
                if (isText) return "CLOB";
                if (isVarChar || isChar) return $"VARCHAR({(string.IsNullOrEmpty(len) ? "255" : len)})";
                if (isJson) return "CLOB";
                return "CLOB";
            }
            string MapToSpanner()
            {
                if (isBool) return "BOOL";
                if (isUuid) return "STRING(36)";
                if (isBigInt || isInt || isSmallInt) return "INT64";
                if (isDecimal) return "NUMERIC";
                if (isFloat) return "FLOAT64";
                if (isDate) return "DATE";
                if (isTimestamp) return "TIMESTAMP";
                if (isTime) return "STRING(16)";
                if (isBinary) return "BYTES(MAX)";
                if (isText || isVarChar || isChar) return "STRING(MAX)";
                if (isJson) return "JSON";
                return "STRING(MAX)";
            }
        }

        private static async Task<List<SourceColumn>> GetSourceColumnsAsync(
            IDbProvider src, string sourceName, string cs, string schema, string table, CancellationToken ct)
        {
            var cols = new List<SourceColumn>();
            string aggSql = sourceName switch
            {
                "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres" =>
                   $@"SELECT string_agg(format('%s|%s|%s|%s|%s|%s',
                        column_name, data_type, is_nullable,
                        COALESCE(character_maximum_length::text,''), COALESCE(numeric_precision::text,''), COALESCE(numeric_scale::text,'')),
                        ';;' ORDER BY ordinal_position)
                        FROM information_schema.columns
                        WHERE table_schema='{schema}' AND table_name='{table}';",
                "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql" =>
                    $@"SELECT GROUP_CONCAT(CONCAT_WS('|',
                        column_name, data_type, is_nullable,
                        IFNULL(character_maximum_length,''), IFNULL(numeric_precision,''), IFNULL(numeric_scale,'')) ORDER BY ordinal_position SEPARATOR ';;')
                       FROM information_schema.columns
                       WHERE table_schema='{schema}' AND table_name='{table}'",
                "SqlServer" or "AzureSQL" =>
                    $@"SELECT STUFF((
                        SELECT ';;' + c.name + '|' + t.name + '|' + CASE WHEN c.is_nullable=1 THEN 'YES' ELSE 'NO' END + '|' +
                               ISNULL(CASE WHEN t.name IN ('nvarchar','nchar') THEN CAST(c.max_length/2 AS VARCHAR(10)) ELSE CAST(c.max_length AS VARCHAR(10)) END,'') + '|' +
                               ISNULL(CAST(c.precision AS VARCHAR(10)),'') + '|' + ISNULL(CAST(c.scale AS VARCHAR(10)),'')
                        FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
                        WHERE c.object_id = OBJECT_ID(N'[{schema}].[{table}]')
                        ORDER BY c.column_id
                        FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'')",
                "Oracle" =>
                    $@"SELECT LISTAGG(column_name || '|' || data_type || '|' || CASE NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END || '|' ||
                               COALESCE(TO_CHAR(data_length),'') || '|' || COALESCE(TO_CHAR(data_precision),'') || '|' || COALESCE(TO_CHAR(data_scale),''),
                               ';;') WITHIN GROUP (ORDER BY column_id)
                       FROM ALL_TAB_COLUMNS
                       WHERE owner='{schema.ToUpperInvariant()}' AND table_name='{table.ToUpperInvariant()}'",
                "Db2" =>
                    $@"SELECT LISTAGG(COLNAME || '|' || TYPENAME || '|' || CASE NULLS WHEN 'Y' THEN 'YES' ELSE 'NO' END || '|' ||
                               COALESCE(CHAR(LENGTH),'') || '|' || COALESCE(CHAR(LENGTH),'') || '|' || COALESCE(CHAR(SCALE),''),
                               ';;') WITHIN GROUP (ORDER BY COLNO)
                       FROM SYSCAT.COLUMNS
                       WHERE TABSCHEMA='{schema.ToUpperInvariant()}' AND TABNAME='{table.ToUpperInvariant()}'",
                "Pervasive" =>
                    $@"SELECT STRING_AGG(COLUMN_NAME || '|' || DATA_TYPE || '|YES|||', ';;')
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'",
                "GoogleCloudSpanner" =>
                    $@"SELECT STRING_AGG(column_name || '|' || spanner_type || '|' || is_nullable || '|||', ';;')
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE table_name='{table}'
                       ORDER BY ordinal_position",
                _ => throw new NotSupportedException($"Source {sourceName} not supported for column introspection.")
            };

            var blob = await src.ExecuteScalarAsync(cs, aggSql, ct) ?? "";
            if (!string.IsNullOrWhiteSpace(blob))
            {
                foreach (var line in blob.Split(new[] { ";;" }, StringSplitOptions.RemoveEmptyEntries))
                {
                    var parts = line.Split('|');
                    var name = parts.ElementAtOrDefault(0) ?? "";
                    var dtype = parts.ElementAtOrDefault(1) ?? "";
                    var isNull = (parts.ElementAtOrDefault(2) ?? "YES").Equals("YES", StringComparison.OrdinalIgnoreCase);
                    var length = int.TryParse(parts.ElementAtOrDefault(3), out var l) ? l : 0;
                    var prec = int.TryParse(parts.ElementAtOrDefault(4), out var p) ? p : 0;
                    var scale = int.TryParse(parts.ElementAtOrDefault(5), out var s) ? s : 0;

                    if (!string.IsNullOrWhiteSpace(name))
                        cols.Add(new SourceColumn { Name = name, DataType = dtype, IsNullable = isNull, Length = length, Precision = prec, Scale = scale });
                }
            }
            return cols;
        }

        private class SourceColumn
        {
            public string Name { get; set; } = string.Empty;
            public string DataType { get; set; } = string.Empty;
            public bool IsNullable { get; set; }
            public int Length { get; set; }
            public int Precision { get; set; }
            public int Scale { get; set; }
        }
    }
}