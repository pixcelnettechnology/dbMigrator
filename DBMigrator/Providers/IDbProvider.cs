using DBMigrator.Models;

namespace DBMigrator.Providers
{
    public interface IDbProvider
    {
        string Name { get; }

        // Metadata
        Task<List<TableSummary>> GetTablesAsync(string connectionString, CancellationToken ct = default);
        Task<List<string>> GetViewsAsync(string connectionString, CancellationToken ct = default);
        Task<string> GetViewDefinitionAsync(string connectionString, string schema, string viewName, CancellationToken ct = default);
        Task<List<string>> GetFunctionsAsync(string connectionString, CancellationToken ct = default);
        Task<string> GetFunctionDefinitionAsync(string connectionString, string functionSignatureOrName, CancellationToken ct = default);
        Task<List<string>> GetProceduresAsync(string connectionString, CancellationToken ct = default);
        Task<string> GetProcedureDefinitionAsync(string connectionString, string procSignatureOrName, CancellationToken ct = default);

        // Analyze / stats
        Task AnalyzeAllAsync(string connectionString, CancellationToken ct = default);
        Task AnalyzeTableAsync(string connectionString, string schema, string table, CancellationToken ct = default);

        // Data IO
        IAsyncEnumerable<(string[] Columns, object?[] Row)> ReadTableRowsAsync(string connectionString, string schema, string table, int batchSize, CancellationToken ct = default);
        Task<int> InsertRowsAsync(string connectionString, string schema, string table, string[] columns, IEnumerable<object?[]> rows, CancellationToken ct = default);

        // Helpers
        Task ExecuteNonQueryAsync(string connectionString, string sql, CancellationToken ct = default);
        Task<string?> ExecuteScalarAsync(string connectionString, string sql, CancellationToken ct = default);
    }
}