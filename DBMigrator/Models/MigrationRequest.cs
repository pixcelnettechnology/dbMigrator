namespace DBMigrator.Models
{
    public class MigrationRequest
    {
        public string SourceProvider { get; set; } = "Postgres"; // "Postgres" or "SqlServer"
        public string TargetProvider { get; set; } = "SqlServer";
        public string SourceConnectionString { get; set; } = string.Empty;
        public string TargetConnectionString { get; set; } = string.Empty;
        public List<string> TablesToMigrate { get; set; } = new(); // schema.table or table
        public bool DropDestinationIfExists { get; set; } = false;
        public int BatchSize { get; set; } = 1000;
        public string? Tables { get; set; }

        // Schema objects
        public bool IncludeViews { get; set; } = false;
        public bool IncludeFunctions { get; set; } = false;
        public bool IncludeProcedures { get; set; } = false;

        // Postgres statistics refresh
        public bool RunAnalyze { get; set; } = false;
        public bool ExactRowCounts { get; set; } = false; // COUNT(*) vs estimates in Summary
    }
}