namespace DBMigrator.Models
{
    public class MigrationResult
    {
        public string ObjectName { get; set; } = string.Empty; // schema.table or VIEW:...
        public long RowsCopied { get; set; } = 0;
        public bool Success { get; set; } = true;
        public string ErrorMessage { get; set; } = string.Empty;
    }

    public class MigrationReport
    {
        public DateTime StartedAt { get; set; }
        public DateTime CompletedAt { get; set; }
        public List<MigrationResult> Results { get; set; } = new();
        public List<string> Errors { get; set; } = new();
        public string SourceProvider { get; set; } = string.Empty;
        public string TargetProvider { get; set; } = string.Empty;
    }
}