namespace DBMigrator.Models
{
    public class MigrationSummary
    {
        public string SourceProvider { get; set; } = string.Empty;
        public string TargetProvider { get; set; } = string.Empty;
        public List<TableSummary> SourceTables { get; set; } = new();
        public List<TableSummary> TargetTables { get; set; } = new();
        public List<string> Views { get; set; } = new();
        public List<string> Functions { get; set; } = new();
        public List<string> Procedures { get; set; } = new();
        public List<string> Warnings { get; set; } = new();
    }
}