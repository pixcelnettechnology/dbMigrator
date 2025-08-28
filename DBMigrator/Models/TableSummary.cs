namespace DBMigrator.Models
{
    public class TableSummary
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public long EstimatedRowCount { get; set; } = 0;
        public long SizeBytes { get; set; } = 0;
        public int ColumnCount { get; set; } = 0;
    }
}