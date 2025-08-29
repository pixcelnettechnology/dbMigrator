namespace DBMigrator.Models
{
    public class LabTarget
    {
        public string Name { get; set; } = "";
        public string Provider { get; set; } = "";
        public string ConnectionString { get; set; } = "";
        public string Hint { get; set; } = "";
    }

    public class LabViewModel
    {
        public List<LabTarget> Targets { get; set; } = new();
    }
}