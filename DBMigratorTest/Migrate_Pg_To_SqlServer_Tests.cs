using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using DBMigrator.Services;
using DBMigrator.Providers;
using DBMigrator.Models;

public class Migrate_Pg_To_SqlServer_Tests
{
    [Fact]
    public async Task EndToEnd_PG_to_SQLServer()
    {
        // Arrange: reuse local docker-compose (or Testcontainers programmatically)
        var srcCs = "Host=localhost;Port=5432;Database=srcdb;Username=user;Password=pass;";
        var tgtCs = "Server=localhost,1433;Database=srcdb;User Id=sa;Password=Pass@word1;Encrypt=True;TrustServerCertificate=True;";

        var factory = new ProviderFactory(new NullLogger<ProviderFactory>());
        var service = new MigrationService(factory, new NullLogger<MigrationService>());

        var req = new MigrationRequest
        {
            SourceProvider = "Postgres",
            TargetProvider = "SqlServer",
            SourceConnectionString = srcCs,
            TargetConnectionString = tgtCs,
            Tables = "public.types_demo",
            ExactRowCounts = true,
            DropDestinationIfExists = true,
            BatchSize = 10000,
            IncludeViews = false, IncludeFunctions = false, IncludeProcedures = false
        };
        // Normalize TablesToMigrate for service
        if (!string.IsNullOrWhiteSpace(req.Tables))
            req.TablesToMigrate = req.Tables.Split(',', System.StringSplitOptions.RemoveEmptyEntries | System.StringSplitOptions.TrimEntries).ToList();

        // Act: preview then migrate
        var summary = await service.PreviewAsync(req);
        summary.SourceTables.Should().ContainSingle(t => t.Name == "types_demo");
        summary.SourceTables[0].EstimatedRowCount.Should().BeGreaterThan(0);

        var report = await service.MigrateAsync(req);

        // Assert: migration success & row counts > 0
        report.Errors.Should().BeEmpty();
        report.Results.Should().Contain(r => r.ObjectName.EndsWith("types_demo") && r.Success);
        report.Results.Find(r => r.ObjectName.EndsWith("types_demo"))!.RowsCopied.Should().BeGreaterThan(0);
    }
}