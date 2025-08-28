using DBMigrator.Models;
using DBMigrator.Providers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DBMigrator.Controllers
{
    public class LabController : Controller
    {
        private readonly ProviderFactory _factory;
        private readonly ILogger<LabController> _log;

        public LabController(ProviderFactory factory, ILogger<LabController> log)
        {
            _factory = factory;
            _log = log;
        }

        [HttpGet]
        public IActionResult Index()
        {
            // Pre-filled local docker lab connection strings
            var vm = new LabViewModel
            {
                Targets = new()
                {
                    new LabTarget{
                        Name="Postgres (Docker)",
                        Provider="Postgres",
                        ConnectionString="Host=localhost;Port=5432;Database=srcdb;Username=user;Password=pass;",
                        Hint="docker: postgres:16 with seed"
                    },
                    new LabTarget{
                        Name="SQL Server (Docker)",
                        Provider="SqlServer",
                        ConnectionString="Server=localhost,1433;Database=srcdb;User Id=sa;Password=Pass@word1;Encrypt=True;TrustServerCertificate=True;",
                        Hint="docker: mssql 2022 dev"
                    },
                    new LabTarget{
                        Name="MySQL (Docker)",
                        Provider="MySql",
                        ConnectionString="Server=localhost;Port=3306;Database=srcdb;User ID=user;Password=pass;",
                        Hint="docker: mysql:8 with seed"
                    },
                    new LabTarget{
                        Name="CockroachDB (Docker)",
                        Provider="CockroachDB",
                        ConnectionString="Host=localhost;Port=26257;Database=defaultdb;Username=root;Password=;SslMode=Disable;",
                        Hint="docker: cockroach single node (insecure)"
                    },
                    new LabTarget{
                        Name="Oracle XE (Docker, optional)",
                        Provider="Oracle",
                        ConnectionString="User Id=APPUSER;Password=pass;Data Source=//localhost:1521/XEPDB1;",
                        Hint="docker: gvenzl/oracle-xe"
                    },
                    new LabTarget{
                        Name="Spanner Emulator (optional)",
                        Provider="GoogleCloudSpanner",
                        ConnectionString="Data Source=projects/test/instances/test/databases/test;EmulatorDetection=EmulatorOnly",
                        Hint="docker: spanner emulator"
                    }
                }
            };
            return View(vm);
        }

        // AJAX: test one target
        [HttpPost]
        public async Task<IActionResult> Test([FromForm] string provider, [FromForm] string connectionString, CancellationToken ct)
        {
            try
            {
                var p = _factory.Create(provider);
                var sql = ProbeSql(provider);
                var scalar = await p.ExecuteScalarAsync(connectionString, sql, ct);
                return Json(new { ok = true, result = scalar ?? "(ok)" });
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Connectivity test failed for {Provider}", provider);
                return Json(new { ok = false, error = ex.Message });
            }
        }

        private static string ProbeSql(string provider) => provider switch
        {
            "Postgres" or "CockroachDB" or "Hyperscale" or "AmazonAuroraPostgres" => "SELECT 1",
            "SqlServer" or "AzureSQL" => "SELECT 1",
            "MySql" or "MariaDB" or "Percona" or "TiDB" or "AmazonAuroraMySql" => "SELECT 1",
            "Oracle" => "SELECT 1 FROM DUAL",
            "Db2" => "SELECT 1 FROM SYSIBM.SYSDUMMY1",
            "Pervasive" => "SELECT 1",
            "GoogleCloudSpanner" => "SELECT 1",
            _ => "SELECT 1"
        };
    }
}