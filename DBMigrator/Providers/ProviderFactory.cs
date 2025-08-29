namespace DBMigrator.Providers
{
    public class ProviderFactory
    {
        private readonly ILogger<ProviderFactory> _logger;
        public ProviderFactory(ILogger<ProviderFactory> logger) => _logger = logger;

        public IDbProvider Create(string providerName)
        {
            return providerName switch
            {
                // Existing
                "Postgres"        => new PostgresProvider(),
                "SqlServer"       => new SqlServerProvider(),
                "Oracle"          => new OracleProvider(),

                // Azure / Cloud variants
                "AzureSQL"        => new AzureSQLProvider(),
                "AmazonAuroraMySql"     => new AmazonAuroraMySqlProvider(),
                "AmazonAuroraPostgres"  => new AmazonAuroraPostgresProvider(),
                "Hyperscale"      => new HyperscalePostgresProvider(),
                "GoogleCloudSpanner" => new GoogleCloudSpannerProvider(),

                // MySQL family
                "MySql"           => new MySqlProvider(),
                "MariaDB"         => new MariaDbProvider(),
                "Percona"         => new PerconaProvider(),
                "TiDB"            => new TiDbProvider(),

                // CockroachDB (PG wire)
                "CockroachDB"     => new CockroachDbProvider(),

                // IBM Db2
                "Db2"             => new Db2OdbcProvider(),

                // Pervasive (ODBC generic)
                "Pervasive"       => new PerconaProvider(),

                // Aliases (optional)
                "Azure SQL"       => new AzureSQLProvider(),
                "Aurora MySQL"    => new AmazonAuroraMySqlProvider(),
                "Aurora Postgres" => new AmazonAuroraPostgresProvider(),

                _ => throw new InvalidOperationException($"Unknown provider '{providerName}'.")
            };
        }
    }
}