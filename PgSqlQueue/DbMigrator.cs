using Npgsql;

namespace PgSqlQueue;

public class DbMigrator
{
    private const string CreateShit =
        """
        CREATE TABLE IF NOT EXISTS "{0}".messages
        (
            id                  uuid        not null primary key,
            
            index               integer not null,
            enqueue_time        timestamp with time zone not null ,
            partition_key       text,
            body                jsonb,
            is_locked           boolean
        );
        """;
    
    public async Task Migrate(string connectionString)
    {
        await using var source = NpgsqlDataSource.Create(connectionString);
        await using var createSchemaCommand = source.CreateCommand("CREATE SCHEMA IF NOT EXISTS pgsqlqueue");
        await createSchemaCommand.ExecuteNonQueryAsync();

        await using var createTablesCommand = source.CreateCommand(string.Format(CreateShit, "pgsqlqueue"));
        await createTablesCommand.ExecuteNonQueryAsync();
    }
}