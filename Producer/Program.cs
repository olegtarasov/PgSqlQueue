using Npgsql;
using NpgsqlTypes;
using PgSqlQueue;

await new DbMigrator()
    .Migrate("Host=localhost;Port=5432;Database=assistant_platform;Username=postgres;Password=p@ssword");

await using var source = NpgsqlDataSource.Create(
    "Host=localhost;Port=5432;Database=assistant_platform;Username=postgres;Password=p@ssword");

var conversations = Enumerable.Range(0, 5).Select(x => x.ToString()).ToArray();
var rnd = new Random();

for (int i = 0; i < 50; i++)
{
    var conversation = conversations[rnd.Next(conversations.Length)];
    var messageId = Guid.NewGuid();
    await using var command = source.CreateCommand("INSERT INTO pgsqlqueue.messages VALUES ($1, $2, $3, $4, $5)");
    command.Parameters.AddRange(new NpgsqlParameter[]
                                {
                                    new() {Value = messageId}, 
                                    new() {Value = DateTime.UtcNow},
                                    new() {Value = conversation},
                                    new() {Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Jsonb}, 
                                    new() {Value = false}
                                });
    int res = await command.ExecuteNonQueryAsync();
    Console.WriteLine(res);
}    