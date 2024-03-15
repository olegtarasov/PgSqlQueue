using Dapper;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Consumer;

public class ConsumerService : IHostedService
{
    private const string FetchPartitionedSql =
        """
        update pgsqlqueue.messages
        set is_locked = true
        where id in (select id
                     from pgsqlqueue.messages
                     where id in (with ready as
                                           (SELECT id,
                                                   partition_key,
                                                   body,
                                                   is_locked,
                                                   row_number() over pw               as row_number,
                                                   bool_or(is_locked is true) over pw as has_lock
                                            FROM pgsqlqueue.messages
                                            WINDOW pw as ( partition by partition_key
                                                    order by enqueue_time
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ))
                                  select id
                                  from ready
                                  where not has_lock
                                    and row_number <= 1)
                         for update of messages skip locked)
        returning *;
        """;
    
    private readonly NpgsqlDataSource _dataSource;
    
    private int _isPolling = 0;
    private Timer _pollingTimer;

    public ConsumerService(string connectionString)
    {
        _dataSource = NpgsqlDataSource.Create(connectionString); 
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _pollingTimer = new Timer(OnPoll, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _pollingTimer.DisposeAsync();
        _pollingTimer = null;
    }

    private async void OnPoll(object? state)
    {
        // Check if previous polling batch is still running
        if (Interlocked.CompareExchange(ref _isPolling, 1, 0) == 1)
            return;

        await using var connection = await _dataSource.OpenConnectionAsync();
        var result = await connection.QueryAsync<Message>(FetchPartitionedSql);

        Interlocked.Exchange(ref _isPolling, 0);
    }
}