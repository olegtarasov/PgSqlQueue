using System.Collections;
using System.Data;
using System.Xml;
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
                     limit 1
                         for update of messages skip locked)
        returning *;
        """;
    
    private readonly NpgsqlDataSource _dataSource;
    
    private int _isPolling = 0;
    private Timer _pollingTimer;
    private CancellationTokenSource _notificationsTokenSource;

    public ConsumerService(string connectionString)
    {
        _dataSource = NpgsqlDataSource.Create(connectionString); 
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _pollingTimer = new Timer(OnPoll, null, TimeSpan.FromSeconds(0.5), TimeSpan.FromSeconds(0.5));
        _notificationsTokenSource = new CancellationTokenSource();

        Task.Run(async () => await ListenToNotifications(_notificationsTokenSource.Token));
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _pollingTimer.DisposeAsync();
        _pollingTimer = null;
        await _notificationsTokenSource.CancelAsync();
    }

    private async Task ListenToNotifications(CancellationToken cancellationToken)
    {
        var connection = _dataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        connection.Notification += (sender, args) =>
        {
            OnPoll(null);
        };

        await using (var cmd = new NpgsqlCommand("LISTEN ap_messages", connection))
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            await connection.WaitAsync(cancellationToken);
        }
    }
    

    private async void OnPoll(object? state)
    {
        // Check if previous polling batch is still running
        if (Interlocked.CompareExchange(ref _isPolling, 1, 0) == 1)
            return;

        var result = Enumerable.Empty<Message>();
        await using (var connection = await _dataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
        {
            try
            {
                result = await connection.QueryAsync<Message>(FetchPartitionedSql, transaction: transaction);
                await transaction.CommitAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error in SELECT transaction!");
            }
        }

        foreach (var message in result)
        {
            var start = DateTime.Now;
            Collector.Items.Add(new(start, DateTime.Now, message.index, message.partition_key));

            Console.WriteLine($"Processed index {message.index}");

            await using (var connection = await _dataSource.OpenConnectionAsync())
            await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead))
            {
                await using var delCmd = new NpgsqlCommand(
                    $"DELETE from pgsqlqueue.messages where id='{message.id.ToString()}'", connection, transaction);
                try
                {
                    await delCmd.ExecuteNonQueryAsync();
                    await transaction.CommitAsync();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error in DELETE transaction!");
                }
            }
        }
        

        Interlocked.Exchange(ref _isPolling, 0);
    }
}