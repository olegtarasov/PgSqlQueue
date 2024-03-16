using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Consumer;

public class CollectorDumper : IHostedService
{
    private readonly ILogger<CollectorDumper> _logger;

    public CollectorDumper(ILogger<CollectorDumper> logger)
    {
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Dumping stats");

        using var writer = new StreamWriter($"{Collector.Key}.csv");
        writer.WriteLine("start_time,end_time,index,key");

        foreach (var item in Collector.Items)
        {
            writer.WriteLine($"{item.StartTime:O},{item.EndTime:O},{item.Index},{item.Key}");
        }
    }
}