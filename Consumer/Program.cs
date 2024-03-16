using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SystemConsole.Themes;

namespace Consumer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Collector.Key = args[0];
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("Microsoft.Hosting", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();
            await CreateHostBuilder(args).Build().RunAsync();
        }
        
        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .UseSerilog()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<ConsumerService>(provider => new(
                        "Host=localhost;Port=5432;Database=assistant_platform;Username=postgres;Password=p@ssword"));
                    services.AddHostedService<CollectorDumper>();
                });
        }
    }
}
