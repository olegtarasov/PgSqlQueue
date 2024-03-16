namespace Consumer;

public static class Collector
{
    public static string Key;
    public static readonly List<CollectorItem> Items = new();
}

public record CollectorItem(DateTime StartTime, DateTime EndTime, int Index, string Key);