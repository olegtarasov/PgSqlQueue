namespace Consumer;

public record Message(Guid id, DateTime enqueue_time, string? partition_key, string? body, bool is_locked);