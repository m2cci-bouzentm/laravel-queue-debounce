# Laravel Queue Debounce

Leading-edge debounce for Laravel queued jobs. One job per debounce window, atomic Redis gating, crash recovery.

**The problem:** Webhooks, event listeners, and real-time triggers often fire multiple times for the same entity within seconds. Without debouncing, you get duplicate jobs flooding your queue — wasted workers, inflated stats, and race conditions.

**Existing solutions** either queue N closures and filter at execution time (polluting queue stats), or use `ShouldBeUnique` which releases the lock after processing (no cooldown window).

**This package** gates at dispatch time — only 1 job enters the queue per debounce window. Zero no-op closures, clean queue stats, and full compatibility with Laravel's `release()`, `$tries`, `$backoff`, and `failed()`.

## How it works

```
T=0s   Event A → Redis GETSET → dispatch(job)->delay(30)   ✓ queued
T=5s   Event B → Redis GETSET → job pending, skip           ✗ no-op (nothing queued)
T=10s  Event C → Redis GETSET → job pending, skip           ✗ no-op (nothing queued)
T=30s  Worker picks up job → middleware: Redis DEL → handle() runs
T=31s  Event D → Redis GETSET → no job pending → dispatch   ✓ queued (new window)
```

### vs ShouldBeUnique / ShouldBeUniqueUntilProcessing

| Feature | ShouldBeUnique | This package |
|---------|---------------|--------------|
| Lock held during | Processing only | Configurable delay window |
| Lock released | After handle() finishes | Before handle() starts (opens next window) |
| Crash recovery | Lock expires via TTL | Detects expired timestamps, re-queues |
| Queue stats | 1 job (clean) | 1 job (clean) |
| `release()` / `$tries` / `failed()` | Works | Works |
| Cooldown after execution | No | Yes (delay window) |

## Installation

```bash
composer require bouzentm/laravel-queue-debounce
```

Requires Redis (phpredis extension).

## Usage

### 1. Add the trait to your job

```php
use Bouzentm\LaravelQueueDebounce\Debounceable;

class AnalyzeConversationJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    use Debounceable;

    protected int $debounceDelay = 15; // seconds

    protected function debounceKey(): string
    {
        return 'analyze-conversation:' . $this->contactId;
    }

    public function __construct(public int $contactId) {}

    public function handle(AIService $aiService): void
    {
        // Your job logic — runs once per debounce window
    }
}
```

### 2. Dispatch with `::debounce()` instead of `::dispatch()`

```php
// Instead of:
AnalyzeConversationJob::dispatch($contact->id);

// Use:
AnalyzeConversationJob::debounce($contact->id);
```

That's it. Same constructor args, same queue, same everything — just debounced.

### 3. Merging with other middleware

The trait defines `middleware()` which cleans up the Redis key before `handle()` runs. If your job needs additional middleware (e.g. `WithoutOverlapping`), override `middleware()` and merge with `debounceMiddleware()`:

```php
public function middleware(): array
{
    return [
        ...$this->debounceMiddleware(),
        new WithoutOverlapping($this->contactId),
    ];
}
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `$debounceDelay` | `60` | Seconds to delay execution. During this window, subsequent `debounce()` calls are no-ops. |
| `debounceKey()` | (abstract) | Unique key for the debounce window. Include class name + entity ID. |

## Crash recovery

If a job crashes without cleanup (worker killed, OOM, etc.), the Redis key holds an expired timestamp. The next `debounce()` call detects this and re-queues:

```
T=0s   Job queued, Redis key set to T+30
T=30s  Worker crashes — Redis key still holds T+30
T=45s  New event → GETSET returns T+30 → T+30 <= now → crash detected → re-queue
```

## How it works internally

1. **`debounce()`** (dispatch-time): Uses Redis `GETSET` to atomically read the old timestamp and write a new one. If no job is pending (key was empty) or the previous job crashed (timestamp expired), the job is dispatched with `->delay()`. Otherwise, returns without queuing.

2. **`middleware()`** (execution-time): Cleans up the Redis key before `handle()` runs, opening the window for the next debounce cycle.

This is a **leading-edge** debounce: the first event triggers execution after the delay. Subsequent events during the window are dropped. After execution + cleanup, the next event starts a new window.

## Requirements

- PHP 8.3+
- Laravel 12+
- Redis (phpredis extension)

## License

MIT
