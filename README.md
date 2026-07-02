# Laravel Queue Debounce

Dispatch-time debounce for Laravel queued jobs. One job per debounce window, atomic Redis gating, crash recovery.

Works with **any Laravel queue driver**: Redis, SQS, Database, etc.

**The problem:** Webhooks, event listeners, and real-time triggers fire multiple times for the same entity within seconds. Without debouncing, you get duplicate jobs flooding your queue.

**This package** gates at dispatch time using Redis GETSET — only 1 job enters the queue per debounce window. Subsequent calls are no-ops (nothing queued).

## How it works

```
Time: 0s     5s      10s     30s     35s
      |      |       |       |       |
      v      v       v       v       v
    call   call    call   [executes] call
      |______|_______|          |_____|
           |                        |
    These 3 calls become       This call
    ONE execution              starts new window
```

1. First `::debounce()` → sets Redis key, queues job with delay
2. Subsequent calls within the window → Redis key exists, skip (nothing queued)
3. Job executes → middleware cleans up Redis key
4. Next call → starts a new debounce window

### vs ShouldBeUnique / ShouldBeUniqueUntilProcessing

| Feature | ShouldBeUnique | This package |
|---------|---------------|--------------|
| Lock held during | Processing only | Configurable delay window |
| Lock released | After handle() finishes | After handle(), only if not released |
| Crash recovery | Lock expires via TTL | Detects expired timestamps, re-queues |
| Queue stats | 1 job (clean) | 1 job (clean) |
| `release()` / `$tries` / `failed()` | Works | Works |
| Cooldown after execution | No | Yes (delay window) |

## Installation

```bash
composer require bouzentm/laravel-queue-debounce
```

## Requirements

- PHP 8.3+
- Laravel 12+
- Redis (phpredis extension)

## Usage

### Basic usage

```php
use Bouzentm\LaravelQueueDebounce\Debounceable;

class SyncContactJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    use Debounceable;

    protected function debounceKey(): string
    {
        return 'sync-contact:' . $this->contactId;
    }

    public function __construct(public int $contactId)
    {
        $this->debounceDelay = 30; // seconds
    }

    public function handle(): void
    {
        Contact::find($this->contactId)->syncToCrm();
    }
}

// In your listener or model:
class Contact extends Model
{
    protected static function booted(): void
    {
        static::saved(function (Contact $contact) {
            // Even if called 100 times in 30 seconds, only ONE job executes
            SyncContactJob::debounce($contact->id);
        });
    }
}
```

### Multiple arguments

The debounce key is defined by you, so you control granularity:

```php
class UpdateTicketJob implements ShouldQueue
{
    use Debounceable;

    protected function debounceKey(): string
    {
        return "update-ticket:{$this->ticketId}:{$this->updateType}";
    }

    public function __construct(
        public int $ticketId,
        public string $updateType
    ) {
        $this->debounceDelay = 60;
    }
}

// These are DIFFERENT debounce windows:
UpdateTicketJob::debounce(123, 'status');   // Window 1
UpdateTicketJob::debounce(123, 'priority'); // Window 2
UpdateTicketJob::debounce(456, 'status');   // Window 3
```

### Merging with other middleware

The trait defines `middleware()` which cleans up the Redis key after `handle()` runs. If your job needs additional middleware (e.g. `WithoutOverlapping`), override `middleware()` and merge with `debounceMiddleware()`:

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
| `debounceKey()` | (abstract) | Unique key for the debounce window. Include entity identifiers for proper scoping. |

### Custom debounce delay (PHP 8.4+)

The trait declares `protected int $debounceDelay = 60`. On PHP 8.4+, redeclaring the property in your job with a different default causes a `FatalError`:

```
App\Jobs\MyJob and Debounceable define the same property ($debounceDelay)
in the composition of App\Jobs\MyJob. However, the definition differs
and is considered incompatible.
```

Set the delay in your constructor instead:

```php
class PrepareReplyJob implements ShouldQueue
{
    use Debounceable;

    // Don't redeclare $debounceDelay here

    public function __construct(public int $contactId)
    {
        $this->debounceDelay = 1200; // 20 minutes
    }
}
```

## Crash recovery

If a job crashes without cleanup (worker killed, OOM, etc.), the Redis key holds an expired timestamp. The next `debounce()` call detects this and re-queues:

```
T=0s   Job queued, Redis key set to T+30
T=30s  Worker crashes — Redis key still holds T+30
T=45s  New event → GETSET returns T+30 → T+30 <= now → crash detected → re-queue
```

## Release-safe cleanup

If your job calls `$this->release()` (e.g. to retry later), the Redis key is **not** deleted — the debounce window stays active. The key is only cleaned up after a successful execution that doesn't release back to queue.

## How it works internally

Uses Redis `GETSET` for atomic dispatch-time gating:

1. `GETSET key new_timestamp` — atomically reads old value, writes new
2. If old value is `false` (no job pending) or expired (crashed) → queue the job
3. If old value is in the future → job already pending, skip
4. Middleware runs after `handle()` → deletes key only if not released → opens window for next cycle

The first event triggers execution after the delay. Subsequent events during the window are dropped.

## Testing

```php
use Illuminate\Support\Facades\Redis;

it('debounces multiple calls into one job', function () {
    Redis::shouldReceive('getset')
        ->once()->andReturn(false)   // first call: no key
        ->once()->andReturn(now()->addSeconds(30)->getTimestamp()); // second: pending

    Redis::shouldReceive('expire')->twice();

    SyncContactJob::debounce(123);
    SyncContactJob::debounce(123);

    Queue::assertPushed(SyncContactJob::class, 1);
});
```

## License

MIT
