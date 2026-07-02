<?php

namespace Bouzentm\LaravelQueueDebounce;

use Illuminate\Support\Facades\Redis;

/**
 * Leading-edge debounce for Laravel queued jobs. First dispatch queues the job,
 * subsequent dispatches within the delay window are no-ops.
 * After execution, the cache key is cleaned up — ready for the next trigger.
 * Recovers from crashed jobs: if the stored timestamp has elapsed but the key
 * still exists, a new job is queued.
 *
 * Uses Redis GETSET for atomic read+write.
 *
 * Usage:
 *   use Debounceable;
 *   protected int $debounceDelay = 60;
 *   protected function debounceKey(): string { return 'my-key:' . $this->id; }
 *
 * Dispatch with:
 *   MyJob::debounce($arg1, $arg2);
 */
trait Debounceable
{
    abstract protected function debounceKey(): string;

    protected int $debounceDelay = 60;

    // Extra TTL so the cache key outlives the delayed job if queue processing drifts
    private const BUFFER = 10;

    public static function debounce(mixed ...$params): void
    {
        /** @var static $instance */
        $instance = new static(...$params);

        $key = $instance->debounceKey();
        $delay = $instance->debounceDelay;
        $expiresAt = now()->addSeconds($delay)->getTimestamp();

        // Dispatch-time gating: check BEFORE queuing so only 1 job enters the queue
        // per debounce window.
        $oldTimestamp = Redis::getset($key, $expiresAt);
        Redis::expire($key, $delay + self::BUFFER);

        // phpredis GETSET returns false when key didn't exist
        $noJobPending = $oldTimestamp === false;
        $previousJobCrashed = (int) $oldTimestamp <= now()->getTimestamp();
        $shouldQueue = $noJobPending || $previousJobCrashed;

        if (!$shouldQueue) return;

        dispatch($instance)->delay($delay);
    }

    /**
     * Returns the debounce cleanup middleware. Deletes the Redis key
     * before handle() runs, opening the window for the next debounce cycle.
     *
     * Called by middleware() below. If your job needs additional middleware,
     * override middleware() and merge:
     *
     *   public function middleware(): array
     *   {
     *       return [
     *           ...$this->debounceMiddleware(),
     *           new WithoutOverlapping($this->id),
     *       ];
     *   }
     */
    protected function debounceMiddleware(): array
    {
        $cleanup = function ($job, $next) {
            Redis::del($this->debounceKey());
            $next($job);
        };

        return [$cleanup];
    }

    /**
     * Laravel queue convention: if a job defines a middleware() method,
     * CallQueuedHandler applies it before handle(). No interface needed —
     * Laravel checks via method_exists(). Same pattern as WithoutOverlapping,
     * RateLimited, ThrottlesExceptions.
     *
     * @see \Illuminate\Queue\CallQueuedHandler::dispatchThroughMiddleware
     */
    public function middleware(): array
    {
        return $this->debounceMiddleware();
    }
}
