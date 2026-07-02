<?php

namespace Bouzentm\LaravelQueueDebounce;

use Illuminate\Support\Facades\Redis;

/**
 * Leading-edge debounce for Laravel queued jobs.
 *
 * First dispatch queues the job with a configurable delay.
 * Subsequent dispatches within the debounce window are no-ops — the job
 * never enters the queue, keeping your queue stats clean.
 * After execution, the Redis key is cleaned up — ready for the next trigger.
 *
 * Crash recovery: if the stored timestamp has elapsed but the key still exists,
 * a new job is queued (the previous one crashed without cleanup).
 *
 * Uses Redis GETSET for atomic dispatch-time gating.
 *
 * Usage:
 *   use Debounceable;
 *   protected int $debounceDelay = 60; // seconds
 *   protected function debounceKey(): string { return 'my-key:' . $this->id; }
 *
 * Dispatch with:
 *   MyJob::debounce($arg1, $arg2);
 */
trait Debounceable
{
    /**
     * Return a unique key for the debounce window.
     * Typically includes the job class and a unique identifier (e.g. model ID).
     */
    abstract protected function debounceKey(): string;

    /**
     * Seconds to delay execution. During this window, subsequent
     * debounce() calls for the same key are silently dropped.
     */
    protected int $debounceDelay = 60;

    /**
     * Extra TTL buffer so the Redis key outlives the delayed job
     * if queue processing drifts slightly behind schedule.
     */
    private const BUFFER = 10;

    /**
     * Dispatch the job with debounce gating.
     *
     * Only 1 job enters the queue per debounce window. Subsequent calls
     * are no-ops — no closure, no queue entry, no worker overhead.
     */
    public static function debounce(mixed ...$params): void
    {
        /** @var static $instance */
        $instance = new static(...$params);

        $key = $instance->debounceKey();
        $delay = $instance->debounceDelay;
        $expiresAt = now()->addSeconds($delay)->getTimestamp();

        // Dispatch-time gating: check BEFORE queuing so only 1 job enters the queue
        // per debounce window. This cannot be a middleware — middleware runs after
        // the job is already queued and deserialized by a worker.
        $oldTimestamp = Redis::getset($key, $expiresAt);
        Redis::expire($key, $delay + self::BUFFER);

        // phpredis GETSET returns false (not null) when key didn't exist
        $noJobPending = $oldTimestamp === false;
        $previousJobCrashed = (int) $oldTimestamp <= now()->getTimestamp();
        $shouldQueue = $noJobPending || $previousJobCrashed;

        if (!$shouldQueue) return;

        dispatch($instance)->delay($delay);
    }

    /**
     * Laravel queue convention: if a job defines a middleware() method,
     * CallQueuedHandler applies it before handle(). No interface needed —
     * Laravel checks via method_exists(). Same pattern as WithoutOverlapping,
     * RateLimited, ThrottlesExceptions.
     *
     * Cleans up the Redis debounce key before the job executes,
     * opening the window for the next debounce cycle.
     *
     * If your job needs additional middleware, override this method
     * and merge:
     *
     *   public function middleware(): array
     *   {
     *       return [
     *           ...$this->debounceMiddleware(),
     *           new WithoutOverlapping($this->id),
     *       ];
     *   }
     *
     * @see \Illuminate\Queue\CallQueuedHandler::dispatchThroughMiddleware
     */
    public function middleware(): array
    {
        return $this->debounceMiddleware();
    }

    /**
     * Returns the debounce cleanup middleware. Extracted so jobs that
     * override middleware() can merge it with their own middleware.
     */
    protected function debounceMiddleware(): array
    {
        $cleanup = function ($job, $next) {
            Redis::del($this->debounceKey());
            $next($job);
        };

        return [$cleanup];
    }
}
