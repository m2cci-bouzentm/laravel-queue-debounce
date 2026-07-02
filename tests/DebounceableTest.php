<?php

namespace Bouzentm\LaravelQueueDebounce\Tests;

use Bouzentm\LaravelQueueDebounce\Debounceable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Redis;
use Orchestra\Testbench\TestCase;

class TestDebouncedJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels, Debounceable;

    public static int $executionCount = 0;

    public function __construct(public int $entityId)
    {
        $this->debounceDelay = 2;
    }

    protected function debounceKey(): string
    {
        return 'test-debounce:' . $this->entityId;
    }

    public function handle(): void
    {
        static::$executionCount++;
    }
}

class DebounceableTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        TestDebouncedJob::$executionCount = 0;

        foreach (Redis::keys('test-debounce:*') as $key) {
            Redis::del($key);
        }
    }

    protected function tearDown(): void
    {
        foreach (Redis::keys('test-debounce:*') as $key) {
            Redis::del($key);
        }
        parent::tearDown();
    }

    private function simulateExecution(string $key, object $instance): void
    {
        Redis::del($key);
        $instance->handle();
    }

    // --- Queue behavior ---

    public function test_first_call_sets_redis_key(): void
    {
        Queue::fake();
        $this->assertNull(Redis::get('test-debounce:123'));

        TestDebouncedJob::debounce(123);

        $this->assertNotNull(Redis::get('test-debounce:123'));
    }

    public function test_subsequent_calls_do_not_queue(): void
    {
        Queue::fake();
        TestDebouncedJob::debounce(123);
        TestDebouncedJob::debounce(123);

        $this->assertNotNull(Redis::get('test-debounce:123'));
    }

    public function test_10_calls_keep_key_alive(): void
    {
        Queue::fake();
        for ($i = 0; $i < 10; $i++) {
            TestDebouncedJob::debounce(123);
        }

        $this->assertNotNull(Redis::get('test-debounce:123'));
    }

    public function test_different_params_get_separate_keys(): void
    {
        Queue::fake();
        TestDebouncedJob::debounce(123);
        TestDebouncedJob::debounce(456);

        $this->assertNotNull(Redis::get('test-debounce:123'));
        $this->assertNotNull(Redis::get('test-debounce:456'));
    }

    public function test_queues_once_per_debounce_window(): void
    {
        Queue::fake();

        for ($i = 0; $i < 10; $i++) {
            TestDebouncedJob::debounce(123);
        }
        $this->assertNotNull(Redis::get('test-debounce:123'));

        Redis::del('test-debounce:123');
        $this->assertNull(Redis::get('test-debounce:123'));

        TestDebouncedJob::debounce(123);
        $this->assertNotNull(Redis::get('test-debounce:123'));
    }

    public function test_cleans_up_redis_key_after_execution(): void
    {
        Queue::fake();
        TestDebouncedJob::debounce(777);
        $this->assertNotNull(Redis::get('test-debounce:777'));

        Redis::del('test-debounce:777');
        $this->assertNull(Redis::get('test-debounce:777'));
    }

    public function test_new_debounce_after_cleanup_works(): void
    {
        Queue::fake();
        TestDebouncedJob::debounce(123);
        $this->assertNotNull(Redis::get('test-debounce:123'));

        Redis::del('test-debounce:123');

        TestDebouncedJob::debounce(123);
        $this->assertNotNull(Redis::get('test-debounce:123'));
    }

    public function test_different_entity_ids_are_independent(): void
    {
        Queue::fake();
        TestDebouncedJob::debounce(100);
        TestDebouncedJob::debounce(200);
        TestDebouncedJob::debounce(300);

        $this->assertNotNull(Redis::get('test-debounce:100'));
        $this->assertNotNull(Redis::get('test-debounce:200'));
        $this->assertNotNull(Redis::get('test-debounce:300'));
    }

    public function test_crash_recovery_queues_new_job(): void
    {
        Queue::fake();
        Redis::set('test-debounce:999', now()->subSeconds(10)->getTimestamp());

        TestDebouncedJob::debounce(999);

        $stored = (int) Redis::get('test-debounce:999');
        $this->assertGreaterThan(now()->getTimestamp(), $stored);
    }

    // --- Execution behavior ---

    public function test_executes_only_once_when_called_10_times(): void
    {
        Queue::fake();

        for ($i = 0; $i < 10; $i++) {
            TestDebouncedJob::debounce(123);
        }

        $this->simulateExecution('test-debounce:123', new TestDebouncedJob(123));

        $this->assertEquals(1, TestDebouncedJob::$executionCount);
    }

    public function test_executes_twice_when_called_with_different_params(): void
    {
        Queue::fake();

        for ($i = 0; $i < 5; $i++) {
            TestDebouncedJob::debounce(123);
        }
        for ($i = 0; $i < 5; $i++) {
            TestDebouncedJob::debounce(456);
        }

        $this->simulateExecution('test-debounce:123', new TestDebouncedJob(123));
        $this->simulateExecution('test-debounce:456', new TestDebouncedJob(456));

        $this->assertEquals(2, TestDebouncedJob::$executionCount);
    }

    public function test_executes_twice_across_two_windows(): void
    {
        Queue::fake();

        for ($i = 0; $i < 10; $i++) {
            TestDebouncedJob::debounce(123);
        }
        $this->simulateExecution('test-debounce:123', new TestDebouncedJob(123));
        $this->assertEquals(1, TestDebouncedJob::$executionCount);

        for ($i = 0; $i < 2; $i++) {
            TestDebouncedJob::debounce(123);
        }
        $this->simulateExecution('test-debounce:123', new TestDebouncedJob(123));
        $this->assertEquals(2, TestDebouncedJob::$executionCount);
    }
}
