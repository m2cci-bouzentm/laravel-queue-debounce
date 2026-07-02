<?php

namespace Bouzentm\LaravelQueueDebounce\Tests;

use Illuminate\Support\Facades\Redis;
use Orchestra\Testbench\TestCase;

class DebounceableIntegrationTest extends TestCase
{
    private const KEYS = [
        'integration-test-debounce',
        'integration-test-debounce:A',
        'integration-test-debounce:B',
    ];

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanupKeys();
    }

    protected function tearDown(): void
    {
        $this->cleanupKeys();
        parent::tearDown();
    }

    private function cleanupKeys(): void
    {
        foreach (self::KEYS as $key) {
            Redis::del($key);
        }
    }

    public function test_first_getset_returns_false(): void
    {
        $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());

        $this->assertFalse($old);
    }

    public function test_key_holds_timestamp_after_first_call(): void
    {
        $expiresAt = now()->addSeconds(2)->getTimestamp();
        Redis::getset(self::KEYS[0], $expiresAt);

        $this->assertEquals($expiresAt, (int) Redis::get(self::KEYS[0]));
    }

    public function test_second_getset_returns_previous_timestamp(): void
    {
        Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());

        $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());

        $this->assertNotFalse($old);
        $this->assertGreaterThan(now()->getTimestamp(), (int) $old);
    }

    public function test_cleanup_then_redispatch(): void
    {
        Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());
        Redis::del(self::KEYS[0]);

        $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());

        $this->assertFalse($old);
    }

    public function test_different_keys_are_independent(): void
    {
        $oldA = Redis::getset(self::KEYS[1], now()->addSeconds(2)->getTimestamp());
        $oldB = Redis::getset(self::KEYS[2], now()->addSeconds(2)->getTimestamp());

        $this->assertFalse($oldA);
        $this->assertFalse($oldB);
        $this->assertNotNull(Redis::get(self::KEYS[1]));
        $this->assertNotNull(Redis::get(self::KEYS[2]));
    }

    public function test_crash_recovery_detects_expired_timestamp(): void
    {
        Redis::set(self::KEYS[0], now()->subSeconds(10)->getTimestamp());

        $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());

        $this->assertNotFalse($old);
        $this->assertLessThanOrEqual(now()->getTimestamp(), (int) $old);
        $this->assertGreaterThan(now()->getTimestamp(), (int) Redis::get(self::KEYS[0]));
    }

    public function test_100_rapid_calls_only_first_queues(): void
    {
        $queued = 0;

        for ($i = 0; $i < 100; $i++) {
            $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());
            Redis::expire(self::KEYS[0], 12);

            $noJobPending = $old === false;
            $crashed = $old !== false && (int) $old <= now()->getTimestamp();

            if ($noJobPending || $crashed) {
                $queued++;
            }
        }

        $this->assertEquals(1, $queued);
    }

    public function test_ttl_is_set(): void
    {
        Redis::getset(self::KEYS[0], now()->addSeconds(5)->getTimestamp());
        Redis::expire(self::KEYS[0], 15);

        $ttl = Redis::ttl(self::KEYS[0]);

        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(15, $ttl);
    }

    public function test_full_lifecycle(): void
    {
        // Dispatch
        $old = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());
        $this->assertFalse($old);

        // Duplicate detected as pending
        $old2 = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());
        $this->assertNotFalse($old2);
        $this->assertGreaterThan(now()->getTimestamp(), (int) $old2);

        // Execute + cleanup
        Redis::del(self::KEYS[0]);
        $this->assertNull(Redis::get(self::KEYS[0]));

        // Re-dispatch succeeds
        $old3 = Redis::getset(self::KEYS[0], now()->addSeconds(2)->getTimestamp());
        $this->assertFalse($old3);
    }
}
