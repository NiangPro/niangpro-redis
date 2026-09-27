<?php

declare(strict_types=1);

namespace Niang\Core\Queue;

use Niang\Core\Config;
use Niang\Core\Job;
use Niang\Core\Log;
use Niang\Core\Queue;
use Niang\Core\Redis;

/**
 * QUEUE_DRIVER=redis. Structures (clés préfixées par REDIS_PREFIX) :
 *  - jobs            hachage id => envelope sérialisée (base64)
 *  - queues          ensemble des noms de files connus
 *  - queues:<nom>    liste des ids prêts
 *  - queues:<nom>:delayed   ensemble trié, score = disponible à (timestamp)
 *  - queues:<nom>:reserved  ensemble trié, score = réservé jusqu'à (timestamp + retry_after)
 *  - failed_jobs     hachage id => envelope échouée
 * Les déplacements entre ces structures sont des scripts Lua : atomiques, même avec plusieurs workers
 * sur plusieurs machines. Un job réservé par un worker arrêté revient dans la file après retry_after.
 *
 * @internal utilisée par Niang\Core\Queue
 */
final class RedisQueue
{
    /** Déplace vers la file les jobs différés devenus disponibles et les réservations expirées. */
    private const MIGRATE = <<<'LUA'
        local moved = 0
        for _, source in ipairs({KEYS[2], KEYS[3]}) do
            local due = redis.call('ZRANGEBYSCORE', source, '-inf', ARGV[1])
            for _, id in ipairs(due) do
                redis.call('ZREM', source, id)
                redis.call('RPUSH', KEYS[1], id)
                moved = moved + 1
            end
        end
        return moved
        LUA;

    /** Retire un job de la file et le réserve, en une seule opération. */
    private const POP = <<<'LUA'
        local id = redis.call('LPOP', KEYS[1])
        if id then redis.call('ZADD', KEYS[2], ARGV[1], id) end
        return id
        LUA;

    /** @param array{id: string, queue: string, attempts: int, available_at: int, job: string} $envelope */
    public static function store(array $envelope): void
    {
        $queue = $envelope['queue'];
        Redis::command('HSET', self::key('jobs'), $envelope['id'], self::encode($envelope));
        Redis::command('SADD', self::key('queues'), $queue);

        if ($envelope['available_at'] > time()) {
            Redis::command('ZADD', self::key("queues:$queue:delayed"), $envelope['available_at'], $envelope['id']);
        } else {
            Redis::command('RPUSH', self::key("queues:$queue"), $envelope['id']);
        }
    }

    public static function work(?string $queue, int $baseBackoff): int
    {
        $processed = 0;
        $retryAfter = (int) Config::get('queue.retry_after', 600);

        foreach ($queue !== null ? [$queue] : self::queues() as $name) {
            $ready = self::key("queues:$name");
            $delayed = self::key("queues:$name:delayed");
            $reserved = self::key("queues:$name:reserved");

            Redis::connection()->eval(self::MIGRATE, [$ready, $delayed, $reserved], [time()]);

            // Seulement les jobs présents au départ : un job remis en file ne boucle pas indéfiniment.
            $count = (int) Redis::command('LLEN', $ready);

            for ($i = 0; $i < $count; $i++) {
                $id = Redis::connection()->eval(self::POP, [$ready, $reserved], [time() + $retryAfter]);

                if (!is_string($id)) {
                    break; // un autre worker a vidé la file
                }

                $envelope = self::decode(Redis::command('HGET', self::key('jobs'), $id));
                $job = $envelope !== null ? @unserialize((string) base64_decode($envelope['job'], true)) : null;

                if ($envelope === null || !$job instanceof Job) {
                    self::forget($id, $reserved);
                    continue;
                }

                $context = Queue::enterJobContext($job);

                try {
                    Queue::runJob($job);
                    $processed++;
                    self::forget($id, $reserved);
                    Queue::jobFinished($job, true);
                } catch (\Throwable $e) {
                    self::fail($envelope, $job, $e, $reserved, $delayed, $baseBackoff);
                } finally {
                    Queue::leaveJobContext($context);
                }
            }
        }

        return $processed;
    }

    /** @return list<array{id: string, queue: string, class: string, error: string, failed_at: string}> */
    public static function failed(): array
    {
        $failed = [];
        $all = (array) Redis::command('HGETALL', self::key('failed_jobs'));

        for ($i = 0; $i + 1 < count($all); $i += 2) {
            $envelope = self::decode($all[$i + 1]);

            if ($envelope === null) {
                continue;
            }

            $job = @unserialize((string) base64_decode($envelope['job'], true));
            $failed[] = [
                'id' => (string) $all[$i],
                'queue' => $envelope['queue'],
                'class' => $job instanceof Job ? $job::class : 'inconnu',
                'error' => (string) ($envelope['error'] ?? ''),
                'failed_at' => (string) ($envelope['failed_at'] ?? ''),
            ];
        }

        usort($failed, fn (array $a, array $b) => strcmp($a['failed_at'], $b['failed_at']));

        return $failed;
    }

    public static function retry(string $id): bool
    {
        $envelope = self::decode(Redis::command('HGET', self::key('failed_jobs'), $id));

        if ($envelope === null) {
            return false;
        }

        unset($envelope['error'], $envelope['failed_at']);
        $envelope['attempts'] = 0;
        $envelope['available_at'] = time();
        Redis::command('HDEL', self::key('failed_jobs'), $id);
        self::store($envelope);

        return true;
    }

    public static function flush(): int
    {
        $count = (int) Redis::command('HLEN', self::key('failed_jobs'));
        Redis::command('DEL', self::key('failed_jobs'));

        return $count;
    }

    public static function pending(): int
    {
        $total = 0;

        foreach (self::queues() as $name) {
            $total += (int) Redis::command('LLEN', self::key("queues:$name"))
                + (int) Redis::command('ZCARD', self::key("queues:$name:delayed"))
                + (int) Redis::command('ZCARD', self::key("queues:$name:reserved"));
        }

        return $total;
    }

    public static function reset(): void
    {
        $keys = [self::key('jobs'), self::key('failed_jobs'), self::key('queues')];

        foreach (self::queues() as $name) {
            array_push($keys, self::key("queues:$name"), self::key("queues:$name:delayed"), self::key("queues:$name:reserved"));
        }

        Redis::command('DEL', ...$keys);
    }

    /** @param array{id: string, queue: string, attempts: int, available_at: int, job: string} $envelope */
    private static function fail(array $envelope, Job $job, \Throwable $e, string $reserved, string $delayed, int $baseBackoff): void
    {
        $envelope['attempts']++;
        Queue::jobFinished($job, false);
        Log::error('Job échoué : ' . $e->getMessage(), ['job' => $job::class, 'attempts' => $envelope['attempts']]);
        Redis::command('ZREM', $reserved, $envelope['id']);

        if ($envelope['attempts'] >= $job->tries) {
            $envelope['error'] = $e->getMessage();
            $envelope['failed_at'] = date('Y-m-d H:i:s');
            Redis::command('HSET', self::key('failed_jobs'), $envelope['id'], self::encode($envelope));
            Redis::command('HDEL', self::key('jobs'), $envelope['id']);

            return;
        }

        $envelope['available_at'] = time() + $baseBackoff * (2 ** ($envelope['attempts'] - 1));
        Redis::command('HSET', self::key('jobs'), $envelope['id'], self::encode($envelope));
        Redis::command('ZADD', $delayed, $envelope['available_at'], $envelope['id']);
    }

    private static function forget(string $id, string $reserved): void
    {
        Redis::command('ZREM', $reserved, $id);
        Redis::command('HDEL', self::key('jobs'), $id);
    }

    /** @return list<string> */
    private static function queues(): array
    {
        $names = Redis::command('SMEMBERS', self::key('queues'));

        return is_array($names) ? array_values(array_map('strval', $names)) : [];
    }

    private static function key(string $name): string
    {
        return Redis::connection()->key($name);
    }

    private static function encode(array $envelope): string
    {
        return (string) json_encode($envelope);
    }

    /** @return array{id: string, queue: string, attempts: int, available_at: int, job: string, error?: string, failed_at?: string}|null */
    private static function decode(mixed $raw): ?array
    {
        $envelope = is_string($raw) ? json_decode($raw, true) : null;

        if (!is_array($envelope) || !isset($envelope['id'], $envelope['queue'], $envelope['job'])) {
            return null;
        }

        $decoded = [
            'id' => (string) $envelope['id'],
            'queue' => (string) $envelope['queue'],
            'attempts' => (int) ($envelope['attempts'] ?? 0),
            'available_at' => (int) ($envelope['available_at'] ?? 0),
            'job' => (string) $envelope['job'],
        ];

        foreach (['error', 'failed_at'] as $optional) {
            if (isset($envelope[$optional])) {
                $decoded[$optional] = (string) $envelope[$optional];
            }
        }

        return $decoded;
    }
}
