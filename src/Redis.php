<?php

declare(strict_types=1);

namespace Niang\Core;

use Niang\Core\Redis\Connection;

/**
 * Connexion Redis partagée (config/redis.php), utilisée par les pilotes 'redis' du cache, des sessions,
 * de la file et de la limitation de débit, ou directement : Redis::command('INCR', 'visites').
 */
final class Redis
{
    private static ?Connection $connection = null;

    public static function connection(): Connection
    {
        return self::$connection ??= new Connection(
            (string) Config::get('redis.host', Env::get('REDIS_HOST', '127.0.0.1')),
            (int) Config::get('redis.port', Env::get('REDIS_PORT', 6379)),
            (string) Config::get('redis.password', Env::get('REDIS_PASSWORD', '')) ?: null,
            (string) Config::get('redis.username', Env::get('REDIS_USERNAME', '')) ?: null,
            (int) Config::get('redis.database', Env::get('REDIS_DB', 0)),
            (float) Config::get('redis.timeout', 2),
            (string) Config::get('redis.prefix', Env::get('REDIS_PREFIX', 'niangpro:')),
        );
    }

    /** Commande brute ; la clé n'est pas préfixée automatiquement (voir connection()->key()). */
    public static function command(string|int|float ...$arguments): mixed
    {
        return self::connection()->command(...$arguments);
    }

    /** @internal ferme la connexion (changement de configuration, tests). */
    public static function reset(): void
    {
        self::$connection?->disconnect();
        self::$connection = null;
    }
}
