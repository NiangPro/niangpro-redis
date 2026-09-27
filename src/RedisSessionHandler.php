<?php

declare(strict_types=1);

namespace Niang\Core;

/**
 * SESSION_DRIVER=redis : une clé par session, expirée par Redis lui-même (pas de nettoyage à faire),
 * partagée entre tous les serveurs web.
 */
final class RedisSessionHandler implements \SessionHandlerInterface
{
    public function __construct(private int $lifetimeSeconds)
    {
    }

    public function open(string $path, string $name): bool
    {
        return true;
    }

    public function close(): bool
    {
        return true;
    }

    public function read(string $id): string
    {
        $data = Redis::command('GET', $this->key($id));

        return is_string($data) ? $data : '';
    }

    public function write(string $id, string $data): bool
    {
        return Redis::command('SET', $this->key($id), $data, 'EX', max(1, $this->lifetimeSeconds)) === 'OK';
    }

    public function destroy(string $id): bool
    {
        Redis::command('DEL', $this->key($id));

        return true;
    }

    public function gc(int $max_lifetime): int
    {
        return 0; // expiration gérée par Redis (EX)
    }

    private function key(string $id): string
    {
        return Redis::connection()->key('session:' . $id);
    }
}
