<?php

declare(strict_types=1);

namespace Niang\Core\Redis;

use Niang\Core\Exceptions\RedisException;

/**
 * Client Redis minimal (protocole RESP2), écrit à la main : pas d'extension phpredis ni de bibliothèque.
 * Compatible Redis, Valkey, KeyDB, et les services gérés (Upstash, ElastiCache...) avec TLS (`tls://`).
 * Connexion à la première commande, puis AUTH et SELECT.
 *
 *   $redis->command('SET', 'clé', 'valeur', 'EX', '60');
 *   $redis->command('GET', 'clé');   // 'valeur', ou null
 */
final class Connection
{
    /** @var resource|null */
    private $socket = null;

    public function __construct(
        private string $host = '127.0.0.1',
        private int $port = 6379,
        private ?string $password = null,
        private ?string $username = null,
        private int $database = 0,
        private float $timeout = 2.0,
        private string $prefix = '',
    ) {
    }

    /** Envoie une commande et renvoie la réponse : string, int, null ou tableau (réponses imbriquées). */
    public function command(string|int|float ...$arguments): mixed
    {
        $this->connect();
        $this->write(self::encode(array_values($arguments)));

        return $this->readReply();
    }

    /** Script Lua exécuté atomiquement par Redis (EVAL) : aucune autre commande ne s'intercale. */
    public function eval(string $script, array $keys = [], array $arguments = []): mixed
    {
        return $this->command('EVAL', $script, (string) count($keys), ...array_map('strval', [...$keys, ...$arguments]));
    }

    /** Clé préfixée (REDIS_PREFIX) : plusieurs applications peuvent partager un même serveur. */
    public function key(string $key): string
    {
        return $this->prefix . $key;
    }

    public function prefix(): string
    {
        return $this->prefix;
    }

    public function disconnect(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    /** @param list<string|int|float> $arguments */
    public static function encode(array $arguments): string
    {
        $encoded = '*' . count($arguments) . "\r\n";

        foreach ($arguments as $argument) {
            $argument = (string) $argument;
            $encoded .= '$' . strlen($argument) . "\r\n" . $argument . "\r\n";
        }

        return $encoded;
    }

    private function connect(): void
    {
        if (is_resource($this->socket)) {
            return;
        }

        $address = str_contains($this->host, '://') ? "{$this->host}:{$this->port}" : "tcp://{$this->host}:{$this->port}";
        $socket = @stream_socket_client($address, $errno, $error, $this->timeout);

        if ($socket === false) {
            throw new RedisException("Connexion à Redis impossible ($address) : $error ($errno).");
        }

        stream_set_timeout($socket, (int) ceil(max(1.0, $this->timeout * 5)));
        $this->socket = $socket;

        try {
            if ($this->password !== null && $this->password !== '') {
                $this->write(self::encode($this->username !== null && $this->username !== '' ? ['AUTH', $this->username, $this->password] : ['AUTH', $this->password]));
                $this->readReply();
            }

            if ($this->database !== 0) {
                $this->write(self::encode(['SELECT', $this->database]));
                $this->readReply();
            }
        } catch (RedisException $e) {
            $this->disconnect();
            // Jamais le mot de passe dans le message.
            throw new RedisException('Redis : ' . $e->getMessage(), 0, $e);
        }
    }

    private function write(string $payload): void
    {
        $socket = $this->socket;

        if ($socket === null) {
            throw new RedisException('Redis : pas de connexion.');
        }

        for ($written = 0; $written < strlen($payload); $written += $bytes) {
            $bytes = @fwrite($socket, substr($payload, $written));

            if ($bytes === false || $bytes === 0) {
                $this->disconnect();
                throw new RedisException('Redis : connexion interrompue pendant l\'envoi.');
            }
        }
    }

    private function readReply(): mixed
    {
        $line = $this->readLine();
        $type = $line[0];
        $payload = substr($line, 1);

        return match ($type) {
            '+' => $payload,
            '-' => throw new RedisException($payload),
            ':' => (int) $payload,
            '$' => $payload === '-1' ? null : $this->readBytes((int) $payload),
            '*' => $payload === '-1' ? null : $this->readArray((int) $payload),
            default => throw new RedisException("Réponse Redis illisible : « $line »."),
        };
    }

    /** @return list<mixed> */
    private function readArray(int $count): array
    {
        return $count <= 0 ? [] : array_map(fn () => $this->readReply(), range(1, $count));
    }

    private function readLine(): string
    {
        $socket = $this->socket;
        $line = $socket === null ? false : fgets($socket);

        if ($line === false) {
            $this->disconnect();
            throw new RedisException('Redis : pas de réponse (connexion fermée ou délai dépassé).');
        }

        return rtrim($line, "\r\n");
    }

    private function readBytes(int $length): string
    {
        $socket = $this->socket;
        $data = '';

        while ($socket !== null && strlen($data) < $length + 2) {
            $chunk = fread($socket, max(1, $length + 2 - strlen($data)));

            if ($chunk === false || $chunk === '') {
                $this->disconnect();
                throw new RedisException('Redis : réponse tronquée.');
            }

            $data .= $chunk;
        }

        return substr($data, 0, $length);
    }
}
