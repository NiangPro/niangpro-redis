<?php

declare(strict_types=1);

namespace Niang\Core\Exceptions;

/** Connexion impossible, authentification refusée, ou erreur renvoyée par Redis (« -ERR ... »). */
class RedisException extends \RuntimeException
{
}
