<?php
declare(strict_types=1);

namespace Examples;

use Bunny\Channel as BunnyChannel;
use Bunny\Async\Client as BunnyClient;
use Bunny\Message as BunnyMessage;
use Workbunny\WebmanRabbitMQ\Constants;
use Workbunny\WebmanRabbitMQ\FastBuilder;

class TestBuilder extends FastBuilder
{
    protected int $prefetch_size = 1;
    protected int $prefetch_count = 0;
    protected bool $is_global = false;


    public function handler(BunnyMessage $message, BunnyChannel $channel, BunnyClient $client): string
    {
        var_dump($message->content);
        return Constants::ACK;
        # Constants::NACK
        # Constants::REQUEUE
    }
}