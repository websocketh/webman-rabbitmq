<?php declare(strict_types=1);
/**
 * @author workbunny/Chaz6chez
 * @email chaz6chez1993@outlook.com
 */

namespace Workbunny\WebmanRabbitMQ\Clients;

use Bunny\Channel;
use Bunny\Channel as OriginalChannel;
use Bunny\ChannelStateEnum;
use Bunny\ClientStateEnum;
use Bunny\Exception\ClientException;
use Bunny\Protocol\MethodChannelOpenOkFrame;
use Bunny\Protocol\MethodConnectionStartFrame;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use Workbunny\WebmanRabbitMQ\Channels\Channel as CurrentChannel;

trait ClientMethods
{
    /**
     * @return OriginalChannel[]|CurrentChannel[]
     */
    public function getChannels(): array
    {
        return $this->channels;
    }

    /**
     * 获取一个可用的通道
     *
     * @return CurrentChannel|PromiseInterface
     */
    public function catchChannel(): CurrentChannel|PromiseInterface
    {
        $resChannel = null;
        // 从已创建的频道中获取一个可用的频道
        $channels = $this->getChannels();
        foreach ($channels as $channel) {
            if ($channel->getState() === ChannelStateEnum::READY) {
                $resChannel = $channel;
                break;
            }
        }
        // 如果没有可用的频道，则创建一个新频道
        if (!$resChannel) {
            $channelId = $this->findChannelId();
            $this->channels[$channelId] = new CurrentChannel($this, $channelId);
            $response = $this->channelOpen($channelId);
            if ($response instanceof MethodChannelOpenOkFrame) {
                return $this->channels[$channelId];

            } elseif ($response instanceof PromiseInterface) {
                return $response->then(function () use ($channelId) {
                    return $this->channels[$channelId];
                });
            } else {
                $this->state = ClientStateEnum::ERROR;
                throw new ClientException(
                    "channel.open unexpected response of type " . gettype($response) .
                    (is_object($response) ? "(" . get_class($response) . ")" : "") .
                    "."
                );
            }
        }
        return ($this instanceof SyncClient)
            ? $resChannel
            : new Promise(function () use ($resChannel) {
                return $resChannel;
            });
    }

    /**
     * 重写authResponse方法
     *  1. 支持PLAIN及AMQPLAIN两种机制
     *
     * @param MethodConnectionStartFrame $start
     * @return bool|PromiseInterface
     */
    abstract protected function authResponse(MethodConnectionStartFrame $start): PromiseInterface|bool;

    /**
     * 回收
     */
    abstract protected function __destruct();

}