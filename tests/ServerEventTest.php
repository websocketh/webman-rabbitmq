<?php

declare(strict_types=1);

namespace Workbunny\Tests;

use Bunny\ClientStateEnum;
use Workbunny\Tests\TestBuilders\TestPublishBuilder;
use Workbunny\WebmanRabbitMQ\Connection\ConnectionInterface;
use Workbunny\WebmanRabbitMQ\ConnectionsManagement;
use Workerman\Coroutine;
use Workerman\Timer;

/**
 * 服务端主动行为测试
 *
 * 结果导向：不验证中间状态，只验证 server 主动关闭连接后客户端能否正确恢复
 */
class ServerEventTest extends BaseTestCase
{
    private const TEST_QUEUE = 'process.workbunny.rabbitmq.TestPublishBuilder';

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        ConnectionsManagement::initialize();
    }

    protected function setUp(): void
    {
        parent::setUp();
        // 测试前清空队列，避免残留消息污染
        @$this->request(
            '/api/queues/' . rawurlencode($this->vhost) . '/' . rawurlencode(self::TEST_QUEUE) . '/contents',
            'DELETE'
        );
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        // 测试后移除队列
        @$this->request(
            '/api/queues/' . rawurlencode($this->vhost) . '/' . rawurlencode(self::TEST_QUEUE),
            'DELETE'
        );
    }

    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();
        ConnectionsManagement::destroy();
    }

    /**
     * 通过 HTTP API 关闭指定连接
     *
     * @param string $connectionId Connection::id() 的返回值，对应 client_properties 中的 client-id
     */
    protected function closeConnectionViaApi(string $connectionId): void
    {
        $connections = $this->listConnections();
        foreach ($connections as $conn) {
            $clientId = $conn['client_properties']['client-id']
                ?? $conn['client_properties']['connection_name']
                ?? null;
            if ($clientId === $connectionId) {
                // rawurlencode for path segments (spaces become %20, not +)
                @$this->request('/api/connections/' . rawurlencode($conn['name']), 'DELETE');

                return;
            }
        }
    }

    /**
     * 等待连接状态变化
     */
    protected function waitConnectionState(ConnectionInterface $connection, int $state, float $timeout = 5): bool
    {
        $start = microtime(true);
        while (microtime(true) - $start < $timeout) {
            if ($connection->getState() === $state) {
                return true;
            }
            Timer::sleep(0.1);
        }

        return false;
    }

    /**
     * 测试：server 关闭连接后，下一次 publish 能自动重连并成功
     *
     * 结果：断开前 publish 成功 → 断开 → 重连后 publish 成功 → 两条消息都在
     */
    public function testReconnectAndPublishAfterServerClose(): void
    {
        $builder = new TestPublishBuilder();

        // 断开前 publish
        $res1 = \Workbunny\WebmanRabbitMQ\publish($builder, 'before-close');
        $this->assertTrue($res1 > 0);

        // 获取当前连接并关闭
        $connection = ConnectionsManagement::get();
        $this->closeConnectionViaApi($connection->id());
        $this->waitConnectionState($connection, ClientStateEnum::ERROR, 5);
        ConnectionsManagement::release($connection);
        Timer::sleep(1);

        // 重连后 publish
        $res2 = \Workbunny\WebmanRabbitMQ\publish($builder, 'after-close');
        $this->assertTrue($res2 > 0, 'Publish should succeed after auto-reconnect');

        // 验证两条消息都在
        Timer::sleep(2);
        $messages = $this->getQueueMessages($builder->getBuilderConfig()->getQueue(), 2, true);
        $this->assertCount(2, $messages);
        $this->assertEquals('before-close', $messages[0]['payload']);
        $this->assertEquals('after-close', $messages[1]['payload']);
    }

    /**
     * 测试：并发 publish 时连接被断开，所有协程都正常结束（不挂死）
     *
     * 结果：Parallel::wait() 能正常返回 = 没有协程挂死
     */
    public function testConcurrentPublishNotHangAfterServerClose(): void
    {
        $builder = new TestPublishBuilder();

        // 先建立连接，拿到 connection id
        \Workbunny\WebmanRabbitMQ\publish($builder, 'warmup');
        $connection = ConnectionsManagement::get();
        $connId = $connection->id();
        ConnectionsManagement::release($connection);

        $completed = 0;
        $parallel = new Coroutine\Parallel();

        // 3 个协程并发 publish
        for ($i = 0; $i < 3; $i++) {
            $parallel->add(function () use ($builder, $i, &$completed) {
                try {
                    \Workbunny\WebmanRabbitMQ\publish($builder, "concurrent-{$i}");
                } catch (\Throwable) {
                    // 连接断开时可能抛异常，这是正常的
                }
                $completed++;
            });
        }

        // 第 4 个协程延迟关闭连接
        $parallel->add(function () use ($connId) {
            Timer::sleep(0.3);
            $this->closeConnectionViaApi($connId);
            Timer::sleep(1);
        });

        // 如果有协程挂死，wait() 会超时
        $parallel->wait();

        // 所有 publish 协程都结束了（成功或异常），没有挂死
        $this->assertEquals(3, $completed, 'All concurrent publishes should complete without hang');
    }
}
