<?php
declare(strict_types=1);

namespace Workbunny\WebmanRabbitMQ\Commands;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use function Workbunny\WebmanRabbitMQ\config;
use function Workbunny\WebmanRabbitMQ\config_path;
use function Workbunny\WebmanRabbitMQ\base_path;
use function Workbunny\WebmanRabbitMQ\is_empty_dir;

class WorkbunnyWebmanRabbitMQRemove extends AbstractCommand
{
    protected static $defaultName        = 'workbunny:rabbitmq-remove';
    protected static $defaultDescription = 'Remove a workbunny/webman-rabbitmq Builder. ';

    /**
     * @return void
     */
    protected function configure(): void
    {
        $this->setName('workbunny:rabbitmq-remove')
            ->setDescription('Remove a workbunny/webman-rabbitmq Builder. ');
        $this->addArgument('name', InputArgument::REQUIRED, 'builder name.');
        $this->addOption('delayed', 'd', InputOption::VALUE_NONE, 'Delayed mode.');
        $this->addOption('close', 'c', InputOption::VALUE_NONE, 'Close only mode.');
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $name = $input->getArgument('name');
        $delayed = $input->getOption('delayed');
        $close = $input->getOption('close');
        list($name, $namespace, $file) = $this->getFileInfo($name, $delayed);
        $file = $close ? '' : $file;
        if(!file_exists($process = config_path() . '/plugin/workbunny/webman-rabbitmq/process.php')) {
            return $this->error($output, "Builder {$name} failed to remove: plugin/workbunny/webman-rabbitmq/process.php does not exist.");
        }
        // remove config
        $config = config('plugin.workbunny.webman-rabbitmq.process', []);
        $processName = str_replace('\\', '.', "$namespace\\$name");
        if(isset($config[$processName])){
            if(file_put_contents($process, preg_replace_callback("/    '$processName' => [[\s\S]*?],\r\n/",
                    function () {
                        return '';
                    }, file_get_contents($process),1)
            ) !== false) {
                $this->info($output, "Config updated.");
            }
        }
        // remove file
        if(file_exists($file)){
            unlink($file);
            $this->info($output, "Builder removed.");
        }
        // remove empty dir
        if(dirname($file) !== base_path() . DIRECTORY_SEPARATOR . self::$baseProcessPath) {
            is_empty_dir(dirname($file), true);
            $this->info($output, "Empty dir removed.");
        }
        return $this->success($output, "Builder $name removed successfully.");
    }

}
