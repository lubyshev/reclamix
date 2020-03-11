<?php

/**
 * Консольное приложение RedisTables.
 *
 * Оперции:
 *
 * synchronize : синхронизация всех таблиц с БД
 *
 * synchronize --table=[имя таблицы] : синхронизация конкретиной таблицы с БД
 *
 * stop : приостановка синхронизации
 *
 * resume : возобновление синхронизации
 *
 */
class RedisTablesCommand extends CConsoleCommand
{

    use TLoggedCommand;

    /**
     * Ключ приостановки
     *
     * @var string
     */
    const RK_STOP = 'cmd:redistables:stop';

    /**
     * Шаблоны комманд
     *
     * @var array
     */
    protected $cmd = [
        'base' => '/yiic redisTables synchronize --table=',
        'log'  => '/var/log/redis_{TABLE}_{DIFF}.log',
    ];

    /**
     * Коммандная строка для исполнения
     *
     * @return string
     */
    protected function getShellCommand($table)
    {
        $result = realpath(Yii::app()->basePath).$this->cmd ['base'].$table;
        if ($this->verbose) {
            $diff   = date('Y-m-d_H-i-s');
            $log    = preg_replace('~\{TABLE\}~', $table, $this->cmd ['log']);
            $log    = preg_replace('~\{DIFF\}~', $diff, $log);
            $result .= ' --verbose=1 >> '.$log.' 2>&1 &';
        } else {
            $result .= ' >> /dev/null 2>&1 &';
        }

        return $result;
    }

    /**
     * Возвращает количество запущенных процессов
     *
     * @return int
     */
    protected function processesCount($table)
    {
        $result = 0;
        $ps     = shell_exec('ps ax|grep "yiic redisTables synchronize --table='.$table.'"');
        $ps     = explode("\n", $ps);
        foreach ($ps as $pid) {
            if (preg_match(
                '~/protected/yiic redisTables synchronize --table='.$table.'~', $pid)) {
                $result++;
            }
        }

        return $result;
    }

    /**
     * Основной цикл
     *
     */
    protected function synchronize($tableName)
    {
        // Обработанных сессий
        $result = 0;
        $redis  = Yii::app()->redis->client();

        if (!$class = Yii::app()->redis->clientAR()->hGet('tblRedis:tables:list', $tableName)) {
            return;
        }

        $pids = $this->processesCount($tableName);
        $this->log('Pids = '.$pids);
        if (1 < $pids) {
            $this->log('Синхронизация таблицы уже запущена');
            exit();
        }
        $tm = microtime(true);
        $this->log("Синхронизация с БД");

        // Обработка
        /* @var $table CActiveRecordRedis */
        $table = new $class();
        $rows  = $table->dumpToDatabase();
        if (false === $rows) {
            $this->log("Таблица `{$tableName}` - запрос на дамп отклонен.");
        } elseif ($rows > 0) {
            $this->log(
                "Таблица `{$tableName}` - записей: {$rows}, время: "
                .number_format(microtime(true) - $tm, 3)." cек.");
            $result++;
        }
        $this->log("Окончание работы");
    }

    /**
     * Приостановлена ли cинхронизация.
     *
     * @return bool
     */
    public static function suspensed()
    {
        return (bool)Yii::app()->redis->client()->exists(self::RK_STOP);
    }

    /**
     * Синхронизирует данные с БД
     *
     * @param int  $limit   Сколько записей за одну итеррацию
     * @param bool $verbose "Разговорчивый" режим
     *
     * @return void
     */
    public function actionSynchronize($verbose = false, $table = false)
    {
        $this->verbose = (bool)$verbose;
        if (self::suspensed()) {
            $this->log([
                'Cинхронизация таблиц приостановлена.',
                'Для восстановления работы воспользуйтесь `resume`:',
                '   ./protected/yiic redistables resume --verbose',
            ]);

            return;
        }
        if ($table) {
            $this->synchronize($table);
        } else {
            /* @var $redis Redis */
            $redis = Yii::app()->redis->clientAR();
            if ($tables = $redis->hGetAll('tblRedis:tables:list')) {
                foreach ($tables as $table => $class) {
                    shell_exec($this->getShellCommand($table));
                }
            }
        }
    }

    /**
     * Остановка синхронизации
     *
     * @return void
     */
    public function actionStop()
    {
        Yii::app()->redis->client()->set(self::RK_STOP, 1);
        $this->log('Флаг приостановки синхронизации установлен');
    }

    /**
     * Возобновление синхронизации
     *
     * @return void
     */
    public function actionResume()
    {
        Yii::app()->redis->client()->del(self::RK_STOP);
        $this->log('Флаг приостановки синхронизации снят');
    }

}
