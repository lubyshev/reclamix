<?php
/*
 * Консольное приложение CampaignsCommand.
 *
 * Предназначено для отслеживания событий различных кампаний проекта.
 *
 */

/**
 * Консольное приложение CampaignsCommand.
 *
 */
class CampaignsCommand extends CConsoleCommand
{
    use TLoggedCommand;

    /**
     * Префикс ключа блокировки кампании
     *
     * @var string
     */
    const RKP_LOCK_CAMPAIGN = "campaigns_listener:locked:";

    /**
     * Сколько секунд ожидать скрипту перед попыткой создать новый поток
     *
     * @var int
     */
    const WAIT_FOR_THREAD = 3;

    /**
     * Текущая кампания
     *
     * @var CampaignsModel
     */
    protected $model = null;

    /**
     * Шаблоны комманд
     *
     * @var array
     */
    protected $cmd = [
        'base'    => '/yiic campaigns start',
        'log'     => '/var/log/campaigns.log',
        'verbose' => true,
    ];

    /**
     * Возвращает количество запущенных процессов
     *
     * @return int
     */
    protected function processesCount()
    {
        $result = 0;
        $ps     = shell_exec('ps ax|grep "yiic campaigns"');
        $ps     = explode("\n", $ps);
        foreach ($ps as $pid) {
            if (
            preg_match('~php [^\s]*/protected/yiic campaigns scan~', $pid)
            ) {
                $result++;
            }
        }

        return $result;
    }

    /**
     * Диспетчер событий кампаний. Сканирует и обрабатывает новые события.
     *
     * @param bool $verbose "Разговорчивый" режим
     *
     * @return void
     */
    public function actionScan($verbose = false)
    {
        $this->verbose = (bool)$verbose;
        $cntPrc        = $this->processesCount();
        if (1 < $cntPrc) {
            exit();
        }
        $this->log([
            '',
            'Старт диспетчера кампаний (процессов: '.$cntPrc.')',
            '=========================',
        ]);
        $this->log(['', 'Опрос активных кампаний']);
        $this->checkCampaigns();
        // Обработка кампаний
        $this->startCampaigns(500);
    }

    /**
     * Старт обработки кампании.
     *
     * @param string $campaign Имя исполняемой кампании.
     * @param int    $id       ID модели кампании.
     * @param int    $limit    Сколько записей обрабатывать за проход.
     * @param bool   $verbose  "Разговорчивый" режим.
     *
     * @return void
     */
    public function actionStart($campaign, $id, $limit, $verbose = false)
    {
        $this->verbose = (bool)$verbose;
        $this->log("{$campaign}. Запуск кампании.");

        if ($this->campaignLocked($campaign)) {
            $this->log($campaign.'. Задание уже запущено.');
        } else {
            $this->lockCampaign($campaign);
            try {
                $this->processCampaign($campaign, $id, $limit);
            } catch (Exception $e) {
                $this->log($campaign.'. Error: '.$e->getMessage().". Trace:\n".$e->getTraceAsString());
            }
            $this->unlockCampaign($campaign);
            $this->log($campaign.'. Окончание работы задания');
        }
    }

    public function actionRemoveProcessingFlags()
    {
        $this->verbose = true;
        /** @var  $redis Redis */
        $redis = Yii::app()->redis->client();
        $flags = $redis->keys(self::RKP_LOCK_CAMPAIGN.'*');
        foreach ($flags as $flag) {
            $redis->del($flag);
            $this->log("Флаг исполнения '{$flag}' снят.");
        }
    }

    /**
     * Основной цикл проверки событий
     *
     * @param int $limit Сколько записей за один пуск
     *
     * @return void
     */
    protected function startCampaigns($limit)
    {
        // Получаем кампании для исполнения
        $campaigns = CampaignsModel::model()->activeList()->findAll();
        foreach ($campaigns as $campaign) {
            $result = realpath(Yii::app()->basePath)
                .$this->cmd ['base']
                .' --campaign='.$campaign->model
                .' --id='.$campaign->id
                .' --limit='.$limit;
            if ($this->verbose) {
                $log    = preg_replace('~\{DIFF\}~',
                    date('Y-m-d_H-i-s'),
                    $this->cmd ['log']);
                $result .= ' --verbose=1 >> '.$log.' 2>&1 &';
            } else {
                $result .= ' >> /dev/null 2>&1 &';
            }
            shell_exec($result);
            $this->log('    '.$result);
        }
    }

    /**
     * Блокирует кампанию
     *
     * @param string $campaign
     *
     * @return void
     */
    protected function lockCampaign($campaign)
    {
        Yii::app()->redis->client()->set(self::RKP_LOCK_CAMPAIGN.$campaign, 1);
    }

    /**
     * Снимает блок с кампании
     *
     * @param string $campaign
     *
     * @return void
     */
    protected function unlockCampaign($campaign)
    {
        Yii::app()->redis->client()->del(self::RKP_LOCK_CAMPAIGN.$campaign);
    }

    /**
     * Блокирована ли кампания
     *
     * @param string $campaign
     *
     * @return bool
     */
    protected function campaignLocked($campaign)
    {
        return (bool)Yii::app()->redis->client()->exists(self::RKP_LOCK_CAMPAIGN.$campaign);
    }

    /**
     * Просроченные и удаленные кампании
     *
     * @return void
     */
    protected function checkCampaigns()
    {
        $redis = Yii::app()->redis->client();
        if ($redis->exists(self::RKP_LOCK_CAMPAIGN.'check:expired')) {
            return;
        }
        $redis->setEx(self::RKP_LOCK_CAMPAIGN.'check:expired', 600, 1);

        // Приостанавливаем просроченные
        $resultExpired = $this->processExpiredCampaigns();
        if ($resultExpired) {
            $this->log('Завершено кампаний: '.$resultExpired);
        }

        // Удаляем данные закрытых кампаний
        $resultFinished = $this->processFinishedCampaigns();
        if ($resultFinished) {
            $this->log('Удалено кампаний: '.$resultFinished);
        }
    }

    /**
     * Обрабатывает события текущей кампании
     *
     * @param int $limit
     *
     * @return void
     */
    protected function processCampaign($className, $id, $limit)
    {
        // Проверяем партнеров на возникновение нового события
        $resultNew = $this->processNewEvents($className, $id, $limit);
        if ($resultNew) {
            $this->log($className.' Новых партнеров кампаний: '.$resultNew);
        }
        $limit -= (int)$resultNew;
        if (0 >= $limit) {
            return;
        }

        // Проверяем партнеров на возникновение регулярных событий
        $resultRegular = $this->processRegularEvents($className, $id, $limit);
        if ($resultRegular) {
            $this->log($className.'. Обработано событий: '.$resultRegular);
        }
    }

    /**
     * Установка кампаний с истекшим сроком действия в неактивный режим
     *
     * @return int Количество обработанных записей или FALSE в случае ошибки
     */
    protected function processExpiredCampaigns()
    {
        $result = 0;
        // Ищем просроченные кампании
        $items = CampaignsModel::model()
            ->expired()
            ->findAll();
        if (!empty($items)) {
            foreach ($items as $item) {
                $className = $item->model;
                if ($className::confirmExpireCampaign($item)) {
                    $log          = "Перевод кампании({$item->id}) в неактивные: ";
                    $item->active = 'false';
                    if ($item->save()) {
                        $result++;
                        $this->log($log.'OK');
                    } else {
                        $this->log($log.'Неудачная попытка');
                    }
                } else {
                    $this->log("Перевод кампании({$item->id}) в неактивные отменен из модели `{$className}`");
                }
            }
        }

        return $result;
    }

    /**
     * Очистка таблиц от данных закрытых кампаний.
     *
     * @return int Количество обработанных записей или FALSE в случае ошибки
     */
    protected function processFinishedCampaigns()
    {
        $result = 0;
        // Удаляем кампании помеченные на удаление
        $items = CampaignsModel::model()
            ->toDelete()
            ->findAll();
        if (!empty($items)) {
            foreach ($items as $item) {
                $className = $item->model;
                if ($className::confirmDeleteCampaign($item)) {
                    $log = "Очистка данных кампании({$item->id}): ";
                    if ($item->delete()) {
                        $result++;
                        $this->log($log.'OK');
                    } else {
                        $this->log($log.'Неудачная попытка');
                    }
                } else {
                    $this->log("Очистка данных кампании({$item->id}) отменена из модели `{$item->model}`");
                }
            }
        }

        return $result;
    }

    /**
     * Проверка новых событий партнеров.
     *
     * @param string $className
     * @param int    $id
     * @param int    $limit
     *
     * @return int Количество обработанных записей или FALSE в случае ошибки
     */
    protected function processNewEvents($className, $id, $limit)
    {
        $result = 0;
        if (0 < $limit) {
            $class = new $className();
            $items = CampaignPartnersModel::model()
                ->newPartners()
                ->findAll([
                    'condition' => 'campaign_id = '.$id,
                    'limit'     => $limit,
                ]);
            if (!empty($items)) {
                foreach ($items as $item) {
                    /* @var $model ICampaign */
                    if ($model = $class->findByPk($item->subject_id)) {
                        $model->partnerInit($item);
                    } else {
                        $this->log("Не найден объект `{$className}` по ключу {$item->subject_id}. CampaignPartner.id = {$item-> id}.");
                        $item->delete();
                    }
                    $result++;
                }
            }
        }

        return $result;
    }

    /**
     * Проверка регулярных событий партнеров.
     *
     * @param string $className
     * @param int    $id
     * @param int    $limit
     *
     * @return int Количество обработанных записей или FALSE в случае ошибки
     */
    protected function processRegularEvents($className, $id, $limit)
    {
        $result = 0;
        if (0 < $limit) {
            $class = new $className();
            $items = CampaignPartnersModel::model()
                ->readyPartners()
                ->findAll([
                    'condition' => 'campaign_id = '.$id,
                    'limit'     => $limit,
                ]);
            if (!empty($items)) {
                foreach ($items as $item) {
                    /* @var $model ICampaign */
                    if ($model = $class->findByPk($item->subject_id)) {
                        $model->partnerEvent($item);
                    } else {
                        $this->log("Не найден объект `{$className}` по ключу {$item->subject_id}. CampaignPartner.id = {$item-> id}.");
                        $item->delete();
                    }
                    $result++;
                }
            }
        }

        return $result;
    }

}
