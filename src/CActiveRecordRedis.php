<?php
/**
 * Для отладки, раскомментируйте строку,
 * и можете использовать функцию ::log()
 * при отладке в консольной комманде
 */
// define( 'DBG_CARR', true );

/**
 * Класс для кеширования таблицы в редис.
 * Использовать на уровне одной записи.
 *
 * ВНИМАТЕЛЬНО! Переписаны не все методы CActiveRecord, возможны накладки.
 * Особое внимание требуется при использовании find-методов.
 *
 * ВНИМАТЕЛЬНО! Не реализованы scopes, relations и conditions.
 *
 */
abstract class CActiveRecordRedis extends CActiveRecord
{
    /**
     * Оперции с индексами. Вычесть индекс.
     *
     * @var string
     */
    const IOP_DIFF = 'diff';

    /**
     * Оперции с индексами. Пересечение с индексом.
     *
     * @var string
     */
    const IOP_INTER = 'intersect';

    /**
     * Оперции с индексами. Начальный индекс в списке.
     *
     * НЕ УСТАНАВЛИВАЙТЕ НАПРЯМУЮ:
     * тк установится автоматически для первого индекса в списке.
     *
     * @var string
     */
    const IOP_PRIMARY = 'primary';

    /**
     * Оперции с индексами. Объединить с индексом.
     *
     * @var string
     */
    const IOP_UNION = 'union';

    /**
     * afterSave() уже отработал
     *
     * @var bool
     */
    protected $afterSaveExecuted = false;

    /**
     * beforeSave() уже отработал
     *
     * @var bool
     */
    protected $beforeSaveExecuted = false;

    /**
     * Запись сейчас присутствует в Redis
     *
     * @var bool
     */
    public $inRedis = false;

    /**
     * Старые значения индекируемых полей
     *
     * @var array
     */
    protected $oldIndexes = [];

    /**
     * Используемые индексы.
     *
     * @var array
     */
    protected $indexesInUse = [];

    /**
     * Вермя последнего обновления записи в Redis
     *
     * @var int|bool
     */
    public $updatedRedis = false;

    /**
     * Сколько микросекунд ожидать скрипту после дампа записи
     *
     * @var int
     */
    protected $dumpRowSleep = 1500;

    protected static function log($str)
    {
        if (!defined('DBG_CARR')) {
            return;
        }
        if (isset(Yii::app()->command)) {
            Yii::app()->command->log($str);
        }
    }

    public function __construct($scenario = 'insert')
    {
        $key = $this->prefix().'tables:list';
        $this->redis()->hSet($key, $this->tableName(), get_class($this));
        parent::__construct($scenario);
    }

    /**
     * Возвращает индексы для Redis.<br />
     * Массив c парами: <br />
     *   имя индекса => массив с именами полей, по котрым будет осуществлятся индексирование,<br />
     *   или индекса => callback( $row )
     *
     * [<br />
     *   'primary' => 'id',<br />
     *   'secondary' => ['uid', 'dt'],<br />
     *   'third' => function( CActiveRecordRedis $row ) { return (string) $indexValue; }<br />
     * ]
     *
     * @return array
     */
    protected function redisIndexes()
    {
        return [];
    }

    /**
     * Процедуры после дампа записи в БД
     *
     * @return void
     */
    protected function afterDump()
    {
        // Если запись в кеше "просрочена"
        if ($this->rowExpiredRedis()) {
            $this->deleteRedis();
        }
    }

    /**
     * Запись в кеше "просрочена" и скорее всего не доживет до следующего дампа в БД.
     *
     * @return bool
     */
    public function rowExpiredRedis()
    {
        $result = false;
        // Если запись в кеше
        if ($this->inRedis) {
            // Если установлено время жизни записи
            if ($this->getRowTTL()) {
                // Окончание жизни в кеше
                $expired = time() + (int)$this->redis()->ttl($this->getRowKeyRedis());
                // "Гарантированно" синхронизируется
                $nextSync = $this->synchronized() + (int)$this->getExpiredDumpScale() * $this->getDumpTimeout();
                if ($expired < $nextSync) {
                    $result = true;
                }
            }
        }

        return $result;
    }

    /**
     * This method is invoked after each record is instantiated by a find method.
     * The default implementation raises the {@link onAfterFind} event.
     * You may override this method to do postprocessing after each newly found record is instantiated.
     * Make sure you call the parent implementation so that the event is raised properly.
     */
    protected function afterFind()
    {
        parent::afterFind();
        // Определим, существет ли запись в Redis
        if (!$this->inRedis) {
            $this->inRedis = (bool)$this->redis()->exists($this->getRowKeyRedis());
        }
        $this->afterFindRedis();
    }

    public function afterFindRedis()
    {
        $this->oldIndexes = $this->getAllIndexesRedis();
    }

    /**
     * Обновляет все индексы текущей записи
     *
     * @return void
     */
    protected function updateIndexesRedis()
    {
        $newIndexes = $toSet = $this->getAllIndexesRedis();
        $toDelete   = [];
        if (!empty($this->oldIndexes)) {
            foreach ($toSet as $k => $index) {
                if ($this->oldIndexes [$k] === $index) {
                    if ($this->redis()->exists($this->getIndexKeyRedis($k, $index))) {
                        unset($toSet [$k]);
                    }
                } else {
                    $toDelete [$k] = $this->oldIndexes [$k];
                }
            }
        }
        if (!empty($toDelete)) {
            $this->deleteIndexesRedis($toDelete);
        }
        if (!empty($toSet)) {
            $this->setIndexesRedis($toSet);
        }
        $this->oldIndexes = $newIndexes;
    }

    /**
     * Удаляет индексы Redis
     *
     * @param array $indexes Массив индексов типа [имя=>значение]
     *
     * @return void
     */
    protected function deleteIndexesRedis($indexes)
    {
        $pk = $this->arrayToStringsRedis($this->getPrimaryKeyRedis());
        foreach ($indexes as $name => $index) {
            $key = $this->getIndexKeyRedis($name, $index);
            $this->redis()->srem($key, $pk [1]);
        }
    }

    /**
     * Переводит массив со значениями вида [ key1 => value1, key2 => value2, ...]
     * к массиву с одним сводным значением [ keys => values ]
     *
     * @param array $array
     *
     * @return array
     */
    protected function arrayToStringsRedis($array)
    {
        if (!is_array($array) || empty($array)) {
            return null;
        }
        $keys = [];
        $vals = [];
        foreach ($array as $k => $v) {
            $keys [] = $this->asString($k);
            $vals [] = $this->asString($v);
        }

        return [
            implode($this->getFieldsDelimiter(), $keys),
            implode($this->getFieldsDelimiter(), $vals),
        ];
    }

    /**
     * Устанавливает индексы Redis
     *
     * @param array $indexes Массив индексов типа [имя=>значение]
     *
     * @return void
     */
    protected function setIndexesRedis($indexes)
    {
        $pk = $this->arrayToStringsRedis($this->getPrimaryKeyRedis());
        foreach ($indexes as $name => $index) {
            $key = $this->getIndexKeyRedis($name, $index);
            $this->redis()->sAdd($key, $pk [1]);
        }
    }

    /**
     * Очищает текущие установки операции над индексами,
     * для нового использования ::findWithIndexesRedis().
     *
     * @return void
     */
    public function clearIndexesRedis()
    {
        $this->indexesInUse = [];
    }

    /**
     * Добавляет индекс для ::findWithIndexesRedis()
     *
     * @param string $indexName      Имя индекса из ::redisIndexes()
     * @param string $indexOperation Операция с индексом IOP_UNION, IOP_INTER, IOP_DIFF
     *
     * @return boolean
     */
    public function useIndexRedis($indexName, $indexOperation = self::IOP_INTER)
    {
        $indexes = $this->redisIndexes();
        if (!array_key_exists($indexName, $indexes)) {
            throw new CDbException("Redis-индекс `{$indexName}` не определен.");
        }
        if (empty($this->indexesInUse)) {
            $this->indexesInUse [$indexName] = [
                'index'     => $indexName,
                'operation' => self::IOP_PRIMARY,
            ];
        } else {
            if (self::IOP_PRIMARY === $indexOperation) {
                throw new CDbException("Первичная операция `IOP_PRIMARY` уже определена.");
            }
            $this->indexesInUse [$indexName] = [
                'index'     => $indexName,
                'operation' => $indexOperation,
            ];
        }

        return true;
    }

    /**
     * Ищет записи по индексам
     *
     * @param array $values   Значения индексов для поиска
     * @param bool  $oneByOne Возвращать по одной записи за раз (yield)
     *
     * Индексы:
     *
     * ['primary' => 'id'. 'secondary' => ['uid', 'dt'], ...]
     *
     * Значения:
     *
     * ['primary' => ['id' => id], 'secondary' => [ 'uid' => uid, 'dt' => dt], ...]
     *
     * @return CActiveRecordRedis|null
     *
     * @throws CDbException
     */
    public function findWithIndexesRedis($values, $oneByOne = false)
    {
        // Если нет текущих индексов
        if (empty($this->indexesInUse)) {
            return null;
        }
        $result       = [];
        $indexes      = $this->redisIndexes();
        $indexesInUse = $this->indexesInUse;
        // Первый индекс
        $primary = array_shift($indexesInUse);
        if (is_callable($indexes [$primary ['index']])) {
            $fKey = $this->getIndexKeyRedis($primary ['index'], $values [$primary ['index']]);
        } else {
            $fKey = $this->getIndexKeyRedis($indexes [$primary ['index']], $values [$primary ['index']]);
        }

        // Если задан только один индекс
        if (empty($indexesInUse)) {
            foreach ($this->redis()->sMembers($fKey) as $pk) {
                if ($model = $this->findByPkRedis($pk)) {
                    if ($oneByOne) {
                        yield $model;
                    } else {
                        $result [] = $model;
                    }
                } else {
                    $this->redis()->sRem($fKey, $pk);
                }
            }
            // Если несколько индексов
        } else {
            // Сгенерируем временное хранилище для результатов
            // операций с индексами
            $tmpKey = '';
            do {
                $tmpKey = md5($tmpKey.microtime());
            } while (!$this->redis()->setnx('temp:tblRedis:'.$tmpKey, 1));
            $tmpSetKey = 'temp:tblRedis:indexop:'.$tmpKey;

            $count = 0;
            foreach ($indexesInUse as $index) {
                $sKey = $this->getIndexKeyRedis($indexes [$index ['index']], $values [$index ['index']]);
                switch ($index ['operation']) {
                    case self::IOP_UNION:
                        $count = (int)$this->redis()->sUnionStore($tmpSetKey, $fKey, $sKey);
                        break;
                    case self::IOP_INTER:
                        $count = (int)$this->redis()->sInterStore($tmpSetKey, $fKey, $sKey);
                        break;
                    case self::IOP_DIFF:
                        $count = (int)$this->redis()->sDiffStore($tmpSetKey, $fKey, $sKey);
                        break;
                    default :
                        throw new CDbException("Неизвестная операция `{$index ['operation']}`.");
                        break;
                }
                $fKey = $tmpSetKey;
            } // END OF foreach $indexesInUse
            // Если есть результаты
            if (0 < $count) {
                foreach ($this->redis()->sMembers($tmpSetKey) as $pk) {
                    if ($model = $this->findByPkRedis($pk)) {
                        if ($oneByOne) {
                            yield $model;
                        } else {
                            $result [] = $model;
                        }
                    }
                }
            }

            // Почистим за собой
            $this->redis()->del($tmpSetKey);
            $this->redis()->del('temp:tblRedis:'.$tmpKey);
        } // END OF Если несколько индексов

        return empty($result) ? null : $result;
    }

    /**
     * Возвращает все индексы текущей записи
     *
     * @return array
     */
    protected function getAllIndexesRedis()
    {
        $result = [];
        foreach ($this->redisIndexes() as $name => $fields) {
            $value = null;
            if (!is_array($fields) && is_callable($fields)) {
                $field = $name;
                $value = $fields($this);
            } else {
                list($field, $value) = $this->getIndexRedis($fields);
            }
            if ($field && $value) {
                // Результат
                $result [$field] = $value;
            }
        }

        return $result;
    }

    /**
     * Возвращает [key,value] значение для индекса.
     *
     * @param mixed $fields Имя поля или массив с именами полей индекса
     *
     * @param mixed $values Значение поля или массив со значениями полей индекса.
     *                      Если параметр не определен, то значения будут взяты из текущей записи.
     *
     * @return array
     */
    protected function getIndexRedis($fields, $values = null)
    {
        if (!is_array($fields)) {
            $fields = [$fields];
        }
        if (null !== $values && !is_array($values)) {
            $values = [$values];
        }
        if (!$values) {
            $values = [];
            foreach ($fields as $field) {
                if (array_key_exists($field, $this->attributes)) {
                    $values [$field] = $this->{$field};
                } else {
                    throw new CDbException("Поле `$field` не определено.");
                }
            }
        }

        return $this->arrayToStringsRedis($values);
    }

    /**
     * This method is invoked after saving a record successfully.
     * The default implementation raises the {@link onAfterSave} event.
     * You may override this method to do postprocessing after record saving.
     * Make sure you call the parent implementation so that the event is raised properly.
     */
    protected function afterSave()
    {
        parent::afterSave();
        $this->afterSaveExecuted = true;
    }

    /**
     * This method is invoked before saving a record (after validation, if any).
     * The default implementation raises the {@link onBeforeSave} event.
     * You may override this method to do any preparation work for record saving.
     * Use {@link isNewRecord} to determine whether the saving is
     * for inserting or updating record.
     * Make sure you call the parent implementation so that the event is raised properly.
     *
     * @return boolean whether the saving should be executed. Defaults to true.
     */
    protected function beforeSave()
    {
        $result                   = parent::beforeSave();
        $this->beforeSaveExecuted = true;

        return $result;
    }

    /**
     * Saves the current record.
     *
     * The record is inserted as a row into the database table if its {@link isNewRecord}
     * property is true (usually the case when the record is created using the 'new'
     * operator). Otherwise, it will be used to update the corresponding row in the table
     * (usually the case if the record is obtained using one of those 'find' methods.)
     *
     * Validation will be performed before saving the record. If the validation fails,
     * the record will not be saved. You can call {@link getErrors()} to retrieve the
     * validation errors.
     *
     * If the record is saved via insertion, its {@link isNewRecord} property will be
     * set false, and its {@link scenario} property will be set to be 'update'.
     * And if its primary key is auto-incremental and is not set before insertion,
     * the primary key will be populated with the automatically generated key value.
     *
     * @param boolean $runValidation whether to perform validation before saving the record.
     *                               If the validation fails, the record will not be saved to database.
     * @param array   $attributes    list of attributes that need to be saved. Defaults to null,
     *                               meaning all attributes that are loaded from DB will be saved.
     * @param bool    $force         Force save to DB
     *
     * @return boolean whether the saving succeeds
     */
    public function save($runValidation = true, $attributes = null, $force = false)
    {
        if ($force) {
            $new    = $this->isNewRecord;
            $result = parent::save($runValidation, $attributes);
            // Если запись есть в Redis
            if ($result && $this->inRedis) {
                // Считаем автозаполняющиеся поля
                if ($new) {
                    $this->refresh();
                }
                $result &= $this->saveRedis(false, null, false);
            }
        } else {
            $result = $this->saveRedis($runValidation, $attributes, true);
        }

        return $result;
    }

    /**
     * Возвращает ключ Redis для признака удаления
     *
     * @param mixed $pk Простой/составной PK. Если задан - ключ будет вычислен из значений PK.
     *                  Иначе ключ будет вычислен из текущих значений.
     *
     * @return string
     */
    protected function getRowKeyDeleted($pk = null)
    {
        $keys = $this->getPrimaryKeyRedis($pk);
        $val  = is_array($keys) ? implode($this->getFieldsDelimiter(), $keys) : $keys;

        return $this->prefix().'{'.$this->tableName().'}:deleted:'.$val;
    }

    /**
     * Возвращает ключ Redis для записи по первичному ключу
     *
     * @param mixed $pk Простой/составной PK. Если задан - ключ будет вычислен из значений PK.
     *                  Иначе ключ будет вычислен из текущих значений.
     *
     * @return string
     */
    protected function getRowKeyRedis($pk = null)
    {
        $keys = $this->getPrimaryKeyRedis($pk);
        $val  = is_array($keys) ? implode($this->getFieldsDelimiter(), $keys) : $keys;

        return $this->prefix().'{'.$this->tableName().'}:row:'.$val;
    }

    /**
     * Возвращает ключ Redis для значения индекса
     *
     * @param string $indexName  Имя индекса
     * @param string $indexValue Значение индекса
     *
     * @return string
     */
    protected function getIndexKeyRedis($indexName, $indexValue)
    {
        return $this->prefix().'{'.$this->tableName().'}:index:'.$indexName.':'.$indexValue;
    }

    /**
     * Возвращает все значения индекса
     *
     * @param string $indexName Имя индекса
     *
     * @return string
     */
    public static function getIndexValues($indexName)
    {
        $result = null;
        $prefix = static::model()->prefix().'{'.static::model()->tableName().'}:index:'.$indexName.':';
        if ($keys = static::model()->redis()->keys($prefix.'*')) {
            foreach ($keys as $key) {
                $result [] = preg_replace("~$prefix(.*)~", '$1', $key);
            }
        }

        return $result;
    }

    /**
     * Finds a single active record with the specified primary key.
     * See {@link find()} for detailed explanation about $condition and $params.
     *
     * @param mixed $pk        primary key value(s). Use array for multiple primary keys. For composite key, each key value must be an array (column name=>column value).
     * @param mixed $condition query condition or criteria.
     * @param array $params    parameters to be bound to an SQL statement.
     *
     * @return static the record found. Null if none is found.
     */
    public function findByPk($pk, $condition = '', $params = [])
    {
        $result = null;
        if ($this->primaryKeyRedis() === $this->primaryKey()) {
            $result = $this->findByPkRedis($pk);
        }
        if (!$result) {
            if ($result = parent::findByPk($pk, $condition, $params)) {
                $result->saveRedis(false, null, false);
            }
        }

        return $result;
    }

    /**
     * Возвращает все записи из кеша
     *
     * @param bool $oneByOne Возвращать по одной записи за раз (yield)
     *
     * @return array
     */
    public function findAllRedis($oneByOne = false)
    {
        $result = [];
        $tpl    = $this->prefix().'{'.$this->tableName().'}:row:*';
        $rKeys  = $this->redis()->keys($tpl);
        foreach ($rKeys as $key) {
            if ($data = $this->redis()->get($key)) {
                $class = get_class($this);
                /* @var $model CActiveRecordRedis */
                $pk = $class::model()->getPrimaryKeyRedis($data ['fields']);
                if ($model = $class::model()->findByPkRedis($pk)) {
                    if ($oneByOne) {
                        yield $model;
                    } else {
                        $result [] = $model;
                    }
                }
            } // if $data
        } // foreach $rKeys

        return empty($result) ? null : $result;
    }

    /**
     * Ищет по первичному ключу в Redis
     *
     * @param mixed $pk
     *
     * @return CActiveRecordRedis
     */
    public function findByPkRedis($pk)
    {
        $result = null;
        if (!$this->isDeleted()) {
            $key = $this->getRowKeyRedis($pk);
            if ($data = $this->redis()->get($key)) {
                /* @var $class CActiveRecordRedis */
                $class                = get_class($this);
                $result               = new $class(null);
                $result->isNewRecord  = $data ['isNewRecord'];
                $result->inRedis      = true;
                $result->updatedRedis = $data ['updated'];
                foreach ($data ['fields'] as $fld => $val) {
                    if (array_key_exists($fld, $this->attributes)) {
                        $result->{$fld} = $val;
                    }
                }
                $result->afterFindRedis();
            }
        }

        return $result;
    }

    /**
     * Помещает запись в Redis.
     * Внимание! Нет никаких проверок!
     * Проверять надо ДО примениеия функции.
     *
     * @return void
     */
    protected function _toRedis()
    {
        $key  = $this->getRowKeyRedis();
        $data = [
            'isNewRecord' => $this->isNewRecord,
            'updated'     => $this->updatedRedis,
            'fields'      => $this->attributes,
        ];
        if ($to = (int)$this->getRowTTL()) {
            $this->redis()->setEx($key, $to, $data);
        } else {
            $this->redis()->set($key, $data);
        }
        $this->updateIndexesRedis();
        $this->inRedis = true;
    }

    /**
     * Сохранение записи в redis
     *
     * @param boolean $runValidation whether to perform validation before saving the record.
     *                               If the validation fails, the record will not be saved to database.
     *
     * @param array   $attributes    list of attributes that need to be saved. Defaults to null,
     *                               meaning all attributes that are loaded from DB will be saved.
     *
     * @param boolean $updated       Флаг изменения записи
     *
     * @return boolean
     */
    public function saveRedis($runValidation = true, $attributes = null, $updated = true)
    {
        // Если запись удаляется в этот момент
        if ($this->isDeleted()) {
            return false;
        }

        // Валидация
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }

        // Событие beforeSave (если еще не отработало)
        if (!$this->beforeSaveExecuted && !$this->beforeSave()) {
            return false;
        }

        $this->updatedRedis = (bool)$updated;
        $this->_toRedis();

        // Событие afterSave  (если еще не отработало)
        if (!$this->afterSaveExecuted) {
            $this->afterSave();
        }

        $this->afterSaveExecuted  = false;
        $this->beforeSaveExecuted = false;

        return true;
    }

    /**
     * Возвращает таймаут, по истечении которого изменения вносятся в бд.
     * Если 0, то дамп не происходит никогда.
     *
     * @return int
     */
    protected function getDumpTimeout()
    {
        return 0;
    }

    /**
     * Возвращает множитель для таймаута дампа, который используется
     * при определении "просроченности" записи ::rowExpiredRedis()
     *
     * @return float
     */
    protected function getExpiredDumpScale()
    {
        return 2;
    }

    /**
     * Возвращает ttl для каждой записи.
     * Если 0, то записывается без ограничения времени жизни.
     *
     * @return int
     */
    protected function getRowTTL()
    {
        return 0;
    }

    /**
     * Возвращает строковое представление значения поля
     *
     * @return string
     */
    protected function asString($value)
    {
        $result = '';
        if (false === $value) {
            $result = '@FALSE@';
        } elseif (true === $value) {
            $result = '@TRUE@';
        } elseif (null === $value) {
            $result = '@NULL@';
        } else {
            $result .= $value;
        }

        return $result;
    }

    /**
     * Возвращает префикс для ключей в кеше
     *
     * @return string
     */
    protected function prefix()
    {
        return 'tblRedis:';
    }

    /**
     * Возвращает разделитель полей для помещения в кеш.
     *
     * @return string
     */
    protected function getFieldsDelimiter()
    {
        return '_';
    }

    /**
     * Имя поля или массив с именами полей комбинированного ключа,
     * по которому осуществляется идентификация записи в Redis
     *
     * @return mixed
     */
    protected function primaryKeyRedis()
    {
        $pk = $this->primaryKey();
        if (!$pk) {
            $table = $this->getTableSchema();
            if (is_array($table->primaryKey)) {
                $pk = [];
                foreach ($table->primaryKey as $name) {
                    $pk [] = $name;
                }
            } else {
                $pk = $table->primaryKey;
            }
        }

        return $pk;
    }

    /**
     * Возвращает значение первичного ключа Redis
     *
     * @param mixed $fields
     *    Массив полей записи или значение первичного ключа.
     *    Если параметр не задан - ключ будет вычислен из текущих значений.
     *
     * @return array
     */
    public function getPrimaryKeyRedis($fields = null)
    {
        $keys     = [];
        $pkFields = $this->primaryKeyRedis();
        if ($fields) {
            // Обычный PK
            if (!is_array($pkFields)) {
                // Если PK передан как значение
                if (!is_array($fields)) {
                    $keys [$pkFields] = $this->asString($fields);
                    // Если PK передан в массиве
                } else {
                    $keys [$pkFields] = $this->asString($fields [$pkFields]);
                }
                // Составной PK
            } else {
                foreach ($pkFields as $field) {
                    if (!is_array($fields) || !array_key_exists($field, $fields)) {
                        return null;
                        // throw new CDbException("Отсутствует поле `{$field}` для формирования первичного ключа.");
                    } else {
                        $keys [$field] = $this->asString($fields [$field]);
                    }
                }
            }
        } else {
            // Обычный PK
            if (!is_array($pkFields)) {
                $keys [$pkFields] = $this->asString($this->{$pkFields});
                // Составной PK
            } else {
                foreach ($pkFields as $field) {
                    $keys [$field] = $this->asString($this->{$field});
                }
            }
        }

        return empty($keys) ? null : $keys;
    }

    /**
     * Deletes the row corresponding to this active record.
     *
     * @return boolean whether the deletion is successful.
     */
    public function delete()
    {
        $result = true;
        // Внимание, если запись новая,
        // то НЕ БУДЕТ вызвано исключение
        if (!$this->isNewRecord) {
            $this->setDeletedRedis();
            $result = parent::delete();
        }
        $this->deleteRedis();
        if (!$this->isNewRecord) {
            $this->unsetDeletedRedis();
        }

        return $result;
    }

    /**
     * Удаляет запись из Redis
     *
     * @return void
     */
    public function deleteRedis()
    {
        if ($this->inRedis) {
            $this->redis()->del($this->getRowKeyRedis());
            $this->deleteIndexesRedis($this->oldIndexes);
            $this->inRedis = false;
        }
    }

    /**
     * Возвращает признак "запись удалена"
     *
     * @return bool
     */
    protected function isDeleted()
    {
        $key = $this->getRowKeyDeleted();

        return $this->redis()->get($key) ? true : false;
    }

    /**
     * Устанавливает признак "запись удалена"
     *
     * @return void
     */
    protected function setDeletedRedis()
    {
        $key = $this->getRowKeyDeleted();
        if ($to = (int)$this->getRowTTL()) {
            $this->redis()->setEx($key, $to, '1');
        } else {
            $this->redis()->set($key, '1');
        }
    }

    /**
     * Убирает признак "запись удалена"
     *
     * @return void
     */
    protected function unsetDeletedRedis()
    {
        $key = $this->getRowKeyDeleted();
        $this->redis()->delete($key);
    }

    /**
     * Выгрузка в БД текущей записи
     *
     * @return bool
     */
    public function dump()
    {
        $result = false;
        // Если были изменения - пишем в БД
        if ($this->updatedRedis) {
            $this->save(true, null, true);
            $result = true;
        }
        $this->afterDump();

        return $result;
    }

    /**
     * Генератор строк для выгрузки в БД таблицы
     *
     * @return yield
     */
    protected function getRowsToDump()
    {
        $tpl   = $this->prefix().'{'.$this->tableName().'}:row:*';
        $rKeys = $this->redis()->keys($tpl);
        $class = get_class($this);
        foreach ($rKeys as $key) {
            if ($data = $this->redis()->get($key)) {
                /* @var $model CActiveRecordRedis */
                $pk    = $class::model()->getPrimaryKeyRedis($data ['fields']);
                $model = $class::model()->findByPkRedis($pk);
                yield $model;
            }
        }
    }

    /**
     * Выгрузка в БД таблицы
     *
     * @param int $maxExecutionTime Масксимальное время исполнения
     *
     * @return int Количество выгруженных записей или FALSE в случае неудачи
     */
    public function dumpToDatabase($maxExecutionTime = false)
    {
        if (!static::readyForDump()) {
            return false;
        }
        $tm     = time();
        $result = 0;
        $toKey  = $this->prefix().'{'.$this->tableName().'}:table:timeout';
        $to     = (int)$this->getDumpTimeout();
        if ($to && !$this->redis()->get($toKey)) {
            // Все записи в кеше
            $rows = $this->getRowsToDump();
            foreach ($rows as $model) {
                if (!$model) {
                    continue;
                }
                if ($model->dump()) {
                    $result++;
                }
                if ($maxExecutionTime && time() - $tm > (int)$maxExecutionTime) {
                    break;
                }
                usleep($this->dumpRowSleep);
            } // foreach $rKeys
            $this->redis()->setEx($toKey, $to, 1);
        }
        $this->synchronized(time());

        return $result;
    }

    /**
     * Готовность таблицы к дампу
     *
     * @return boolean
     */
    protected static function readyForDump()
    {
        return true;
    }

    /**
     * Возвращает (и устанавливает) время последней синхронизации таблицы
     *
     * @param int $time Если FALSE - новое значение не устанавливается.
     *
     * @return int Время последней синхронизации.
     *             Если задан $time - вернет значение ДО установки нового значения.
     */
    public function synchronized($time = false)
    {
        /**
         * Последняя синхронизация таблицы с кешем
         *
         * @var int
         */
        static $synchronized = -1;
        $syncKey = false;
        if (0 > $synchronized) {
            $syncKey = $this->prefix().'{'.$this->tableName().'}:table:synchronized';
            if (!$synchronized = (int)$this->redis()->get($syncKey)) {
                $synchronized = 0;
            }
        }
        $result = $synchronized;
        if (false !== $time) {
            $synchronized = (int)$time;
            $syncKey || $syncKey = $this->prefix().'{'.$this->tableName().'}:table:synchronized';
            $this->redis()->set($syncKey, $synchronized);
        }

        return $result;
    }

    /**
     * Возвращает клиент для доступа к redis
     *
     * @return Redis
     */
    protected function redis()
    {
        return Yii::app()->redis->clientAR();
    }

}
