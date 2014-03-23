<?php


class PgSchemaVersionsYiiSwitchBehavior extends CActiveRecordBehavior {
    static $exclude = [];
    static $include_only = [];

    public function beforeSave($event)
    {
        $this->checkTable($event);
        return parent::beforeSave($event);
    }

    public function beforeDelete($event)
    {
        $this->checkTable($event);
        return parent::beforeDelete($event);
    }

    protected function checkTable($event)
    {
        $table = $event->sender->tableName();
        if (!empty(static::$include_only) && !in_array($table, static::$include_only)) {
            return;
        }
        if (in_array($table, static::$exclude)) {
            return;
        }
        Yii::app()->pg_versions->getHelper()->checkTable($table);
    }
} 
