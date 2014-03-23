<?php

require_once(__DIR__.'/PgSchemaVersionsDbInterface.php');
class PgSchemaVersionsYiiComponent extends CApplicationComponent implements PgSchemaVersionsDbInterface {
    protected $helper;
    public $dev_schema = 'develop';
    public $prod_schema = 'public';

    public function init()
    {
        require_once(__DIR__.'/PgSchemaVersionsHelper.php');
        $this->helper = new PgSchemaVersionsHelper($this);
        $this->helper->develop_schema = $this->dev_schema;
        $this->helper->product_schema = $this->prod_schema;
        parent::init();
    }

    /**
     * @return PgSchemaVersionsHelper
     */
    public function getHelper()
    {
        return $this->helper;
    }

    public function setProductMode()
    {
        $this->getHelper()->setProductMode();
    }

    public function setDevelopMode()
    {
        $this->getHelper()->setDevelopMode();
        require_once(__DIR__.'/PgSchemaVersionsYiiSwitchBehavior.php');
    }

    public function migrateUp()
    {
        require_once(__DIR__.'/PgSchemaVersionsInit.php');
        $migrate = new PgSchemaVersionsInit($this);
        $migrate->up($this->dev_schema);
    }

    public function migrateDown()
    {
        require_once(__DIR__.'/PgSchemaVersionsInit.php');
        $migrate = new PgSchemaVersionsInit($this);
        $migrate->down($this->dev_schema);
    }

    public function query($query, $params=array())
    {
        return Yii::app()->db->createCommand($query)->queryAll(true, $params);
    }

    public function execute($query, $params=array())
    {
        return Yii::app()->db->createCommand($query)->execute($params);
    }

} 