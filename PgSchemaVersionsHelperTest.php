<?php

/**
 * Class PgSchemaVersionsHelperTest
 * Test for Yii
 * Use from test folder (phpunit ../extensions/pg_versions/PgSchemaVersionsHelperTest.php
 */
class PgSchemaVersionsHelperTest extends CDbTestCase {

    protected function setUp()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;
        $prod = Yii::app()->pg_versions->getHelper()->product_schema;
        $db->createCommand("DROP TABLE IF EXISTS {$dev}.pgschematest")->execute();
        $db->createCommand("DROP TABLE IF EXISTS {$prod}.pgschematest")->execute();
        $db->createCommand("CREATE TABLE {$prod}.pgschematest(id integer, c2 integer, c3 text, CONSTRAINT pgschematest_pkey PRIMARY KEY (id))")->execute();
        $db->createCommand("TRUNCATE TABLE {$dev}.develop_audit")->execute();
        $db->createCommand("INSERT INTO {$prod}.pgschematest VALUES (1, 1, '1'), (2, 2, '2'), (3, 3, '3')")->execute();
    }

    public function testCreateDevelopTable()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->assertEquals(
            [
                ['id' => '1', 'c2' => '1', 'c3' => '1'],
                ['id' => '2', 'c2' => '2', 'c3' => '2'],
                ['id' => '3', 'c2' => '3', 'c3' => '3'],
            ],
            $db->createCommand("SELECT * FROM {$dev}.pgschematest ORDER BY id")->queryAll()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testIsTableInDevelop()
    {
        $this->assertFalse($this->callMethod('isTableInDevelop', ['pgschematest']));
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->assertTrue($this->callMethod('isTableInDevelop', ['pgschematest']));
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testTriggersInsert()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);

        $db->createCommand("INSERT INTO {$dev}.pgschematest VALUES (4, 4, '4')")->execute();
        $this->assertEquals(
            '1',
            $db->createCommand("SELECT COUNT(*) FROM {$dev}.develop_audit WHERE tablename='pgschematest' AND pkey='4' AND operation='INSERT'")->queryScalar()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testTriggersUpdate()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);

        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 99 WHERE id=3")->execute();
        $this->assertEquals(
            '1',
            $db->createCommand("SELECT COUNT(*) FROM {$dev}.develop_audit WHERE tablename='pgschematest' AND pkey='3' AND operation='UPDATE'")->queryScalar()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testTriggersDelete()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);

        $db->createCommand("DELETE FROM {$dev}.pgschematest WHERE id=2")->execute();
        $this->assertEquals(
            '1',
            $db->createCommand("SELECT COUNT(*) FROM {$dev}.develop_audit WHERE tablename='pgschematest' AND pkey='2' AND operation='DELETE'")->queryScalar()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testGetDevelopTableList()
    {
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->assertEquals(
            ['pgschematest'],
            $this->callMethod('getDevelopTableList')
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testGetTableFields()
    {
        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->assertEquals(
            ['id', 'c2', 'c3'],
            $this->callMethod('getTableFields', ['pgschematest'])
        );
    }

    public function testSyncDelete()
    {
        $db = Yii::app()->db;
        $prod = Yii::app()->pg_versions->getHelper()->product_schema;
        $this->callMethod('syncDelete', ['pgschematest', [1,2]]);
        $this->assertEquals(
            [
                ['id' => '3', 'c2' => '3', 'c3' => '3'],
            ],
            $db->createCommand("SELECT * FROM {$prod}.pgschematest ORDER BY id")->queryAll()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testSyncInsert()
    {
        $db = Yii::app()->db;
        $prod = Yii::app()->pg_versions->getHelper()->product_schema;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;

        $this->callMethod('createDevelopTable', ['pgschematest']);
        $db->createCommand("INSERT INTO {$dev}.pgschematest VALUES (4, 4, '4'), (5, 5, '5')")->execute();

        $res = $this->callMethod('syncInsert', ['pgschematest', [3,4,5]]);

        $this->assertEquals(
            [4, 5],
            $res
        );

        $this->assertEquals(
            [
                ['id' => '1', 'c2' => '1', 'c3' => '1'],
                ['id' => '2', 'c2' => '2', 'c3' => '2'],
                ['id' => '3', 'c2' => '3', 'c3' => '3'],
                ['id' => '4', 'c2' => '4', 'c3' => '4'],
                ['id' => '5', 'c2' => '5', 'c3' => '5'],
            ],
            $db->createCommand("SELECT * FROM {$prod}.pgschematest ORDER BY id")->queryAll()
        );
    }

    /**
     * @depends testCreateDevelopTable
     */
    public function testSyncUpdate()
    {
        $db = Yii::app()->db;
        $prod = Yii::app()->pg_versions->getHelper()->product_schema;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;

        $this->callMethod('createDevelopTable', ['pgschematest']);
        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 99 WHERE id=3")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c3 = 88 WHERE id=2")->execute();

        $this->callMethod('syncUpdate', ['pgschematest', [3,2]]);
        $this->assertEquals(
            [
                ['id' => '1', 'c2' => '1', 'c3' => '1'],
                ['id' => '2', 'c2' => '2', 'c3' => '88'],
                ['id' => '3', 'c2' => '99', 'c3' => '3'],
            ],
            $db->createCommand("SELECT * FROM {$prod}.pgschematest ORDER BY id")->queryAll()
        );
    }

    /**
     * @depends testTriggersDelete
     * @depends testTriggersInsert
     * @depends testTriggersUpdate
     */
    public function testSyncAnalyse()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;

        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);

        $db->createCommand("DELETE FROM {$dev}.pgschematest WHERE id=1")->execute();
        $db->createCommand("INSERT INTO {$dev}.pgschematest VALUES (4, 4, '4'), (5, 5, '5'), (6, 6, '6')")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 99 WHERE id=3")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c3 = 88 WHERE id=2")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 44 WHERE id=4")->execute();
        $db->createCommand("DELETE FROM {$dev}.pgschematest WHERE id=6")->execute();

        $res = $this->callMethod('syncAnalyse', ['pgschematest']);
        $this->assertEquals(
            [
                1 => 0,
                2 => 1,
                3 => 1,
                4 => 1,
                5 => 1,
                6 => 0,
            ],
            $res
        );
    }

    /**
     * @depends testSyncAnalyse
     * @depends testSyncDelete
     * @depends testSyncInsert
     * @depends testSyncUpdate
     */
    public function testSyncTable()
    {
        $db = Yii::app()->db;
        $prod = Yii::app()->pg_versions->getHelper()->product_schema;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;

        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);

        $db->createCommand("DELETE FROM {$dev}.pgschematest WHERE id=1")->execute();
        $db->createCommand("INSERT INTO {$dev}.pgschematest VALUES (4, 4, '4'), (5, 5, '5'), (6, 6, '6')")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 99 WHERE id=3")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c3 = 88 WHERE id=2")->execute();
        $db->createCommand("UPDATE {$dev}.pgschematest SET c2 = 44 WHERE id=4")->execute();
        $db->createCommand("DELETE FROM {$dev}.pgschematest WHERE id=6")->execute();

        $this->callMethod('syncToProduct', ['pgschematest']);
        $this->assertEquals(
            [
                ['id' => '2', 'c2' => '2', 'c3' => '88'],
                ['id' => '3', 'c2' => '99', 'c3' => '3'],
                ['id' => '4', 'c2' => '44', 'c3' => '4'],
                ['id' => '5', 'c2' => '5', 'c3' => '5'],
            ],
            $db->createCommand("SELECT * FROM {$prod}.pgschematest ORDER BY id")->queryAll()
        );
    }

    /**
     * @depends testCreateDevelopTable
     * @depends testIsTableInDevelop
     * @depends testTriggersInsert
     */
    public function testDropDevelopTable()
    {
        $db = Yii::app()->db;
        $dev = Yii::app()->pg_versions->getHelper()->develop_schema;

        $this->callMethod('createDevelopTable', ['pgschematest']);
        $this->callMethod('triggersOn', ['pgschematest']);
        $db->createCommand("INSERT INTO {$dev}.pgschematest VALUES (4, 4, '4'), (5, 5, '5'), (6, 6, '6')")->execute();

        $this->callMethod('dropDevelopTable', ['pgschematest']);
        $this->assertFalse($this->callMethod('isTableInDevelop', ['pgschematest']));
        $this->assertEquals(
            '0',
            $db->createCommand("SELECT COUNT(*) FROM {$dev}.develop_audit WHERE tablename='pgschematest'")->queryScalar()
        );
    }

    protected static function callMethod($name, array $args=[]) {
        $helper = Yii::app()->pg_versions->getHelper();
        $class = new \ReflectionClass($helper);
        $method = $class->getMethod($name);
        $method->setAccessible(true);
        return $method->invokeArgs($helper, $args);
    }
} 