<?php


class PgSchemaVersionsHelper {
    public $product_schema = 'public';
    public $develop_schema = 'develop';

    /** @var  PgSchemaVersionsDbInterface */
    protected $db;

    public function __construct(PgSchemaVersionsDbInterface $db)
    {
        $this->db = $db;
    }

    /**
     * Is table_name presents in develop schema or not
     * @param string $table_name
     * @return bool
     */
    protected function isTableInDevelop($table_name)
    {
        $q = $this->query("SELECT * FROM pg_tables WHERE tablename = ? AND schemaname = ?", [$table_name, $this->develop_schema]);
        return isset($q[0]);
    }

    /**
     * Test if shema exists or not, create if not
     */
    protected function checkDevelopSchema($sub = false)
    {
        static $checked = false;

        if ($checked === true) {
            return;
        }

        $q = $this->query("SELECT * FROM pg_namespace WHERE nspname = ?", [$this->develop_schema]);
        if (!isset($q[0])) {
            if ($sub) {
                throw new Exception('Develop schema lost!');
            } else {
                require_once(__DIR__.'/PgSchemaVersionsInit.php');
                $migrate = new PgSchemaVersionsInit($this->db);
                $migrate->up($this->develop_schema);
                $this->checkDevelopSchema(true);
            }
        }

        $checked = true;
    }

    /**
     * Create copy of table in develop schema
     * @param $table_name
     */
    protected function createDevelopTable($table_name)
    {
        $this->execute("CREATE TABLE \"{$this->develop_schema}\".\"{$table_name}\" (LIKE \"{$this->product_schema}\".\"{$table_name}\" INCLUDING ALL)");
        $this->execute("INSERT INTO \"{$this->develop_schema}\".\"{$table_name}\" SELECT * FROM \"{$this->product_schema}\".\"{$table_name}\"");
    }

    /**
     * Trigger for track changes
     * @param $table_name
     */
    protected function triggersOn($table_name)
    {
        $this->execute("CREATE TRIGGER \"t_{$table_name}\" BEFORE INSERT OR UPDATE OR DELETE ON "
            ."\"{$this->develop_schema}\".\"{$table_name}\" FOR EACH ROW EXECUTE PROCEDURE \"{$this->develop_schema}\".develop_audit_trigger()");
    }

    /**
     * If table not exists in develop schema - create it and fill with data
     * @param $table_name
     * @throws Exception
     */
    public function checkTable($table_name)
    {
        $this->transaction('begin');
        try {
            $this->checkDevelopSchema();
            if (!$this->isTableInDevelop($table_name)) {
                $this->createDevelopTable($table_name);
                $this->triggersOn($table_name);
            }
            $this->transaction('commit');
        } catch (Exception $e) {
            $this->transaction('rollback');
            throw $e;
        }
    }

    public function setProductMode()
    {
        $this->execute("SET search_path = ".$this->product_schema);
    }

    public function setDevelopMode()
    {
        $this->checkDevelopSchema();
        $this->execute("SET search_path = ".$this->develop_schema.", ".$this->product_schema);
    }

    /**
     * List of tables in dev schema
     * @return array
     */
    protected function getDevelopTableList()
    {
        $q = $this->query("SELECT * FROM pg_tables WHERE tablename != 'develop_audit' AND schemaname = ?", [$this->develop_schema]);
        $tables = [];
        foreach($q as $i) {
            $tables[] = $i['tablename'];
        }
        return $tables;
    }

    /**
     * Move date from dev to prod and drop all dev data
     * @param $table_name
     * @throws Exception
     */
    public function syncToProduct($table_name)
    {
        $this->transaction('begin');

        try {
            $keys = $this->syncAnalyse($table_name);

            $opers = ['DELETE' => [], 'UPDATE' => []];
            foreach ($keys as $id=>$state) {
                if (!$state) {
                    $opers['DELETE'][] = $id;
                } else {
                    $opers['UPDATE'][] = $id;
                }
            }

            $this->syncDelete($table_name, $opers['DELETE']);

            $used = $this->syncInsert($table_name, $opers['UPDATE']);

            $this->syncUpdate($table_name, array_diff($opers['UPDATE'], $used));

            $this->dropDevelopTable($table_name);

            $this->transaction('commit');
        } catch (Exception $e) {
            $this->transaction('rollback');
            throw $e;
        }
    }

    /**
     * Drop table
     * @param $table_name
     */
    protected function dropDevelopTable($table_name)
    {
        $this->execute("DELETE FROM  \"{$this->develop_schema}\".develop_audit WHERE tablename = ?", [$table_name]);
        $this->execute("DROP TABLE \"{$this->develop_schema}\".\"{$table_name}\"");
    }

    /**
     * Find records for update/delete, ignore mutual annihilation (INSERT+DELETE, UPDATE+DELETE, etc)
     * @param $table_name
     * @return array
     */
    protected function syncAnalyse($table_name)
    {
        $changed = $this->query("SELECT pkey,operation FROM \"{$this->develop_schema}\".develop_audit WHERE tablename = ? ORDER BY timestamp ASC", [$table_name]);

        $keys = [];

        foreach ($changed as $i) {
            $op = $i['operation'];
            $curstate = 1;

            if ($op == 'UPDATE') {
                $curstate = 1;
            }
            if ($op == 'INSERT') {
                $curstate = 1;
            }
            if ($op == 'DELETE') {
                $curstate = 0;
            }

            $keys[$i['pkey']] = $curstate;
        }

        return $keys;
    }

    /**
     * Delete rows from product schema
     * @param $table_name
     * @param $ids
     */
    protected function syncDelete($table_name, $ids)
    {
        if ($ids) {
            $id = implode(',', $ids);
            $this->execute("DELETE FROM \"{$this->product_schema}\".\"{$table_name}\" WHERE id IN ({$id})");
        }
    }

    /**
     * Move new rows from dev to prod and return id of new records
     * @param $table_name
     * @param $ids
     * @return array
     */
    protected function syncInsert($table_name, $ids)
    {
        $used = [];
        if ($ids) {
            $id = implode(',', $ids);
            $ins = $this->query("INSERT INTO \"{$this->product_schema}\".\"{$table_name}\" "
                ."SELECT dev.* FROM \"{$this->develop_schema}\".\"{$table_name}\" AS dev LEFT JOIN \"{$this->product_schema}\".\"{$table_name}\" AS prod USING (id) "
                ."WHERE dev.id IN ({$id}) AND prod.id IS NULL RETURNING id");

            foreach($ins as $in) {
                $used[] = $in['id'];
            }
        }
        return $used;
    }

    /**
     * Update row from dev to prod
     * @param $table_name
     * @param $ids
     */
    protected function syncUpdate($table_name, $ids)
    {
        if ($ids) {
            $id = implode(',', $ids);
            $fields = $this->getTableFields($table_name);
            $field_str = [];
            foreach ($fields as $field) {
                $field_str[] = "\"".$field."\" = dev_table.\"".$field."\"";
            }
            $field_str = implode(',', $field_str);

            $this->execute("UPDATE \"{$this->product_schema}\".\"{$table_name}\" AS prod_table SET {$field_str} "
                ."FROM \"{$this->develop_schema}\".\"{$table_name}\" AS dev_table WHERE prod_table.id=dev_table.id AND dev_table.id IN ({$id})");
        }
    }

    /**
     * List of fields of table
     * @param $table_name
     * @return array
     */
    protected function getTableFields($table_name)
    {
        $res = $this->query("SELECT attrelid::regclass, attnum, attname FROM pg_attribute "
            ."WHERE attrelid = '{$this->product_schema}.{$table_name}'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum");
        $cols = [];
        foreach ($res as $row) {
            $cols[] = $row['attname'];
        }
        return $cols;
    }

    protected function query($query, $params=array())
    {
        return $this->db->query($query, $params);
    }

    protected function execute($query, $params=array())
    {
        return $this->db->execute($query, $params);
    }

    /**
     * Transaction + exclusive lock
     * @param $action
     * @throws Exception
     */
    protected function transaction($action)
    {
        /** @var CDbTransaction $tr */
        static $tr;
        switch($action) {
            case 'begin':
            case 'start':
                if (!$tr) {
                    $tr = Yii::app()->db->beginTransaction();
                    $this->execute("LOCK TABLE \"{$this->develop_schema}\".develop_audit IN ACCESS EXCLUSIVE MODE");
                }
                break;
            case 'commit':
            case 'end':
                if ($tr) {
                    $tr->commit();
                    $tr = null;
                }
                break;
            case 'rollback':
                if ($tr) {
                    $tr->rollback();
                    $tr = null;
                }
                break;
            default:
                if ($tr) {
                    $tr->rollback();
                    $tr = null;
                }
                throw new Exception('Unknown action $action');
        }
    }
} 
