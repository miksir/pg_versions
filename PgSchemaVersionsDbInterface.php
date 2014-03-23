<?php


interface PgSchemaVersionsDbInterface {

    public function query($sql, $params=array());
    public function execute($sql, $params=array());

} 