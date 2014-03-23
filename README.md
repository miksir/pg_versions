pg_versions
===========

Production/develop version system based on PostgreSQL schema

Yii usage
=========

1. Add Yii component (`PgSchemaVersionsYiiComponent`)
2. Extend CDbConnection, add `Yii::app()->pg_versions->setProductMode()` to `initConnection`
3. For admin interface: `Yii::app()->pg_versions->setDevelopMode()` and add `PgSchemaVersionsYiiSwitchBehavior` to all/required Active Records

