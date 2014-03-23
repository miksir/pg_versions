<?php


class PgSchemaVersionsInit {
    protected $pdo;

    public function __construct(PgSchemaVersionsDbInterface $db)
    {
        $this->db = $db;
    }

    public function up($schema)
    {
        $this->createSchema($schema);
        $this->createTrackTable($schema);
        $this->createTrackFunction($schema);
    }

    public function down($schema)
    {
        $this->dropSchema($schema);
    }

    protected function createSchema($schema)
    {
        $this->execute('CREATE SCHEMA '.$schema);
    }

    protected function createTrackTable($schema)
    {
        $sql = <<<END
CREATE TABLE IF NOT EXISTS "{$schema}".develop_audit
(
  id serial NOT NULL,
  tablename text,
  pkey integer,
  operation text,
  "timestamp" timestamp without time zone DEFAULT now(),
  CONSTRAINT develop_audit_pkey PRIMARY KEY (id)
)
END;
        $this->execute($sql);
    }

    protected function createTrackFunction($schema)
    {
        $sql = <<<END
CREATE OR REPLACE FUNCTION "{$schema}".develop_audit_trigger() RETURNS trigger AS $$
        BEGIN
                IF      TG_OP = 'INSERT'
                THEN
                        INSERT INTO "{$schema}".develop_audit(tablename, pkey, operation)
                                VALUES (TG_RELNAME, NEW.id, TG_OP);
                        RETURN NEW;

                ELSIF   TG_OP = 'UPDATE'
                THEN
                        INSERT INTO "{$schema}".develop_audit(tablename, pkey, operation)
                                VALUES (TG_RELNAME, NEW.id, TG_OP);
                        RETURN NEW;

                ELSIF   TG_OP = 'DELETE'
                THEN
                        INSERT INTO "{$schema}".develop_audit(tablename, pkey, operation)
                                VALUES (TG_RELNAME, OLD.id, TG_OP);
                        RETURN OLD;

                END IF;
        END;
$$ LANGUAGE 'plpgsql' SECURITY DEFINER;
END;
        $this->execute($sql);
    }

    protected function dropSchema($schema)
    {
        $this->execute('DROP SCHEMA '.$schema.' CASCADE');
    }

    protected function execute($query, $params=array())
    {
        return $this->db->execute($query, $params);
    }
} 