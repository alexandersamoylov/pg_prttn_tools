-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_prttn_tools" to load this file. \quit


-- Function: prttn_tools.drop_ins_trigger(
--     CHARACTER VARYING,
--     CHARACTER VARYING)

-- Добавлено удаление триггерной функции с суффиксом "_ac"

CREATE OR REPLACE FUNCTION prttn_tools.drop_ins_trigger(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING)
    RETURNS TEXT AS
$body$
DECLARE

/*
Удаление триггера на вставку записей и триггерной функции, созданных
функцией part_%_create_trigger()

p_schema      Схема родительской таблицы
p_table       Имя родительской таблицы
*/

    v_ddl_text CHARACTER VARYING;

BEGIN

    -- Удаление триггера
    v_ddl_text := 'DROP TRIGGER ' || p_table || '_part_ins_tr ON ' ||
        p_schema || '.' || p_table;
    EXECUTE v_ddl_text;

    -- Удаление триггерной функции
    v_ddl_text := 'DROP FUNCTION IF EXISTS ' || p_schema || '.' || p_table ||
        '_part_ins_tr()';
    EXECUTE v_ddl_text;
    v_ddl_text := 'DROP FUNCTION IF EXISTS ' || p_schema || '.' || p_table ||
        '_part_ins_tr_ac()';
    EXECUTE v_ddl_text;


    RETURN 'ok';

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.drop_ins_trigger(CHARACTER VARYING,
    CHARACTER VARYING) OWNER TO postgres;


-- END
