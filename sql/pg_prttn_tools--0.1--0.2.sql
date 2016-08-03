-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_prttn_tools" to load this file. \quit


-- Function: prttn_tools.part_merge(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING)

CREATE OR REPLACE FUNCTION prttn_tools.part_merge(
        p_parent_schema CHARACTER VARYING,
        p_parent_table CHARACTER VARYING,
        p_child_schema CHARACTER VARYING,
        p_child_table CHARACTER VARYING)
    RETURNS TEXT AS
$body$
DECLARE

/*
Слияние дочерней таблицы с родительской

p_parent_schema     Схема родительской таблицы
p_parent_table      Имя родительской таблицы
p_child_schema      Схема дочерней таблицы
p_child_table       Имя дочерней таблицы
*/

    v_count BIGINT;
    v_sql_text CHARACTER VARYING;

BEGIN

    -- Проверка существования таблицы p_parent_schema.p_parent_table

    SELECT count(*) INTO v_count
    FROM information_schema.tables t1
    WHERE t1.table_catalog::name = current_database()
        AND t1.table_type = 'BASE TABLE'
        AND t1.table_schema = p_parent_schema
        AND t1.table_name = p_parent_table;

    IF v_count != 1 THEN
        RAISE 'parent table not found: %', p_parent_schema || '.' ||
            p_parent_table;
    END IF;

    -- Проверка существования таблицы p_child_schema.p_child_table

    SELECT count(*) INTO v_count
    FROM information_schema.tables t1
    WHERE t1.table_catalog::name = current_database()
        AND t1.table_type = 'BASE TABLE'
        AND t1.table_schema = p_child_schema
        AND t1.table_name = p_child_table;

    IF v_count != 1 THEN
        RAISE 'child table not found: %', p_child_schema || '.' ||
            p_child_table;
    END IF;

    -- Проверка совместимости таблиц

    SELECT count(*) INTO v_count FROM (
        SELECT c1.column_name, c1.data_type
        FROM information_schema.tables t1
        JOIN information_schema.columns c1
            ON t1.table_name::name = c1.table_name::name
        WHERE t1.table_catalog::name = current_database()
            AND t1.table_type = 'BASE TABLE'
            AND t1.table_schema = p_parent_schema
            AND t1.table_name = p_parent_table
        EXCEPT
        SELECT c2.column_name, c2.data_type
        FROM information_schema.tables t2
        JOIN information_schema.columns c2
            ON t2.table_name::name = c2.table_name::name
        WHERE t2.table_catalog::name = current_database()
            AND t2.table_type = 'BASE TABLE'
            AND t2.table_schema = p_child_schema
            AND t2.table_name = p_child_table
    ) tt;
    
    IF v_count != 0 THEN
        RAISE 'tables are not compatible: %', p_parent_schema || '.' ||
            p_parent_table || ', ' || p_child_schema || '.' || p_child_table;
    END IF;

    -- Вставка данных в родительскую таблицу
    v_sql_text := 'INSERT INTO ' || p_parent_schema || '.' || p_parent_table ||
        ' SELECT * FROM ' || p_child_schema || '.' || p_child_table;
    EXECUTE v_sql_text;

    -- Удаление дочерней таблицы
    v_sql_text := 'DROP TABLE ' || p_child_schema || '.' || p_child_table;
    EXECUTE v_sql_text;

    RETURN 'ok';

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_merge(CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING) OWNER TO postgres;
