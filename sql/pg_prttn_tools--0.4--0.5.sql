-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_prttn_tools" to load this file. \quit


-- Function: prttn_tools.part_time_cleanup(
--     character varying,
--     character varying,
--     character varying)

CREATE OR REPLACE FUNCTION prttn_tools.part_time_cleanup(
    p_schema character varying,
    p_table_prefix character varying,
    p_interval character varying)
    RETURNS text AS
$BODY$
DECLARE

/*
Удаление устаревших таблиц с суффиксом-датой(yyyymmdd_hh24mi)

p_schema        Схема таблицы
p_table_prefix  Префикс, общий для всех таблиц.  Других таблиц, имеющих тот же
                префикс в схеме быть не должно!
p_interval      Интервал, старше которого удаляются таблицы

*/

    v_table_old character varying;
    r record;
    v_sql_text text;
  
BEGIN

    v_table_old := p_table_prefix
        || to_char(now() - p_interval::interval, 'yyyymmdd_hh24mi');
    
    FOR r IN SELECT '"' || schemaname || '"."' || tablename || '"'
        AS tablename
        FROM pg_tables
        WHERE schemaname = p_schema
            AND tablename like p_table_prefix || '%'
            AND tablename < v_table_old
        ORDER BY tablename
    LOOP
    
        v_sql_text := 'DROP TABLE ' || r.tablename || ';';
        RAISE NOTICE '%', v_sql_text;
        EXECUTE v_sql_text;
        
    END LOOP;

    RETURN 'successfully';

END;
$BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_time_cleanup(character varying,
    character varying, character varying) OWNER TO postgres;


-- END
