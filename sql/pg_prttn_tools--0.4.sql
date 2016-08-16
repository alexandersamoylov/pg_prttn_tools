-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_prttn_tools" to load this file. \quit


-- Function: prttn_tools.create_child_table(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.create_child_table(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_child_schema CHARACTER VARYING,
        p_child_table CHARACTER VARYING,
        p_check_condition CHARACTER VARYING,
        p_with_rule BOOLEAN)
    RETURNS TEXT AS
$body$
DECLARE

/*
Создание дочерней таблицы

p_schema            Схема родительской таблицы
p_table             Имя родительской таблицы
p_child_schema      Схема дочерней таблицы
p_child_table       Имя дочерней таблицы
p_check_condition   Условие для создания constraint на дочерней таблице
p_with_rule         Создание rule для родительской таблицы с условием,
                    заданным в p_check_condition
*/

    v_ddl_text CHARACTER VARYING;

BEGIN

    -- Создание дочерней таблицы

    v_ddl_text := 'CREATE TABLE ' || p_child_schema || '.' || p_child_table ||
        ' ( LIKE ' || p_schema || '.' || p_table || ' INCLUDING ALL )';
    EXECUTE v_ddl_text;

    v_ddl_text := 'ALTER TABLE ' || p_child_schema || '.' || p_child_table ||
        ' INHERIT ' || p_schema || '.' || p_table;
    EXECUTE v_ddl_text;

    -- Создание CONSTRAINT для дочерней таблицы

    IF p_check_condition IS NOT NULL THEN
        v_ddl_text := 'ALTER TABLE ' ||
            p_child_schema || '.' || p_child_table ||
            ' ADD CONSTRAINT ' || p_child_table || '_check CHECK ' ||
            p_check_condition;
        EXECUTE v_ddl_text;

        -- Создание правила для родительской таблицы

        IF p_with_rule THEN
            v_ddl_text := 'CREATE RULE route_' ||
                p_child_schema || '_' || p_child_table || ' AS ' ||
                ' ON INSERT TO ' || p_schema ||'.'|| p_table ||
                ' WHERE ' || p_check_condition || ' DO INSTEAD INSERT INTO ' ||
                p_child_schema || '.' || p_child_table || ' VALUES (new.*)';
            EXECUTE v_ddl_text;
        END IF;

    END IF;

    RETURN 'created';

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.create_child_table(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
    BOOLEAN) OWNER TO postgres;


-- Function: prttn_tools.drop_ins_trigger(
--     CHARACTER VARYING,
--     CHARACTER VARYING)

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


-- Function: prttn_tools.part_list_check(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_check(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_list_value CHARACTER VARYING)
    RETURNS table(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_list CHARACTER VARYING
    ) AS
$body$
DECLARE

/*
Проверка существования дочерней таблицы

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_list_value    Значение для вставки в поле p_list_column
*/

    v_count INTEGER;

BEGIN

    -- Формирование имени и параметров дочерней таблицы

    child_table := p_table || '_' || p_list_value;
    child_list := p_list_value;

    -- Проверка существования дочерней таблицы

    SELECT count(*) INTO v_count
    FROM pg_catalog.pg_tables
    WHERE schemaname = p_schema AND tablename = child_table;

    IF v_count != 0 THEN
        child_table_status := 'exist';
    ELSE
        child_table_status := 'noexist';
    END IF;

    -- Returns:
    -- child_table          p_table_p_list_value
    -- child_table_status   exist/noexist
    -- child_list           p_list_value

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_list_check(CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING) OWNER TO postgres;


-- Function: prttn_tools.part_list_add(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_add(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_list_value CHARACTER VARYING,
        p_with_rule BOOLEAN)
    RETURNS table(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_list CHARACTER VARYING
    ) AS
$body$
DECLARE

/*
Создание дочерней таблицы

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_list_value    Значение для вставки в поле p_list_column
p_with_rule     Создание rule для родительской таблицы с условием,
                заданным в v_check_condition
*/

    v_count INTEGER;
    v_check_condition CHARACTER VARYING;
    v_ddl_text CHARACTER VARYING;

BEGIN

    -- Проверка существования поля p_schemaname.p_tablename.p_list_column
    
    SELECT count(*) INTO v_count
    FROM information_schema.tables t
    JOIN information_schema.columns c
        ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
    WHERE t.table_catalog::name = current_database() AND
        t.table_type = 'BASE TABLE' AND
        t.table_schema = p_schema AND
        t.table_name = p_table AND
        c.column_name = p_list_column;

    -- Выход c ошибкой если данные о поле или таблице не верны

    IF v_count != 1 THEN
        raise 'incorrect master table %', p_schema || '.' || p_table;
    END IF;

    -- Формирование имени и параметров дочерней таблицы

    SELECT r.child_table, r.child_table_status, r.child_list
        INTO child_table, child_table_status, child_list
    FROM prttn_tools.part_list_check(p_schema, p_table, p_list_column,
        p_list_value) r;

    v_check_condition := '(' ||
        p_list_column || ' = ' || quote_literal(p_list_value) || ')';

    -- Создание таблицы если дочерняя таблица не существует

    IF child_table_status = 'noexist' THEN

        -- Создание дочерней таблицы

        SELECT prttn_tools.create_child_table(p_schema, p_table,
            p_schema, child_table, v_check_condition, p_with_rule)
            INTO child_table_status;

    END IF;

    -- Returns:
    -- child_table          p_table_p_list_value
    -- child_table_status   exist/created
    -- child_list           p_list_value

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_list_add(CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, BOOLEAN) OWNER TO postgres;


-- Function: prttn_tools.part_list_create_trigger(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_create_trigger(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_autocreate BOOLEAN)
    RETURNS TABLE(
        trigger_function CHARACTER VARYING,
        trigger_status CHARACTER VARYING
    ) AS
$body$
DECLARE

/*
Создание триггера для включения секционирования

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_autocreate    Автоматическое/ручное создание секций
*/

    v_sql TEXT;

BEGIN

    trigger_function := p_schema || '.' || p_table || '_part_ins_tr';
    IF p_autocreate THEN
        trigger_function := trigger_function || '_ac';
    END IF;

    v_sql := 'CREATE OR REPLACE FUNCTION ' || trigger_function || '()
    RETURNS TRIGGER AS
$trigger$
DECLARE
    -- this function is automatically created
    -- from prttn_tools.part_list_create_trigger
    -- ' || now() || '

    v_child_table CHARACTER VARYING;
    v_child_table_status CHARACTER VARYING;
BEGIN

    -- Проверка существования дочерней таблицы
    SELECT child_table, child_table_status
        INTO v_child_table, v_child_table_status
    FROM prttn_tools.part_list_check(
            ' || quote_literal(p_schema) || ',
            ' || quote_literal(p_table) || ',
            ' || quote_literal(p_list_column) || ',
            new.' || p_list_column || '::CHARACTER VARYING);';

    IF p_autocreate THEN
        v_sql := v_sql || '

    -- Создание новой дочерней таблицы, если не существует
    IF v_child_table_status = ''noexist'' THEN
        SELECT child_table, child_table_status
            INTO v_child_table, v_child_table_status
        FROM prttn_tools.part_list_add(
                ' || quote_literal(p_schema) || ',
                ' || quote_literal(p_table) || ',
                ' || quote_literal(p_list_column) || ',
                new.' || p_list_column || '::CHARACTER VARYING,
                FALSE);
    END IF;';
    END IF;

    v_sql := v_sql || '

    -- Вставка записи в дочернюю таблицу
    IF v_child_table_status = ''exist'' OR v_child_table_status = ''created''
    THEN
        EXECUTE ''INSERT INTO ' || p_schema || '.'' || v_child_table ||
            '' SELECT ( ('' || quote_literal(new) ||
            '')::' || p_schema || '.' || p_table || ' ).*'';
    ELSE
        RAISE ''child table % not exist'', v_child_table;
    END IF;

    RETURN NULL;

END;
$trigger$
    LANGUAGE plpgsql VOLATILE
    COST 100;

CREATE TRIGGER ' || p_table || '_part_ins_tr
    BEFORE INSERT
    ON ' || p_schema || '.' || p_table || '
    FOR EACH ROW
    EXECUTE PROCEDURE ' || trigger_function || '();';

    EXECUTE v_sql;
    trigger_status := 'created';

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_list_create_trigger(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, BOOLEAN) OWNER TO postgres;


-- Function: prttn_tools.part_time_check(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     TIMESTAMP WITHOUT TIME ZONE)

CREATE OR REPLACE FUNCTION prttn_tools.part_time_check(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_time_value TIMESTAMP WITHOUT TIME ZONE)
    RETURNS TABLE(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_time_from TIMESTAMP WITHOUT TIME ZONE,
        child_time_to TIMESTAMP WITHOUT TIME ZONE
    ) AS
$body$
DECLARE

/*
Проверка существования дочерней таблицы

p_schema      Схема родительской таблицы
p_table       Имя родительской таблицы
p_time_column Поле для секционирования по интервалу времени
p_time_range  Интервал для секционирования:
              year, month, day, hour, minute
p_time_value  Значение для вставки в поле p_time_column
*/

    v_count INTEGER;

BEGIN

    -- Формирование имени и параметров дочерней таблицы

    child_table := p_table || '_';

    CASE lower(p_time_range)
        WHEN 'year' THEN
            child_table := child_table || to_char(p_time_value, 'yyyy');
            child_time_from := date_trunc('year', p_time_value);
            child_time_to := date_trunc('year',
                p_time_value + interval '1 year');
        WHEN 'month' THEN
            child_table := child_table || to_char(p_time_value, 'yyyymm');
            child_time_from := date_trunc('month', p_time_value);
            child_time_to := date_trunc('month',
                p_time_value + interval '1 month');
        WHEN 'day' THEN
            child_table := child_table || to_char(p_time_value, 'yyyymmdd');
            child_time_from := date_trunc('day', p_time_value);
            child_time_to := date_trunc('day',
                p_time_value + interval '1 day');
        WHEN 'hour' THEN
            child_table := child_table ||
                to_char(p_time_value, 'yyyymmdd_hh24');
            child_time_from := date_trunc('hour', p_time_value);
            child_time_to := date_trunc('hour',
                p_time_value + interval '1 hour');
        WHEN 'minute' THEN
            child_table := child_table ||
                to_char(p_time_value, 'yyyymmdd_hh24mi');
            child_time_from := date_trunc('minute', p_time_value);
            child_time_to := date_trunc('minute',
                p_time_value + interval '1 minute');
        ELSE
            RAISE 'incorrect variable p_time_range %s', p_time_range;
    END CASE;

    -- Проверка существования дочерней таблицы

    SELECT count(*) INTO v_count
    FROM pg_catalog.pg_tables
    WHERE schemaname = p_schema AND tablename = child_table;

    IF v_count != 0 THEN
        child_table_status := 'exist';
    ELSE
        child_table_status := 'noexist';
    END IF; 

    -- Returns:
    -- child_table        | p_table_p_time_value
    -- child_table_status | exist/noexist
    -- child_time_from    | Дата/время начала интервала
    -- child_time_to      | Дата/время конца интервала

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_time_check(CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, TIMESTAMP WITHOUT TIME ZONE)
    OWNER TO postgres;


-- Function: prttn_tools.part_time_add(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     TIMESTAMP WITHOUT TIME ZONE,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_time_add(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_time_value TIMESTAMP WITHOUT TIME ZONE,
        p_with_rule BOOLEAN)
    RETURNS TABLE(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_time_from TIMESTAMP WITHOUT TIME ZONE,
        child_time_to TIMESTAMP WITHOUT TIME ZONE
    ) AS
$body$
DECLARE

/*
Создание дочерней таблицы

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_time_column   Поле для секционирования по интервалу времени
p_time_range    Интервал для секционирования:
                year, month, day, hour, minute
p_time_value    Значение для вставки в поле p_time_column
p_with_rule     Создание rule для родительской таблицы с условием,
                заданным в v_check_condition
*/

    v_count INTEGER;
    v_check_condition CHARACTER VARYING;
    v_ddl_text CHARACTER VARYING;

BEGIN

    -- Проверка существования поля
    -- p_schemaname.p_tablename.p_time_column(timestamp without time zone)
    
    SELECT count(*) INTO v_count
    FROM information_schema.tables t
    JOIN information_schema.columns c
        ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
    WHERE t.table_catalog::name = current_database() AND
        t.table_type = 'BASE TABLE' AND
        t.table_schema = p_schema AND
        t.table_name = p_table AND
        c.column_name = p_time_column AND
        c.data_type = 'timestamp without time zone';

    -- Выход c ошибкой если данные о поле или таблице не верны

    IF v_count != 1 THEN
        RAISE 'incorrect master table %', p_schema || '.' || p_table;
    END IF;

    -- Формирование имени и параметров дочерней таблицы

    SELECT r.child_table, r.child_table_status, r.child_time_from,
        r.child_time_to
        INTO child_table, child_table_status, child_time_from, child_time_to
    FROM prttn_tools.part_time_check(p_schema, p_table, p_time_column,
        p_time_range, p_time_value) r;

    v_check_condition := '(' ||
        p_time_column || ' >= ' || quote_literal(child_time_from) || ' AND ' ||
        p_time_column || ' < ' || quote_literal(child_time_to) || ')';

    -- Создание таблицы если дочерняя таблица не существует

    IF child_table_status = 'noexist' THEN

        -- Создание дочерней таблицы

        SELECT prttn_tools.create_child_table(p_schema, p_table,
            p_schema, child_table, v_check_condition, p_with_rule)
            INTO child_table_status;

    END IF;

    -- Returns:
    -- child_table          p_table_p_time_value
    -- child_table_status   exist/created
    -- child_time_from      Дата/время начала интервала
    -- child_time_to        Дата/время конца интервала

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_time_add(CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, TIMESTAMP WITHOUT TIME ZONE, BOOLEAN)
    OWNER TO postgres;


-- Function: prttn_tools.part_time_create_trigger(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_time_create_trigger(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_autocreate BOOLEAN)
    RETURNS TABLE(
        trigger_function CHARACTER VARYING,
        trigger_status CHARACTER VARYING
    ) AS
$body$
DECLARE

/*
Создание триггера для включения секционирования

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_time_column   Поле для секционирования по интервалу времени
p_time_range    Интервал для секционирования:
                year, month, day, hour, minute 
p_autocreate    Автоматическое/ручное создание секций
*/

    v_sql TEXT;

BEGIN

    trigger_function := p_schema || '.' || p_table || '_part_ins_tr';
    IF p_autocreate THEN
        trigger_function := trigger_function || '_ac';
    END IF;

    v_sql := 'CREATE OR REPLACE FUNCTION ' || trigger_function || '()
    RETURNS TRIGGER AS
$trigger$
DECLARE
    -- this function is automatically created
    -- from prttn_tools.part_time_create_trigger
    -- ' || now() || '

    v_child_table CHARACTER VARYING;
    v_child_table_status CHARACTER VARYING;
BEGIN

    -- Проверка существования дочерней таблицы
    SELECT child_table, child_table_status
        INTO v_child_table, v_child_table_status
    FROM prttn_tools.part_time_check(
            ' || quote_literal(p_schema) || ',
            ' || quote_literal(p_table) || ',
            ' || quote_literal(p_time_column) || ',
            ' || quote_literal(p_time_range) || ',
            new.' || p_time_column || '::TIMESTAMP WITHOUT TIME ZONE);';

    IF p_autocreate THEN
        v_sql := v_sql || '

    -- Создание новой дочерней таблицы, если не существует
    IF v_child_table_status = ''noexist'' THEN
        SELECT child_table, child_table_status
            INTO v_child_table, v_child_table_status
        FROM prttn_tools.part_time_add(
                ' || quote_literal(p_schema) || ',
                ' || quote_literal(p_table) || ',
                ' || quote_literal(p_time_column) || ',
                ' || quote_literal(p_time_range) || ',
                new.' || p_time_column || '::TIMESTAMP WITHOUT TIME ZONE,
                FALSE);
    END IF;';
    END IF;

    v_sql := v_sql || '

    -- Вставка записи в дочернюю таблицу
    IF v_child_table_status = ''exist'' OR v_child_table_status = ''created''
    THEN
        EXECUTE ''INSERT INTO ' || p_schema || '.'' || v_child_table ||
            '' SELECT ( ('' || quote_literal(new) ||
            '')::' || p_schema || '.' || p_table || ' ).*'';
    ELSE
        RAISE ''child table % not exist'', v_child_table;
    END IF;

    RETURN NULL;

END;
$trigger$
    LANGUAGE plpgsql VOLATILE
    COST 100;

CREATE TRIGGER ' || p_table || '_part_ins_tr
    BEFORE INSERT
    ON ' || p_schema || '.' || p_table || '
    FOR EACH ROW
    EXECUTE PROCEDURE ' || trigger_function || '();';

    EXECUTE v_sql;
    trigger_status := 'created';

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_time_create_trigger(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, BOOLEAN)
    OWNER TO postgres;


-- Function: prttn_tools.part_list_time_check(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     TIMESTAMP WITHOUT TIME ZONE)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_time_check(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_list_value CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_time_value TIMESTAMP WITHOUT TIME ZONE)
    RETURNS TABLE(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_list CHARACTER VARYING,
        child_time_from TIMESTAMP WITHOUT TIME ZONE,
        child_time_to TIMESTAMP WITHOUT TIME ZONE
    ) AS
$body$
DECLARE

/*
Проверка существования дочерней таблицы

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_list_value    Значение для вставки в поле p_list_column
p_time_column   Поле для секционирования по интервалу времени
p_time_range    Интервал для секционирования:
                year, month, day, hour, minute 
p_time_value    Значение для вставки в поле p_time_column
*/

    v_count INTEGER;

BEGIN

    -- Формирование имени и параметров дочерней таблицы

    child_table := p_table || '_' || p_list_value || '_';
    child_list := p_list_value;

    CASE lower(p_time_range)
        WHEN 'year' THEN
            child_table := child_table || to_char(p_time_value, 'yyyy');
            child_time_from := date_trunc('year', p_time_value);
            child_time_to := date_trunc('year',
                p_time_value + interval '1 year');
        WHEN 'month' THEN
            child_table := child_table || to_char(p_time_value, 'yyyymm');
            child_time_from := date_trunc('month', p_time_value);
            child_time_to := date_trunc('month',
                p_time_value + interval '1 month');
        WHEN 'day' THEN
            child_table := child_table || to_char(p_time_value, 'yyyymmdd');
            child_time_from := date_trunc('day', p_time_value);
            child_time_to := date_trunc('day',
                p_time_value + interval '1 day');
        WHEN 'hour' THEN
            child_table := child_table ||
                to_char(p_time_value, 'yyyymmdd_hh24');
            child_time_from := date_trunc('hour', p_time_value);
            child_time_to := date_trunc('hour',
                p_time_value + interval '1 hour');
        WHEN 'minute' THEN
            child_table := child_table ||
                to_char(p_time_value, 'yyyymmdd_hh24mi');
            child_time_from := date_trunc('minute', p_time_value);
            child_time_to := date_trunc('minute',
                p_time_value + interval '1 minute');
        ELSE
            RAISE 'incorrect variable p_time_range %s', p_time_range;
    END CASE;

    -- Проверка существования дочерней таблицы

    SELECT count(*) INTO v_count
    FROM pg_catalog.pg_tables
    WHERE schemaname = p_schema AND tablename = child_table;

    IF v_count != 0 THEN
        child_table_status := 'exist';
    ELSE
        child_table_status := 'noexist';
    END IF;

    -- Returns:
    -- child_table          p_table_p_list_value_p_time_value
    -- child_table_status   exist/noexist
    -- child_list           p_list_value
    -- child_time_from      Дата/время начала интервала
    -- child_time_to        Дата/время конца интервала

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;
    
ALTER FUNCTION prttn_tools.part_list_time_check(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, TIMESTAMP WITHOUT TIME ZONE) OWNER TO postgres;


-- Function: prttn_tools.part_list_time_add(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     TIMESTAMP WITHOUT TIME ZONE,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_time_add(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_list_value CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_time_value TIMESTAMP WITHOUT TIME ZONE,
        p_with_rule BOOLEAN)
    RETURNS TABLE(
        child_table CHARACTER VARYING,
        child_table_status CHARACTER VARYING,
        child_list CHARACTER VARYING,
        child_time_from TIMESTAMP WITHOUT TIME ZONE,
        child_time_to TIMESTAMP WITHOUT TIME ZONE
    ) AS
$body$
DECLARE

/*
Создание дочерней таблицы

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_list_value    Значение для вставки в поле p_list_column
p_time_column   Поле для секционирования по интервалу времени
p_time_range    Интервал для секционирования:
                year, month, day, hour, minute 
p_time_value    Значение для вставки в поле p_time_column
p_with_rule     Создание rule для родительской таблицы с условием,
                заданным в v_check_condition
*/

    v_count INTEGER;
    v_check_condition CHARACTER VARYING;
    v_ddl_text CHARACTER VARYING;

BEGIN

    -- Проверка существования полей:
    -- p_schemaname.p_tablename.p_list_column
    -- p_schemaname.p_tablename.p_time_column(timestamp without time zone)
    
    SELECT count(*) INTO v_count
    FROM information_schema.tables t
    JOIN information_schema.columns c
        ON t.table_catalog = c.table_catalog
            AND t.table_schema = c.table_schema
            AND t.table_name = c.table_name
    WHERE t.table_catalog::name = current_database() AND
        t.table_type = 'BASE TABLE' AND
        t.table_schema = p_schema AND
        t.table_name = p_table AND
        (c.column_name = p_list_column OR
            (c.column_name = p_time_column AND
                c.data_type = 'timestamp without time zone'
            )
        );

    -- Выход c ошибкой если данные о поле или таблице не верны

    IF v_count != 2 THEN
        RAISE 'incorrect master table %', p_schema || '.' || p_table;
    END IF;

    -- Формирование имени и параметров дочерней таблицы

    SELECT r.child_table, r.child_table_status, r.child_list, r.child_time_from,
        r.child_time_to INTO child_table, child_table_status, child_list,
            child_time_from, child_time_to
    FROM prttn_tools.part_list_time_check(p_schema, p_table, p_list_column,
        p_list_value, p_time_column, p_time_range, p_time_value) r;

    v_check_condition := '(' ||
        p_list_column || ' = ' || quote_literal(p_list_value) || ' AND ' ||
        p_time_column || ' >= ' || quote_literal(child_time_from) || ' AND ' ||
        p_time_column || ' < ' || quote_literal(child_time_to) || ')';

    -- Создание таблицы если дочерняя таблица не существует

    IF child_table_status = 'noexist' THEN

        -- Создание child-таблицы

        SELECT prttn_tools.create_child_table(p_schema, p_table,
            p_schema, child_table, v_check_condition, p_with_rule)
            INTO child_table_status;

    END IF;

    -- Returns:
    -- child_table          p_table_p_list_value_p_time_value
    -- child_table_status   exist/created
    -- child_list           p_list_value
    -- child_time_from      Дата/время начала интервала
    -- child_time_to        Дата/время конца интервала

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_list_time_add(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
    CHARACTER VARYING, TIMESTAMP WITHOUT TIME ZONE, BOOLEAN) OWNER TO postgres;


-- Function: prttn_tools.part_list_time_create_trigger(
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     CHARACTER VARYING,
--     BOOLEAN)

CREATE OR REPLACE FUNCTION prttn_tools.part_list_time_create_trigger(
        p_schema CHARACTER VARYING,
        p_table CHARACTER VARYING,
        p_list_column CHARACTER VARYING,
        p_time_column CHARACTER VARYING,
        p_time_range CHARACTER VARYING,
        p_autocreate BOOLEAN)
    RETURNS TABLE(
        trigger_function CHARACTER VARYING,
        trigger_status CHARACTER VARYING
    ) AS
$body$
DECLARE

/*
Создание триггера для включения секционирования

p_schema        Схема родительской таблицы
p_table         Имя родительской таблицы
p_list_column   Поле для секционирования по значениям
p_time_column   Поле для секционирования по интервалу времени
p_time_range    Интервал для секционирования:
                year, month, day, hour, minute 
p_autocreate    Автоматическое/ручное создание секций
*/

    v_sql TEXT;
BEGIN

    trigger_function := p_schema || '.' || p_table || '_part_ins_tr';

    v_sql := 'CREATE OR REPLACE FUNCTION ' || trigger_function || '()
    RETURNS TRIGGER AS
$trigger$
DECLARE
    -- this function is automatically created
    -- from prttn_tools.part_list_time_create_trigger
    -- ' || now() || '

    v_child_table CHARACTER VARYING;
    v_child_table_status CHARACTER VARYING;
BEGIN

    -- Проверка существования дочерней таблицы
    SELECT child_table, child_table_status
        INTO v_child_table, v_child_table_status
    FROM prttn_tools.part_list_time_check(
            ' || quote_literal(p_schema) || ',
            ' || quote_literal(p_table) || ',
            ' || quote_literal(p_list_column) || ',
            new.' || p_list_column || '::CHARACTER VARYING,
            ' || quote_literal(p_time_column) || ',
            ' || quote_literal(p_time_range) || ',
            new.' || p_time_column || '::TIMESTAMP WITHOUT TIME ZONE);';

    IF p_autocreate THEN
        v_sql := v_sql || '

    -- Создание новой дочерней таблицы, если не существует
    IF v_child_table_status = ''noexist'' THEN
        SELECT child_table, child_table_status
            INTO v_child_table, v_child_table_status
        FROM prttn_tools.part_list_time_add(
                ' || quote_literal(p_schema) || ',
                ' || quote_literal(p_table) || ',
                ' || quote_literal(p_list_column) || ',
                new.' || p_list_column || '::CHARACTER VARYING,
                ' || quote_literal(p_time_column) || ',
                ' || quote_literal(p_time_range) || ',
                new.' || p_time_column || '::TIMESTAMP WITHOUT TIME ZONE,
                FALSE);
    END IF;';
    END IF;

    v_sql := v_sql || '

    -- Вставка записи в дочернюю таблицу
    IF v_child_table_status = ''exist'' OR v_child_table_status = ''created''
    THEN
        EXECUTE ''INSERT INTO ' || p_schema || '.'' || v_child_table ||
            '' SELECT ( ('' || quote_literal(new) ||
            '')::' || p_schema || '.' || p_table || ' ).*'';
    ELSE
        RAISE ''child table % not exist'', v_child_table;
    END IF;

    RETURN NULL;

END;
$trigger$
    LANGUAGE plpgsql VOLATILE
    COST 100;

CREATE TRIGGER ' || p_table || '_part_ins_tr
    BEFORE INSERT
    ON ' || p_schema || '.' || p_table || '
    FOR EACH ROW
    EXECUTE PROCEDURE ' || trigger_function || '();';

    EXECUTE v_sql;
    trigger_status := 'created';

    RETURN NEXT;
    RETURN;

END;
$body$
    LANGUAGE plpgsql VOLATILE
    COST 100;

ALTER FUNCTION prttn_tools.part_list_time_create_trigger(CHARACTER VARYING,
    CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
    BOOLEAN) OWNER TO postgres;


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
            ON t1.table_catalog = c1.table_catalog
                AND t1.table_schema = c1.table_schema
                AND t1.table_name = c1.table_name
        WHERE t1.table_catalog::name = current_database()
            AND t1.table_type = 'BASE TABLE'
            AND t1.table_schema = p_parent_schema
            AND t1.table_name = p_parent_table
        EXCEPT
        SELECT c2.column_name, c2.data_type
        FROM information_schema.tables t2
        JOIN information_schema.columns c2
            ON t2.table_catalog = c2.table_catalog
                AND t2.table_schema = c2.table_schema
                AND t2.table_name = c2.table_name
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
