-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- -----------------------------------------------------------------------------
-- Common code for OLTP benchmarks.
-- -----------------------------------------------------------------------------

function init()
    assert(event ~= nil,
            "this script is meant to be included by other OLTP scripts and " ..
                    "should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, prewarm, run, " ..
            "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
    table_size =
    {"Number of rows per table", 10000},
    range_size =
    {"Range size for range SELECT queries", 100},
    tables =
    {"Number of tables", 1},
    point_selects =
    {"Number of point SELECT queries per transaction", 10},
    simple_ranges =
    {"Number of simple range SELECT queries per transaction", 1},
    sum_ranges =
    {"Number of SELECT SUM() queries per transaction", 1},
    order_ranges =
    {"Number of SELECT ORDER BY queries per transaction", 1},
    distinct_ranges =
    {"Number of SELECT DISTINCT queries per transaction", 1},
    index_updates =
    {"Number of UPDATE index queries per transaction", 1},
    non_index_updates =
    {"Number of UPDATE non-index queries per transaction", 1},
    delete_inserts =
    {"Number of DELETE/INSERT combinations per transaction", 1},
    range_selects =
    {"Enable/disable all range SELECT queries", true},
    auto_inc =
    {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
             "or its alternatives in other DBMS. When disabled, use " ..
             "client-generated IDs", true},
    skip_trx =
    {"Don't start explicit transactions and execute all queries " ..
             "in the AUTOCOMMIT mode", false},
    secondary =
    {"Use a secondary index in place of the PRIMARY KEY", false},
    create_secondary =
    {"Create a secondary index in addition to the PRIMARY KEY", false},
    mysql_storage_engine =
    {"Storage engine, if MySQL is used", "innodb"},
    pgsql_variant =
    {"Use this PostgreSQL variant when running with the " ..
             "PostgreSQL driver. The only currently supported " ..
             "variant is 'redshift'. When enabled, " ..
             "create_secondary is automatically disabled, and " ..
             "delete_inserts is set to 0"}
}

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
    sysbench.opt.threads do
        create_table(drv, con, i)
    end
end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
--
-- PS. Currently, this command is only meaningful for MySQL/InnoDB benchmarks
function cmd_prewarm()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    assert(drv:name() == "mysql", "prewarm is currently MySQL only")

    -- Do not create on disk tables for subsequent queries
    con:query("SET tmp_table_size=2*1024*1024*1024")
    con:query("SET max_heap_table_size=2*1024*1024*1024")

    for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
    sysbench.opt.threads do
        local t = "sagm_choose_buy_pool" .. i
        print("Prewarming table " .. t)
        con:query("ANALYZE TABLE sagm_choose_buy_pool" .. i)
        con:query(string.format(
                "SELECT AVG(id) FROM " ..
                        "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                        "LIMIT %u) t",
                t, sysbench.opt.table_size))
        con:query(string.format(
                "SELECT COUNT(*) FROM " ..
                        "(SELECT * FROM %s WHERE k LIKE '%%0%%' LIMIT %u) t",
                t, sysbench.opt.table_size))
    end
end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
    prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
    prewarm = {cmd_prewarm, sysbench.cmdline.PARALLEL_COMMAND}
}


-- template strings of random digits with 11-digit groups separated by dashes

-- 10 groups, 119 characters
--local c_value_template = "###########-###########-###########-" ..
--        "###########-###########-###########-" ..
--        "###########-###########-###########-" ..
--        "###########"

-- 5 groups, 59 characters
--local pad_value_template = "###########-###########-###########-" ..
--        "###########-###########"
local auth_list_code_value_template = "#############################"
local show_supplier_name_template = "###################################################################################################"

--function get_c_value()
--    return sysbench.rand.string(c_value_template)
--end
function get_auth_list_code_value()
    return sysbench.rand.string(auth_list_code_value_template)
end
function get_show_supplier_name_value()
    return sysbench.rand.string(show_supplier_name_template)
end

--
--function get_pad_value()
--    return sysbench.rand.string(pad_value_template)
--end

function create_table(drv, con, table_num)
    local id_index_def, id_def
    local engine_def = ""
    local extra_table_options = ""
    local query

    if sysbench.opt.secondary then
        id_index_def = "KEY xid"
    else
        id_index_def = "PRIMARY KEY"
    end

    if drv:name() == "mysql" or drv:name() == "attachsql" or
            drv:name() == "drizzle"
    then
        if sysbench.opt.auto_inc then
            id_def = "bigint NOT NULL AUTO_INCREMENT"
        else
            id_def = "bigint NOT NULL AUTO_INCREMENT"
        end
        engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
        extra_table_options = mysql_table_options or ""
    elseif drv:name() == "pgsql"
    then
        if not sysbench.opt.auto_inc then
            id_def = "INTEGER NOT NULL"
        elseif pgsql_variant == 'redshift' then
            id_def = "INTEGER IDENTITY(1,1)"
        else
            id_def = "SERIAL"
        end
    else
        error("Unsupported database driver:" .. drv:name())
    end

    print(string.format("Creating table 'sagm_choose_buy_pool%d'...", table_num))

    query = string.format([[
create table sagm_choose_buy_pool%d
(
    id   %s,
    auth_list_code        varchar(30)                           not null comment '权限集code',
    sku_id                bigint                                not null comment '商品id',
    show_supplier_name    varchar(100)                          not null comment '展示供应商名称',
    org_id                bigint                                not null comment '组织id',
    price                 decimal(30, 10)                       null,
    agreement_type        varchar(30)                           not null comment '协议类型',
    agreement_header_id   bigint                                null comment '协议头id',
    agreement_line_id     bigint                                not null comment '协议行id',
    shelf_flag            tinyint(1)  default 0                 not null comment '上下架标识',
    tenant_id             bigint      default 0                 not null comment '租户ID',
    object_version_number bigint      default 1                 not null comment '行版本号，用来处理锁',
    creation_date         datetime    default CURRENT_TIMESTAMP not null,
    created_by            bigint      default -1                not null,
    last_updated_by       bigint      default -1                not null,
    last_update_date      datetime    default CURRENT_TIMESTAMP not null,
    price_hidden_flag     tinyint(1)  default 0                 not null comment '是否隐藏价格',
    delete_flag           tinyint(1)  default 0                 not null comment '删除标识',
    channel               varchar(80) default 'ENTERPRISE'      not null comment '所属频道',
    aggregate_id          bigint                                null comment '聚合表id',
    constraint sagm_choose_buy_pool_n2
        unique (sku_id, auth_list_code, agreement_line_id, org_id, tenant_id),
    index sagm_choose_buy_pool_n1 (auth_list_code, tenant_id, agreement_type),
    index sagm_choose_buy_pool_n3 (last_update_date),
    index sagm_choose_buy_pool_n4 (agreement_line_id, agreement_type),
    index sagm_choose_buy_pool_n5 (delete_flag, last_update_date),
    index sagm_choose_buy_pool_n6 (aggregate_id),
    %s (id)
)
    comment '选买池' %s %s]],
            table_num, id_def, id_index_def, engine_def, extra_table_options)

    con:query(query)

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into 'sagm_choose_buy_pool%d'",
                sysbench.opt.table_size, table_num))
    end

    if sysbench.opt.auto_inc then
        query = "INSERT INTO sagm_choose_buy_pool" .. table_num .. "(auth_list_code, sku_id, show_supplier_name, org_id, price,agreement_type, agreement_header_id, agreement_line_id, shelf_flag, tenant_id, aggregate_id) VALUES"
    else
        query = "INSERT INTO sagm_choose_buy_pool" .. table_num .. "(id,auth_list_code, sku_id, show_supplier_name, org_id, price,agreement_type, agreement_header_id, agreement_line_id, shelf_flag, tenant_id, aggregate_id) VALUES"
    end

    con:bulk_insert_init(query)

    local auth_list_code_val
    local show_supplier_name_val

    for i = 1, sysbench.opt.table_size do

        auth_list_code_val = get_auth_list_code_value()
        show_supplier_name_val=get_show_supplier_name_value()

        if (sysbench.opt.auto_inc) then
            query = string.format("('%s',%d,'%s',%d,%d,'PUR_AGREEMENT',%d,%d,%d,%d,%d )",
                    auth_list_code_val,sb_rand(1, sysbench.opt.table_size),show_supplier_name_val, sb_rand(1, sysbench.opt.table_size),
                    sb_rand(1, sysbench.opt.table_size),sb_rand(1, sysbench.opt.table_size),sb_rand(1, sysbench.opt.table_size),1,sb_rand(1, sysbench.opt.table_size),-1
                    )
        else
            query = string.format("(%d,'%s',%d,'%s',%d,%d,'PUR_AGREEMENT',%d,%d,%d,%d,%d )",
                    i, auth_list_code_val, sb_rand(1, sysbench.opt.table_size), show_supplier_name_val, sb_rand(1, sysbench.opt.table_size),
                    sb_rand(1, sysbench.opt.table_size), sb_rand(1, sysbench.opt.table_size), sb_rand(1, sysbench.opt.table_size), 1, sb_rand(1, sysbench.opt.table_size), -1
            )
        end

        con:bulk_insert_next(query)
    end

    con:bulk_insert_done()

    if sysbench.opt.create_secondary then
        print(string.format("Creating a secondary index on 'sagm_choose_buy_pool%d'...",
                table_num))
        con:query(string.format("CREATE INDEX k_%d ON sagm_choose_buy_pool%d(k)",
                table_num, table_num))
    end
end

local t = sysbench.sql.type
local stmt_defs = {
    point_selects = {
        "SELECT id,auth_list_code, sku_id,show_supplier_name,org_id,price,agreement_type,agreement_header_id,agreement_line_id,shelf_flag,tenant_id,object_version_number,creation_date,created_by,last_updated_by,last_update_date,price_hidden_flag,delete_flag,channel,aggregate_id FROM sagm_choose_buy_pool%u WHERE id=?",
        t.BIGINT},
    simple_ranges = {
        "SELECT id,auth_list_code, sku_id,show_supplier_name,org_id,price,agreement_type,agreement_header_id,agreement_line_id,shelf_flag,tenant_id,object_version_number,creation_date,created_by,last_updated_by,last_update_date,price_hidden_flag,delete_flag,channel,aggregate_id FROM sagm_choose_buy_pool%u WHERE id BETWEEN ? AND ?",
        t.BIGINT, t.BIGINT},
    sum_ranges = {
        "SELECT SUM(price) FROM sagm_choose_buy_pool%u WHERE id BETWEEN ? AND ?",
        t.BIGINT, t.BIGINT},
    order_ranges = {
        "SELECT id,auth_list_code, sku_id,show_supplier_name,org_id,price,agreement_type,agreement_header_id,agreement_line_id,shelf_flag,tenant_id,object_version_number,creation_date,created_by,last_updated_by,last_update_date,price_hidden_flag,delete_flag,channel,aggregate_id FROM sagm_choose_buy_pool%u WHERE id BETWEEN ? AND ? ORDER BY sku_id",
        t.BIGINT, t.BIGINT},
    distinct_ranges = {
        "SELECT DISTINCT sku_id FROM sagm_choose_buy_pool%u WHERE id BETWEEN ? AND ? ORDER BY sku_id",
        t.BIGINT, t.BIGINT},
    index_updates = {
        "UPDATE sagm_choose_buy_pool%u SET price=price+1 WHERE id=?",
        t.BIGINT},
    non_index_updates = {
        "UPDATE sagm_choose_buy_pool%u SET shelf_flag=0 WHERE id=?",
        t.BIGINT},
    deletes = {
        "DELETE FROM sagm_choose_buy_pool%u WHERE id=?",
        t.BIGINT},
    inserts = {
        "INSERT INTO sagm_choose_buy_pool%u (auth_list_code, sku_id, show_supplier_name, org_id, price,agreement_type, agreement_header_id, agreement_line_id, shelf_flag, tenant_id, aggregate_id) VALUES (?, ?, ?, ?,?,'PUR_AGREEMENT',?,?,1,?,-1)",
        {t.CHAR, 30},t.BIGINT,{t.CHAR, 100}, t.BIGINT,t.DOUBLE,t.BIGINT,t.BIGINT,t.BIGINT },
}

function prepare_begin()
    stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
    stmt.commit = con:prepare("COMMIT")
end

function prepare_for_each_table(key)
    for t = 1, sysbench.opt.tables do
        stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t))

        local nparam = #stmt_defs[key] - 1

        if nparam > 0 then
            param[t][key] = {}
        end

        for p = 1, nparam do
            local btype = stmt_defs[key][p+1]
            local len

            if type(btype) == "table" then
                len = btype[2]
                btype = btype[1]
            end
            if btype == sysbench.sql.type.VARCHAR or
                    btype == sysbench.sql.type.CHAR then
                param[t][key][p] = stmt[t][key]:bind_create(btype, len)
            else
                param[t][key][p] = stmt[t][key]:bind_create(btype)
            end
        end

        if nparam > 0 then
            stmt[t][key]:bind_param(unpack(param[t][key]))
        end
    end
end

function prepare_point_selects()
    prepare_for_each_table("point_selects")
end

function prepare_simple_ranges()
    prepare_for_each_table("simple_ranges")
end

function prepare_sum_ranges()
    prepare_for_each_table("sum_ranges")
end

function prepare_order_ranges()
    prepare_for_each_table("order_ranges")
end

function prepare_distinct_ranges()
    prepare_for_each_table("distinct_ranges")
end

function prepare_index_updates()
    prepare_for_each_table("index_updates")
end

function prepare_non_index_updates()
    prepare_for_each_table("non_index_updates")
end

function prepare_delete_inserts()
    prepare_for_each_table("deletes")
    prepare_for_each_table("inserts")
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    -- Create global nested tables for prepared statements and their
    -- parameters. We need a statement and a parameter set for each combination
    -- of connection/table/query
    stmt = {}
    param = {}

    for t = 1, sysbench.opt.tables do
        stmt[t] = {}
        param[t] = {}
    end

    -- This function is a 'callback' defined by individual benchmark scripts
    prepare_statements()
end

-- Close prepared statements
function close_statements()
    for t = 1, sysbench.opt.tables do
        for k, s in pairs(stmt[t]) do
            stmt[t][k]:close()
        end
    end
    if (stmt.begin ~= nil) then
        stmt.begin:close()
    end
    if (stmt.commit ~= nil) then
        stmt.commit:close()
    end
end

function thread_done()
    close_statements()
    con:disconnect()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    for i = 1, sysbench.opt.tables do
        print(string.format("Dropping table 'sagm_choose_buy_pool%d'...", i))
        con:query("DROP TABLE IF EXISTS sagm_choose_buy_pool" .. i )
    end
end

local function get_table_num()
    return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_id()
    return sysbench.rand.default(1, sysbench.opt.table_size)
end

function begin()
    stmt.begin:execute()
end

function commit()
    stmt.commit:execute()
end

function execute_point_selects()
    local tnum = get_table_num()
    local i

    for i = 1, sysbench.opt.point_selects do
        param[tnum].point_selects[1]:set(get_id())

        stmt[tnum].point_selects:execute()
    end
end

local function execute_range(key)
    local tnum = get_table_num()

    for i = 1, sysbench.opt[key] do
        local id = get_id()

        param[tnum][key][1]:set(id)
        param[tnum][key][2]:set(id + sysbench.opt.range_size - 1)

        stmt[tnum][key]:execute()
    end
end

function execute_simple_ranges()
    execute_range("simple_ranges")
end

function execute_sum_ranges()
    execute_range("sum_ranges")
end

function execute_order_ranges()
    execute_range("order_ranges")
end

function execute_distinct_ranges()
    execute_range("distinct_ranges")
end

function execute_index_updates()
    local tnum = get_table_num()

    for i = 1, sysbench.opt.index_updates do
        param[tnum].index_updates[1]:set(get_id())
        stmt[tnum].index_updates:execute()
    end
end

function execute_non_index_updates()
    local tnum = get_table_num()

    for i = 1, sysbench.opt.non_index_updates do
        param[tnum].non_index_updates[1]:set(get_id())

        stmt[tnum].non_index_updates:execute()
    end
end

function execute_delete_inserts()
    local tnum = get_table_num()

    for i = 1, sysbench.opt.delete_inserts do
        local id = get_id()
        param[tnum].deletes[1]:set(id)
        param[tnum].inserts[1]:set_rand_str(auth_list_code_value_template)
        param[tnum].inserts[2]:set(sb_rand(1, sysbench.opt.table_size))
        param[tnum].inserts[3]:set_rand_str(show_supplier_name_template)
        param[tnum].inserts[4]:set(sb_rand(1, sysbench.opt.table_size))
        param[tnum].inserts[5]:set(sb_rand(1, sysbench.opt.table_size))
        param[tnum].inserts[6]:set(sb_rand(1, sysbench.opt.table_size))
        param[tnum].inserts[7]:set(sb_rand(1, sysbench.opt.table_size))
        param[tnum].inserts[8]:set(sb_rand(1, sysbench.opt.table_size))

        stmt[tnum].deletes:execute()
        stmt[tnum].inserts:execute()
    end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
    if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
            errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
            errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
            errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
    then
        close_statements()
        prepare_statements()
    end
end
