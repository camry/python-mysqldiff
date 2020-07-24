import click
import mysql.connector


@click.command()
@click.option("--source", required=True, help="指定源服务器。(格式: <user>:<password>@<host>:<port>)")
@click.option("--target", help="指定目标服务器。(格式: <user>:<password>@<host>:<port>)")
@click.option("--db", required=True, help="指定数据库。(格式: <source_db>:<target_db>)")
@click.pass_context
def mysqldiff(ctx, source, target, db):
    """差异 SQL 工具。"""
    source_cnx = None
    target_cnx = None

    try:
        source_index = source.rindex('@')
        source_user = source[0:source_index].split(':', 1)
        source_host = source[source_index + 1:].split(':', 1)

        databases = db.split(':', 1)

        source_db_config = {
            'user': source_user[0],
            'password': source_user[1],
            'host': source_host[0],
            'port': source_host[1],
            'database': 'information_schema',
            'charset': 'utf8',
            'autocommit': True,
            'raise_on_warnings': True
        }

        source_cnx = mysql.connector.connect(**source_db_config)

        source_cursor_schema = source_cnx.cursor(dictionary=True)

        source_query_schema = "SELECT * FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = '%s'" % \
                              databases[0]

        source_cursor_schema.execute(source_query_schema)

        source_schema_data = source_cursor_schema.fetchone()

        if source_cursor_schema.rowcount <= 0:
            raise Exception('源数据库 `%s` 不存在。' % databases[0])

        source_cursor_schema.close()

        source_cursor_table = source_cnx.cursor(dictionary=True)

        source_query_table = "SELECT * FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' " \
                             "ORDER BY `TABLE_NAME` ASC" % \
                             databases[0]

        source_cursor_table.execute(source_query_table)

        source_table_data_t = source_cursor_table.fetchall()

        if source_cursor_table.rowcount <= 0:
            raise Exception('源数据库 `%s` 没有数据表。' % databases[0])

        source_cursor_table.close()

        source_table_data_dic = {}

        for v in source_table_data_t:
            source_table_data_dic[v['TABLE_NAME']] = v

        if target is None:
            target_cnx = source_cnx
        else:
            target_index = target.rindex('@')
            target_user = target[0:target_index].split(':', 1)
            target_host = target[target_index + 1:].split(':', 1)

            target_db_config = {
                'user': target_user[0],
                'password': target_user[1],
                'host': target_host[0],
                'port': target_host[1],
                'database': 'information_schema',
                'charset': 'utf8',
                'autocommit': True,
                'raise_on_warnings': True,
            }

            target_cnx = mysql.connector.connect(**target_db_config)

        target_cursor_schema = target_cnx.cursor(dictionary=True)

        target_query_schema = "SELECT * FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = '%s'" % \
                              databases[1]

        target_cursor_schema.execute(target_query_schema)

        target_schema_data = target_cursor_schema.fetchone()

        if target_cursor_schema.rowcount <= 0:
            raise Exception('目标数据库 `%s` 不存在。' % databases[1])

        target_cursor_schema.close()

        target_cursor_table = target_cnx.cursor(dictionary=True)

        target_query_table = "SELECT * FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' " \
                             "ORDER BY `TABLE_NAME` ASC" % \
                             databases[1]

        target_cursor_table.execute(target_query_table)

        target_table_data_t = target_cursor_table.fetchall()

        if target_cursor_table.rowcount <= 0:
            raise Exception('目标数据库 `%s` 没有数据表。' % databases[1])

        target_cursor_table.close()

        target_table_data_dic = {}

        for v in target_table_data_t:
            target_table_data_dic[v['TABLE_NAME']] = v

        diff_sql = []

        # DROP TABLE...
        for target_table_name, target_table_data in target_table_data_dic.items():
            if target_table_name not in source_table_data_dic:
                diff_sql.append("DROP TABLE IF EXISTS `%s`;" % target_table_name)

        for source_table_name, source_table_data in source_table_data_dic.items():
            if source_table_name in target_table_data_dic:
                # ALTER TABLE
                source_cursor_column = source_cnx.cursor(dictionary=True)

                source_query_column = "SELECT * FROM `information_schema`.`COLUMNS` " \
                                      "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' " \
                                      "ORDER BY `ORDINAL_POSITION` ASC" % \
                                      (databases[0], source_table_name)

                source_cursor_column.execute(source_query_column)

                source_column_data_t = source_cursor_column.fetchall()
                source_column_data_count = source_cursor_column.rowcount

                source_cursor_column.close()

                target_cursor_column = target_cnx.cursor(dictionary=True)

                target_query_column = "SELECT * FROM `information_schema`.`COLUMNS` " \
                                      "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' " \
                                      "ORDER BY `ORDINAL_POSITION` ASC" % \
                                      (databases[1], source_table_name)

                target_cursor_column.execute(target_query_column)

                target_column_data_t = target_cursor_column.fetchall()
                target_column_data_count = target_cursor_column.rowcount

                target_cursor_column.close()

                if source_column_data_count > 0 and target_column_data_count > 0:
                    columns_local = {}
                    columns_online = {}
                    columns_pos_local = {}
                    columns_pos_online = {}

                    for source_column_data in source_column_data_t:
                        column_data = get_column(source_column_data)
                        columns_local[source_column_data['COLUMN_NAME']] = column_data
                        columns_pos_local[source_column_data['ORDINAL_POSITION']] = column_data

                    for target_column_data in target_column_data_t:
                        column_data = get_column(target_column_data)
                        columns_online[target_column_data['COLUMN_NAME']] = column_data
                        columns_pos_online[target_column_data['ORDINAL_POSITION']] = column_data

                    if columns_pos_local != columns_pos_online:
                        alter_tables = []
                        alter_columns = []

                        alter_tables.append("ALTER TABLE `%s`" % source_table_name)

                        for column_name, column_online in columns_online.items():
                            if column_name not in columns_local:
                                alter_columns.append("  DROP COLUMN `%s`" % column_name)

                        for column_name, column_local in columns_local.items():
                            if column_name in columns_online:
                                # MODIFY COLUMN
                                if column_local != columns_online[column_name]:
                                    null_able = get_column_default(column_local)

                                    extra = ''

                                    if column_local['EXTRA'] != '':
                                        extra = ' %s' % column_local['EXTRA'].upper()

                                    after = get_column_after(column_local['ORDINAL_POSITION'], columns_pos_local)

                                    # 重新计算字段位置
                                    columns_online = reset_calc_position(column_name, 0,
                                                                         column_local['ORDINAL_POSITION'],
                                                                         columns_online, True)

                                    alter_columns.append(
                                        "  MODIFY COLUMN `{column_name}` {column_type}{null_able}{extra} {after}".format
                                        (column_name=column_name, column_type=column_local['COLUMN_TYPE'],
                                         null_able=null_able, extra=extra, after=after))

                            else:
                                # ADD COLUMN
                                null_able = get_column_default(column_local)

                                extra = ''

                                if column_local['EXTRA'] != '':
                                    extra = ' %s' % column_local['EXTRA'].upper()

                                after = get_column_after(column_local['ORDINAL_POSITION'], columns_pos_local)

                                # 重新计算字段位置
                                columns_online = reset_calc_position(column_name, 0,
                                                                     column_local['ORDINAL_POSITION'], columns_online)

                                alter_columns.append(
                                    "  ADD COLUMN `{column_name}` {column_type}{null_able}{extra} {after}".format(
                                        column_name=column_name, column_type=column_local['COLUMN_TYPE'],
                                        null_able=null_able, extra=extra, after=after))

                        for alter_column in alter_columns:
                            if alter_column == alter_columns[-1]:
                                column_dot = ';'
                            else:
                                column_dot = ','

                            alter_tables.append('%s%s' % (alter_column, column_dot))

                        diff_sql.append('\n'.join(alter_tables))

                source_cursor_statistic = source_cnx.cursor(dictionary=True)

                source_query_statistic = "SELECT * FROM `information_schema`.`STATISTICS` " \
                                         "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'" % \
                                         (databases[0], source_table_name)

                source_cursor_statistic.execute(source_query_statistic)

                source_statistic_data_t = source_cursor_statistic.fetchall()
                source_statistic_data_count = source_cursor_statistic.rowcount

                source_cursor_statistic.close()

                target_cursor_statistic = target_cnx.cursor(dictionary=True)

                target_query_statistic = "SELECT * FROM `information_schema`.`STATISTICS` " \
                                         "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'" % \
                                         (databases[1], source_table_name)

                target_cursor_statistic.execute(target_query_statistic)

                target_statistic_data_t = target_cursor_statistic.fetchall()
                target_statistic_data_count = target_cursor_statistic.rowcount

                target_cursor_statistic.close()

                if source_statistic_data_count > 0 and target_statistic_data_count > 0:
                    statistics_local = {}
                    statistics_online = {}

                    for source_statistic_data in source_statistic_data_t:
                        if source_statistic_data['INDEX_NAME'] in statistics_local:
                            statistics_local[source_statistic_data['INDEX_NAME']].update({
                                source_statistic_data['SEQ_IN_INDEX']: get_statistic(source_statistic_data)
                            })
                        else:
                            statistics_local[source_statistic_data['INDEX_NAME']] = {
                                source_statistic_data['SEQ_IN_INDEX']: get_statistic(source_statistic_data)
                            }

                    for target_statistic_data in target_statistic_data_t:
                        if target_statistic_data['INDEX_NAME'] in statistics_online:
                            statistics_online[target_statistic_data['INDEX_NAME']].update({
                                target_statistic_data['SEQ_IN_INDEX']: get_statistic(target_statistic_data)
                            })
                        else:
                            statistics_online[target_statistic_data['INDEX_NAME']] = {
                                target_statistic_data['SEQ_IN_INDEX']: get_statistic(target_statistic_data)
                            }

                    if statistics_local != statistics_online:
                        alter_tables = []
                        alter_keys = []

                        alter_tables.append("ALTER TABLE `%s`" % source_table_name)

                        for index_name, statistic_online in statistics_online.items():
                            if index_name not in statistics_local:
                                if 'PRIMARY' == index_name:
                                    alter_keys.append("  DROP PRIMARY KEY")
                                else:
                                    alter_keys.append("  DROP INDEX `%s`" % index_name)

                        for index_name, statistic_local in statistics_local.items():
                            if index_name in statistics_online:
                                # DROP INDEX ... AND ADD KEY ...
                                if statistic_local != statistics_online[index_name]:
                                    if 'PRIMARY' == index_name:
                                        alter_keys.append("  DROP PRIMARY KEY")
                                    else:
                                        alter_keys.append("  DROP INDEX `%s`" % index_name)

                                    alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic_local))
                            else:
                                # ADD KEY
                                alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic_local))

                        if alter_keys:
                            for alter_key in alter_keys:
                                if alter_key == alter_keys[-1]:
                                    key_dot = ';'
                                else:
                                    key_dot = ','

                                alter_tables.append('%s%s' % (alter_key, key_dot))

                            diff_sql.append('\n'.join(alter_tables))

            else:
                # CREATE TABLE...
                source_cursor_column = source_cnx.cursor(dictionary=True)

                source_query_column = "SELECT * FROM `information_schema`.`COLUMNS` " \
                                      "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' " \
                                      "ORDER BY `ORDINAL_POSITION` ASC" % \
                                      (databases[0], source_table_name)

                source_cursor_column.execute(source_query_column)

                source_column_data = source_cursor_column.fetchall()
                source_column_data_count = source_cursor_column.rowcount

                source_cursor_column.close()

                if source_column_data_count > 0:
                    source_cursor_statistics = source_cnx.cursor(dictionary=True)

                    source_query_statistics = "SELECT * FROM `information_schema`.`STATISTICS` " \
                                              "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'" % (
                                                  databases[0], source_table_name)

                    source_cursor_statistics.execute(source_query_statistics)

                    source_statistics_data_t = source_cursor_statistics.fetchall()
                    source_statistics_data_count = source_cursor_statistics.rowcount

                    source_cursor_statistics.close()

                    create_tables = ["CREATE TABLE IF NOT EXISTS `%s` (" % source_table_name]

                    # COLUMN...
                    for column in source_column_data:
                        null_able = get_column_default(column)

                        extra = dot = ''

                        if column['EXTRA'] != '':
                            extra = ' %s' % column['EXTRA'].upper()

                        if column != source_column_data[-1] or source_statistics_data_count > 0:
                            dot = ','

                        create_tables.append(
                            "  `{column_name}` {column_type}{null_able}{extra}{dot}".format(
                                column_name=column['COLUMN_NAME'], column_type=column['COLUMN_TYPE'],
                                null_able=null_able, extra=extra, dot=dot))

                    # KEY...
                    create_tables_keys = []
                    if source_statistics_data_count > 0:
                        source_statistics_data_dic = {}

                        for source_statistics_data in source_statistics_data_t:
                            if source_statistics_data['INDEX_NAME'] in source_statistics_data_dic:
                                source_statistics_data_dic[source_statistics_data['INDEX_NAME']].update({
                                    source_statistics_data['SEQ_IN_INDEX']: source_statistics_data
                                })
                            else:
                                source_statistics_data_dic[source_statistics_data['INDEX_NAME']] = {
                                    source_statistics_data['SEQ_IN_INDEX']: source_statistics_data
                                }

                        for index_name, source_statistics_data in source_statistics_data_dic.items():
                            create_tables_keys.append(
                                "  {key_slot}".format(key_slot=get_add_keys(index_name, source_statistics_data)))

                    create_tables.append(",\n".join(create_tables_keys))
                    create_tables.append(
                        ") ENGINE={engine} DEFAULT CHARSET={charset};".format(engine=source_table_data['ENGINE'],
                                                                              charset=source_schema_data[
                                                                                  'DEFAULT_CHARACTER_SET_NAME']))
                    diff_sql.append("\n".join(create_tables))

        if diff_sql:
            click.echo('SET NAMES %s;\n' % source_schema_data['DEFAULT_CHARACTER_SET_NAME'])
            click.echo("\n\n".join(diff_sql))
        else:
            click.secho('数据库表结构一致。', fg='green')

    except Exception as e:
        click.secho('ERROR: %s' % e, fg='red')
    finally:
        if source_cnx is not None:
            source_cnx.close()
        if target_cnx is not None:
            target_cnx.close()


def get_column(column):
    return {
        'COLUMN_NAME': column['COLUMN_NAME'],
        'ORDINAL_POSITION': column['ORDINAL_POSITION'],
        'COLUMN_DEFAULT': column['COLUMN_DEFAULT'],
        'IS_NULLABLE': column['IS_NULLABLE'],
        'DATA_TYPE': column['DATA_TYPE'],
        'CHARACTER_MAXIMUM_LENGTH': column['CHARACTER_MAXIMUM_LENGTH'],
        'CHARACTER_OCTET_LENGTH': column['CHARACTER_OCTET_LENGTH'],
        'NUMERIC_PRECISION': column['NUMERIC_PRECISION'],
        'NUMERIC_SCALE': column['NUMERIC_SCALE'],
        'DATETIME_PRECISION': column['DATETIME_PRECISION'],
        'CHARACTER_SET_NAME': column['CHARACTER_SET_NAME'],
        'COLLATION_NAME': column['COLLATION_NAME'],
        'COLUMN_TYPE': column['COLUMN_TYPE'],
        'EXTRA': column['EXTRA']
    }


def get_column_default(column):
    if column['IS_NULLABLE'] == 'YES':
        null_able = ' DEFAULT NULL'
    else:
        if column['COLUMN_DEFAULT'] is not None:
            if column['COLUMN_DEFAULT'] in ['CURRENT_TIMESTAMP']:
                null_able = " NOT NULL DEFAULT %s" % column['COLUMN_DEFAULT']
            else:
                null_able = " NOT NULL DEFAULT '%s'" % column['COLUMN_DEFAULT']

        else:
            null_able = " NOT NULL"

    return null_able


def get_column_after(ordinal_position, column_pos):
    pos = ordinal_position - 1

    if pos in column_pos:
        return "AFTER `%s`" % column_pos[pos]['COLUMN_NAME']
    else:
        return "FIRST"


def get_add_keys(index_name, statistic):
    non_unique = statistic[1]['NON_UNIQUE']

    if 1 == non_unique:
        columns_name = []

        for k in sorted(statistic):
            sub_part = ''

            if statistic[k]['SUB_PART'] is not None:
                sub_part = '(%d)' % statistic[k]['SUB_PART']

            columns_name.append(
                "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

        return "KEY `{index_name}` ({columns_name})".format(index_name=index_name, columns_name=",".join(columns_name))
    else:
        columns_name = []

        if 'PRIMARY' == index_name:

            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}{sub_part}`".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "PRIMARY KEY ({columns_name})".format(columns_name=",".join(columns_name))
        else:
            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "UNIQUE KEY `{index_name}` ({columns_name})".format(index_name=index_name,
                                                                       columns_name=",".join(columns_name))


def reset_calc_position(column_name, online_pos, local_pos, columns_online, is_modify=False):
    for k, v in columns_online.items():
        cur_pos = v['ORDINAL_POSITION']

        if is_modify is True:
            if local_pos <= cur_pos < online_pos:
                columns_online[k]['ORDINAL_POSITION'] = columns_online[k]['ORDINAL_POSITION'] + 1
        else:
            if cur_pos >= local_pos:
                columns_online[k]['ORDINAL_POSITION'] = columns_online[k]['ORDINAL_POSITION'] + 1

        if column_name in columns_online:
            columns_online[column_name]['ORDINAL_POSITION'] = local_pos

    return columns_online


def get_statistic(statistic):
    return {
        'NON_UNIQUE': statistic['NON_UNIQUE'],
        'INDEX_NAME': statistic['INDEX_NAME'],
        'SEQ_IN_INDEX': statistic['SEQ_IN_INDEX'],
        'COLUMN_NAME': statistic['COLUMN_NAME'],
        'SUB_PART': statistic['SUB_PART'],
        'INDEX_TYPE': statistic['INDEX_TYPE']
    }


if __name__ == '__main__':
    mysqldiff(obj={})
