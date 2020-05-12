import json
import datetime

from backend import models
from backend.database_schema import get_schema, get_column
from backend.database_schema import create_conn, execute_transaction
from backend.database_schema import timeit
from backend.physicalconfig import hub_sql, sat_sql, link_sql
from backend.physicalconfig import hub_load_sql, sat_load_sql, link_load_sql


def add_to_logfile(msg):
    with open('engine_log.txt', 'a+') as f:
        f.write(msg + '\n')


def get_databases():
    source_db = json.load(open('./source_database.json', 'r'))
    target_db = json.load(open('./target_database.json', 'r'))
    return source_db, target_db

def load_mapping(mapping_path='./mapping.json'):
    return json.load(open(mapping_path))

def load_config(config_path='./engine_config.json'):
    return json.load(open(config_path))


def get_hub_def(mapping, hub_name):
    for hub_def in mapping['hubs']:
        if hub_def["name"] == hub_name:
            return hub_def


def create_hub_ddl(hub_def, config):
    ddl = hub_sql.format(
        hub_name=hub_def["name"],
        hk_dtype=config['hk_dtype'],
        bk_dtype=config['bk_dtype'],
        rec_src_dtype=config['rec_src_dtype']
    )
    return ddl


def create_sat_ddl(sat_def, source_db, config, mapping):
    other_col = '{} {}'
    other_cols = []
    for col in sat_def['columns']:
        col_metadata = get_column(
            table=col['source_table'],
            column=col['source_col'],
            database=source_db
        )

        if len(col_metadata) == 0:
            raise Exception('Column metadata not found!')

        other_cols.append(
            other_col.format(
                col['target'],
                col_metadata[0]['data_type']
            )
        )
    ddl = sat_sql.format(
        sat_name=sat_def['name'],
        hk_dtype=config['hk_dtype'],
        bk_dtype=config['bk_dtype'],
        rec_src_dtype=config['rec_src_dtype'],
        other_cols=',\n    '.join(other_cols)
    )
    return ddl


def create_link_ddl(link_def, config):
    ddl = link_sql.format(
        link_name=link_def['name'],
        hk_dtype=config['hk_dtype'],
        bk_dtype=config['bk_dtype'],
        left_hub=link_def['hub_left'],
        right_hub=link_def['hub_right']
    )
    return ddl


def create_hub_sql(hub_def, config):

    if hub_def["bk"]['transform']:
        source_col = hub_def["bk"]['transform'].format(hub_def["bk"]["column"])
    else:
        source_col = hub_def["bk"]["column"]

    dv_source = '.'.join([
        hub_def["bk"]["database"],
        hub_def["bk"]["schema"],
        hub_def["bk"]["table"]
    ])

    sql = hub_load_sql.format(
        hub_name=hub_def["name"],
        bk_name=source_col,
        hash_func=config['hash_func'],
        load_time=str(datetime.datetime.now())[:-3],
        dv_source=dv_source,
        bk_table=dv_source
    )

    return sql


def create_sat_sql(sat_def, source_db, config, mapping):
    other_cols = []
    rowhash_field_list = []
    for col in sat_def['columns']:
        if col['transform']:
            source_col = col['transform'].format(col['source_col'])
        else:
            source_col = col['source_col']

        other_col = '{} AS {}'.format(
            source_col,
            col['target']
        )

        rowhash_field_list.append('{}'.format(source_col))

        other_cols.append(other_col)

    hub_def = get_hub_def(mapping, sat_def['hub'])

    dv_source = '.'.join([
        hub_def["bk"]["database"],
        hub_def["bk"]["schema"],
        hub_def["bk"]["table"],
    ])
    
    if len(rowhash_field_list) > 1:
        rowhash_fields = ", '{}', ".format(
            config['bk_delim']
        ).join(rowhash_field_list)
        rowhash_fields = 'CONCAT({})'.format(rowhash_fields)
    else:
        rowhash_fields = 'CONVERT(NVARCHAR, {})'.format(
            rowhash_field_list[0]
        )

    if hub_def["bk"]['transform']:
        bk_col = hub_def["bk"]['transform'].format(hub_def["bk"]["column"])
    else:
        bk_col = hub_def["bk"]["column"]

    sql = sat_load_sql.format(
        sat_name=sat_def["name"],
        bk_name=bk_col,
        hash_func=config['hash_func'],
        load_time=str(datetime.datetime.now())[:-3],
        dv_source=dv_source,
        rowhash_fields=rowhash_fields,
        other_cols=',\n       '.join(other_cols),
        source_table=dv_source
    )

    return sql


def create_link_sql(link_def, source_db, config, mapping):
    left_hub_def = get_hub_def(mapping, link_def['hub_left'])
    left_hub_bk = '.'.join([
        left_hub_def["bk"]["database"], 
        left_hub_def["bk"]["schema"], 
        left_hub_def["bk"]["table"], 
        left_hub_def["bk"]["column"]
    ])
    right_hub_def = get_hub_def(mapping, link_def['hub_right'])
    right_hub_bk = '.'.join([
        right_hub_def["bk"]["database"], 
        right_hub_def["bk"]["schema"], 
        right_hub_def["bk"]["table"], 
        right_hub_def["bk"]["column"]
    ])

    sql = link_load_sql.format(
        link_name=link_def['name'],
        hash_func=config['hash_func'],
        bk_delim=config['bk_delim'],
        left_hub=link_def['hub_left'],
        right_hub=link_def['hub_right'],
        load_time=str(datetime.datetime.now())[:-3],
        left_bk=left_hub_bk,
        right_bk=right_hub_bk,
        source_from=link_def['source_from']
    )

    return sql

@timeit
def do_datavault_etl(mapping, config, source_db, target_db):
    hub_ddls = []
    hub_load_sqls = []
    print('Loading hubs...')
    add_to_logfile('Loading hubs...')
    for hub_def in mapping['hubs']:
        hub_ddl = create_hub_ddl(hub_def, config)
        hub_ddls.append(hub_ddl)
        hub_sql = create_hub_sql(hub_def, config)
        hub_load_sqls.append(hub_sql)

        print('creating and loading {}'.format(hub_def['name']))
        add_to_logfile('creating and loading {}'.format(hub_def['name']))

        print(
            '\n'.join([
                hub_ddl,
                hub_sql
            ]), 
        )

        execute_transaction(
            '\n'.join([
                hub_ddl,
                hub_sql
            ]), 
            target_db
        )
    print('Hubs loaded!')
    add_to_logfile('Hubs loaded!')

    sat_ddls = []
    sat_load_sqls = []
    print('loading sats...')
    add_to_logfile('loading sats...')
    for sat_def in mapping['sats']:
        sat_ddl = create_sat_ddl(sat_def, source_db, config, mapping)
        sat_ddls.append(sat_ddl)
        sat_sql = create_sat_sql(sat_def, source_db, config, mapping)
        sat_load_sqls.append(sat_sql)

        print('creating and loading {}'.format(sat_def['name']))
        add_to_logfile('creating and loading {}'.format(sat_def['name']))

        # print(
        #     '\n'.join([
        #         sat_ddl,
        #         sat_sql
        #     ]), 
        # )

        execute_transaction(
            '\n'.join([
                sat_ddl,
                sat_sql
            ]), 
            target_db
        )
    print('Sats loaded!')
    add_to_logfile('Sats loaded!')

    link_ddls = []
    link_load_sqls = []
    print('Loading links...')
    add_to_logfile('Loading links...')
    for link_def in mapping['links']:
        link_ddl = create_link_ddl(link_def, config)
        link_ddls.append(link_ddl)
        link_sql = create_link_sql(link_def, source_db, config, mapping)
        link_load_sqls.append(link_sql)

        print('creating and loading {}'.format(link_def['name']))
        add_to_logfile('creating and loading {}'.format(link_def['name']))

        # print('\n'.join([
        #         link_ddl,
        #         link_sql
        #     ]), )

        execute_transaction(
            '\n'.join([
                link_ddl,
                link_sql
            ]), 
            target_db
        )
    print('Links loaded!')
    add_to_logfile('Links loaded!')


def run_dv_engine():
    mapping = load_mapping()
    config = load_config()
    source_db, target_db = get_databases()
    do_datavault_etl(mapping, config, source_db, target_db)


if __name__ == '__main__':

    config = load_config()
    source_db, target_db = get_databases()
    do_datavault_etl(config, source_db, target_db)
