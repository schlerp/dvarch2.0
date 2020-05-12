import json
from hashlib import md5

from backend.physicalconfig import hub_cols, link_cols, sat_cols


def create_attribute_hash(version_id, staging_table, staging_column, 
                          integration_table, integration_column, 
                          transformation_rule, hash_func=md5):
    '''creates a hash in the TEAM metadataformat'''

    hash_list = [str(version_id), staging_table, staging_column, 
                 integration_table, integration_column, transformation_rule]
    unhashed_hash = '|'.join(hash_list)
    return hash_func(unhashed_hash.encode('utf8')).hexdigest()


def create_physical_json_column(version_id, staging_table, staging_column, 
                                integration_table, integration_column, 
                                transformation_rule, database_name, schema_name, 
                                table_name, column_name, data_type, char_max_len, 
                                num_precision, ordinal_pos, is_pk, is_ma):
    
    att_hash = create_attribute_hash(version_id, staging_table, staging_column, 
                           integration_table, integration_column, transformation_rule)

    col_dict = {
        "versionAttributeHash": att_hash,
        "versionId": version_id,
        "databaseName": database_name,
        "schemaName": schema_name,
        "tableName": table_name,
        "columnName": column_name,
        "dataType": data_type,
        "characterMaximumLength": char_max_len,
        "numericPrecision": num_precision,
        "ordinalPosition": ordinal_pos,
        "primaryKeyIndicator": 'Y' if is_pk else 'N',
        "multiActiveIndicator": 'Y' if is_ma else 'N'
    }
    return col_dict


def create_physical_json(json_data):
    '''creates teh physical json for TEAM'''
    # load json data and fetch node list (dv table list)
    nodes = json_data['nodes']

    version_id = 0 

    col_defs = []

    for node in nodes:
        
        # set up table name
        table_name = node['data']['title']
        schema_name = node['data']['schema']

        if node['data']['type'] == 'hub':
            # used for hubs
            cols = hub_cols
            bk = node['data']['bk']

        elif node['data']['type'] == 'link':
            # used for links
            cols = link_cols
            left_table = node['data']['hubs'][0]
            right_table = node['data']['hubs'][1]

        elif node['data']['type'] in ('satellite', 'linksatellite'):
            # used for sats
            cols = sat_cols

        else:
            # ignore node!
            continue
        
        # loop through required cols for this DV table type
        for col in cols:

            # set place holders for now since we havent 
            # designed the server architecture
            database_name = 'PLACEHOLDER'
            staging_table = 'PLACEHOLDER'
            staging_column = 'PLACEHOLDER'
            integration_table = 'PLACEHOLDER'
            integration_column = 'PLACEHOLDER'
            transformation_rule = 'PLACEHOLDER'

            if col['name'] == '*HASH*':
                col_name = '{}_HSH'.format(table_name)

            # for hubs these col names need to be set on the fly
            elif col['name'] == '*BUSINESSKEY*':
                col_name = '{}_ID'.format(bk)

            # for links these col names need to be set on the fly
            elif col['name'] == '*LEFTTABLE*':
                col_name = '{}_HSH'.format(left_table)
            elif col['name'] == '*RIGHTTABLE*':
                col_name = '{}_HSH'.format(right_table)

            else:
                col_name = col['name']
            
            col_object = create_physical_json_column(
                version_id, staging_table, staging_column, 
                integration_table, integration_column, 
                transformation_rule, database_name, schema_name, 
                table_name, col_name, col['data_type'], col['char_max_len'], 
                col['num_precision'], col['ordinal_pos'], col['is_pk'], 
                col['is_ma']
            )

            col_defs.append(col_object)
    return json.dumps(col_defs, indent=2)


if __name__ == '__main__':
    json_dag = '{"nodes":[{"id":2,"title":"H_PERSON","x":-6,"y":92,"data":{"type":"hub","schema":"hub","bk":"","satellites":["S_DEMOGRAPHICS"],"links":["L_ADDRESS_PERSON","L_ALIAS_PERSON"],"title":"H_PERSON"}},{"id":3,"title":"L_ALIAS_PERSON","x":160,"y":224,"data":{"type":"link","schema":"lnk","bk":"","hubs":["H_PERSON","H_ALIAS"],"linksatellites":[],"title":"L_ALIAS_PERSON"}},{"id":4,"title":"H_ALIAS","x":244,"y":71,"data":{"type":"hub","schema":"hub","bk":"","satellites":["S_ALIAS"],"links":["L_ALIAS_PERSON"],"title":"H_ALIAS"}},{"id":5,"title":"S_ALIAS","x":372,"y":214,"data":{"type":"satellite","schema":"sat","bk":"","hubs":["H_ALIAS"],"title":"S_ALIAS"}},{"id":6,"title":"L_ADDRESS_PERSON","x":-100,"y":227,"data":{"type":"link","schema":"lnk","bk":"","hubs":["H_ADDRESS","H_PERSON"],"linksatellites":["LS_ADDRESSPERIOD"],"title":"L_ADDRESS_PERSON"}},{"id":7,"title":"S_ADDRESS","x":-320,"y":130,"data":{"type":"satellite","schema":"sat","bk":"","hubs":["H_ADDRESS"],"title":"S_ADDRESS"}},{"id":8,"title":"H_ADDRESS","x":-180,"y":53,"data":{"type":"hub","schema":"hub","bk":"","satellites":["S_ADDRESS"],"links":["L_ADDRESS_PERSON"],"title":"H_ADDRESS"}},{"id":9,"title":"S_DEMOGRAPHICS","x":105,"y":-23,"data":{"type":"satellite","schema":"sat","bk":"","hubs":["H_PERSON"],"title":"S_DEMOGRAPHICS"}},{"id":10,"title":"LS_ADDRESSPERIOD","x":-32,"y":382,"data":{"type":"linksatellite","schema":"sat","bk":"","links":["L_ADDRESS_PERSON"],"title":"LS_ADDRESSPERIOD"}}],"edges":[{"source":7,"target":8},{"source":8,"target":6},{"source":6,"target":2},{"source":2,"target":3},{"source":3,"target":4},{"source":4,"target":5},{"source":2,"target":9},{"source":10,"target":6}]}'
    json_string = create_physical_json(json_dag)

    with open('test_physical_format.json', 'w+') as f:
        f.write(json_string)