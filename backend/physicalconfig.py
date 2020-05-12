
# define minimum columns for hubs

hub_sql = '''
DROP TABLE IF EXISTS {hub_name};

CREATE TABLE {hub_name} (
    HK {hk_dtype},
    BK {bk_dtype},
    DV_RECORD_DATETIME DATETIME,
    DV_RECORD_SOURCE {rec_src_dtype},
    PRIMARY KEY (HK),
    INDEX IND (HK, DV_RECORD_DATETIME, DV_RECORD_SOURCE)
);
'''

sat_sql = '''
DROP TABLE IF EXISTS {sat_name};

CREATE TABLE {sat_name} (
    HK {hk_dtype},
    BK {bk_dtype},
    DV_RECORD_DATETIME DATETIME,
    DV_RECORD_SOURCE {rec_src_dtype},
    ROWHASH {hk_dtype},
    {other_cols},
    PRIMARY KEY (HK, DV_RECORD_DATETIME),
    INDEX IND (HK, DV_RECORD_DATETIME, DV_RECORD_SOURCE)
);
'''

link_sql = '''
DROP TABLE IF EXISTS {link_name};

CREATE TABLE {link_name} (
    HK {hk_dtype},
    BK {bk_dtype},
    DV_RECORD_DATETIME DATETIME,
    BK_{left_hub} {bk_dtype},
    HK_{left_hub} {hk_dtype},
    BK_{right_hub} {bk_dtype},
    HK_{right_hub} {hk_dtype},
    PRIMARY KEY (HK),
    INDEX IND (HK, DV_RECORD_DATETIME)
);
'''

hub_load_sql = '''
INSERT INTO {hub_name}
SELECT DISTINCT
       HASHBYTES('{hash_func}', CONVERT(NVARCHAR, {bk_name})) AS HK, 
       {bk_name} AS BK,
       CONVERT(DATETIME, '{load_time}', 21) AS DV_RECORD_DATETIME,
       '{dv_source}' AS DV_RECORD_SOURCE
  FROM {bk_table};
'''

sat_load_sql = '''
INSERT INTO {sat_name}
SELECT HASHBYTES('{hash_func}', CONVERT(NVARCHAR, {bk_name})) AS HK, 
       {bk_name} AS BK,
       CONVERT(DATETIME, '{load_time}', 21) AS DV_RECORD_DATETIME,
       '{dv_source}' AS DV_RECORD_SOURCE,
       HASHBYTES('{hash_func}', {rowhash_fields}) AS ROWHASH,
       {other_cols}
  FROM {source_table};
'''

link_load_sql = '''
INSERT INTO {link_name}
SELECT HASHBYTES('{hash_func}', CONVERT(NVARCHAR, CONCAT({left_bk}, '{bk_delim}', {right_bk}))) AS HK, 
       CONCAT({left_bk}, '{bk_delim}', {right_bk}) AS BK,
       CONVERT(DATETIME, '{load_time}', 21) AS DV_RECORD_DATETIME,
       {left_bk} AS BK_{left_hub},
       HASHBYTES('{hash_func}', CONVERT(NVARCHAR, {left_bk})) AS HK_{left_hub},
       {right_bk} AS BK_{right_hub},
       HASHBYTES('{hash_func}', CONVERT(NVARCHAR, {right_bk})) AS HK_{right_hub}
  FROM {source_from};
'''

transaction_sql = '''
BEGIN TRY
    BEGIN TRANSACTION 
        
        {sql}

    COMMIT
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
        DECLARE @ErrorState INT = ERROR_STATE()

    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
'''







hub_cols = [
    {
        'name': '*HASH*', 
        'data_type': 'varbinary', 
        'char_max_len': 32,
        'num_precision': 0,
        'ordinal_pos': 1,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'ETL_INSERT_RUN_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 2,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': 'LOAD_DATETIME', 
        'data_type': 'datetime', 
        'char_max_len': 0,
        'num_precision': 0,
        'ordinal_pos': 3,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'RECORD_SOURCE', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 4,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': '*BUSINESSKEY*', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 5,
        'is_pk': True,
        'is_ma': False
    }
]

# link columns
link_cols = [
    {
        'name': '*HASH*', 
        'data_type': 'varbinary', 
        'char_max_len': 32,
        'num_precision': 0,
        'ordinal_pos': 1,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'ETL_INSERT_RUN_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 2,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': 'LOAD_DATETIME', 
        'data_type': 'datetime', 
        'char_max_len': 0,
        'num_precision': 0,
        'ordinal_pos': 3,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'RECORD_SOURCE', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 4,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': '*LEFTTABLE*', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 5,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': '*RIGHTTABLE*', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 6,
        'is_pk': False,
        'is_ma': False
    }
]

# define minimum columns for sats
sat_cols = [
    {
        'name': '*HASH*', 
        'data_type': 'varbinary', 
        'char_max_len': 32,
        'num_precision': 0,
        'ordinal_pos': 1,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'ETL_INSERT_RUN_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 2,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': 'LOAD_DATETIME', 
        'data_type': 'datetime', 
        'char_max_len': 0,
        'num_precision': 0,
        'ordinal_pos': 3,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'LOAD_END_DATETIME', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 4,
        'is_pk': False,
        'is_ma': False
    }, {
        'name': 'CURRENT_RECORD_INDICATOR', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 5,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'ETL_INSERT_RUN_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 6,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'ETL_UPDATE_RUN_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 7,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'CDC_OPERATION', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 8,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'SOURCE_ROW_ID', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 9,
        'is_pk': True,
        'is_ma': False
    }, {
        'name': 'RECORD_SOURCE', 
        'data_type': 'nvarchar', 
        'char_max_len': 255,
        'num_precision': 0,
        'ordinal_pos': 10,
        'is_pk': True,
        'is_ma': False
    }
]

# # define minimum columns for lsats
# lsat_cols = [
#     {
#         'name': '*HASH*', 
#         'data_type': 'varbinary', 
#         'char_max_len': 32,
#         'num_precision': 0,
#         'ordinal_pos': 1,
#         'is_pk': True,
#         'is_ma': False
#     }, {
#         'name': 'ETL_INSERT_RUN_ID', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 2,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'LOAD_DATETIME', 
#         'data_type': 'datetime', 
#         'char_max_len': 0,
#         'num_precision': 0,
#         'ordinal_pos': 3,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'LOAD_END_DATETIME', 
#         'data_type': 'datetime', 
#         'char_max_len': 0,
#         'num_precision': 0,
#         'ordinal_pos': 4,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'CURRENT_RECORD_INDICATOR', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 5,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'ETL_INSERT_RUN_ID', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 6,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'ETL_UPDATE_RUN_ID', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 7,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'CDC_OPERATION', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 8,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'SOURCE_ROW_ID', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 9,
#         'is_pk': False,
#         'is_ma': False
#     }, {
#         'name': 'RECORD_SOURCE', 
#         'data_type': 'nvarchar', 
#         'char_max_len': 255,
#         'num_precision': 0,
#         'ordinal_pos': 10,
#         'is_pk': False,
#         'is_ma': False
#     }
# ]