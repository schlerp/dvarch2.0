from flask import request
from flask import abort
from flask import redirect
from flask import url_for
from flask import jsonify
import json
from random import randint

from backend.physical import create_physical_json
from backend.database_schema import get_schema
from backend.models import Database, ConfigFile
from backend.dvetl import run_dv_engine
from backend import db


# testing api
def random_number():
    response = {
        'randomNumber': randint(1, 100)
    }
    return jsonify(response)


# mapping api
def get_mapping():
    data = json.load(open('./mapping.json', 'r'))
    return jsonify(data)

def save_mapping():
    if not request.json:
        abort(400)
    with open('./mapping.json', 'w+') as f:
        json.dump(request.json, f, indent=2)
    return jsonify({'status': 200})


# database api
def get_source_database():
    data = json.load(open('./source_database.json', 'r'))
    return jsonify(data)

def get_target_database():
    data = json.load(open('./target_database.json', 'r'))
    return jsonify(data)

def save_source_database():
    if not request.json:
        abort(400)
    with open('./source_database.json', 'w+') as f:
        json.dump(request.json, f, indent=2)
    return jsonify({'status': 200})

def save_target_database():
    if not request.json:
        abort(400)
    with open('./target_database.json', 'w+') as f:
        json.dump(request.json, f, indent=2)
    return jsonify({'status': 200})


# engine api
def get_engine_config():
    data = json.load(open('./engine_config.json', 'r'))
    return jsonify(data)

def save_engine_config():
    if not request.json:
        abort(400)
    with open('./engine_config.json', 'w+') as f:
        json.dump(request.json, f, indent=2)
    return jsonify({'status': 200})

def get_engine_log_latest():
    with open('./engine_log.txt', 'r+') as f:
        data = f.read()
        f.truncate(0)
    return jsonify(data)

def clear_engine_log():
    with open('./engine_log.txt', 'r+') as f:
        f.truncate(0)
    return jsonify({'status': 200})

def run_engine():
    run_dv_engine()
    return jsonify({'status': 200})


# currently unused
def get_schema():
    if not request.json:
        abort(400)

    if request.json['user'] == '':
        request.json['user'] = None

    if request.json['pwd'] == '':
        request.json['pwd'] = None

    if request.json['port'] == '':
        request.json['port'] = None

    tables, relations = get_schema(server=request.json['server'],
                                   database=request.json['database'],
                                   db_type=request.json['db_type'],
                                   user=request.json['user'],
                                   pwd=request.json['pwd'],
                                   port=request.json['port'])
    
    return json.dumps({'tables': tables, 
                       'relations': relations})