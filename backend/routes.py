from flask import render_template
from flask import request
from flask_socketio import emit

from backend import app, socketio
from backend import api


@socketio.on('my event', namespace='/socket')
def test_message(message):
    emit('my response', {'data': message['data']})

@socketio.on('my broadcast event', namespace='/socket')
def test_message(message):
    emit('my response', {'data': message['data']}, broadcast=True)

@socketio.on('connect', namespace='/socket')
def test_connect():
    emit('my response', {'data': 'Connected'})

@socketio.on('disconnect', namespace='/socket')
def test_disconnect():
    print('Client disconnected')


# add REST API urls
app.route("/api/random", methods=['GET'])(api.random_number)

app.route("/api/get_mapping", methods=['GET'])(api.get_mapping)
app.route("/api/save_mapping", methods=['POST'])(api.save_mapping)

app.route("/api/get_source_database", methods=['GET'])(api.get_source_database)
app.route("/api/get_target_database", methods=['GET'])(api.get_target_database)
app.route("/api/save_source_database", methods=['POST'])(api.save_source_database)
app.route("/api/save_target_database", methods=['POST'])(api.save_target_database)

app.route("/api/get_engine_config", methods=['GET'])(api.get_engine_config)
app.route("/api/save_engine_config", methods=['POST'])(api.save_engine_config)
app.route("/api/run_engine", methods=['POST'])(api.run_engine)
app.route("/api/get_engine_log_latest", methods=['GET'])(api.get_engine_log_latest)
app.route("/api/clear_engine_log", methods=['POST'])(api.clear_engine_log)
