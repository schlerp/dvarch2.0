from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from flask_socketio import SocketIO
from random import *
from queue import Queue


app = Flask(__name__,
            static_folder = "../dist/static",
            template_folder = "../dist")

CORS(app, 
    resources={
        #r"/api/*": {"origins": "*"},
        r"*": {"origins": "*"},
    }
)


# config
import os
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SECRET_KEY'] = 'supersecret!'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'app.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


# for flask-sqlalchemy
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# for flask-socketio
socketio = SocketIO(app, cors_allowed_origins="*")

# improt app now everything is set up!
import backend.models
import backend.routes
import backend.api
import backend.physical
import backend.database_schema
