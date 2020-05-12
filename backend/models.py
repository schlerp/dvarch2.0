from backend import db


class Database(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    server = db.Column(db.String(128), index=True)
    database = db.Column(db.String(128), index=True, unique=True)
    db_type = db.Column(db.String(32))
    db_role = db.Column(db.String(32))
    port = db.Column(db.Integer())
    user = db.Column(db.String(128))
    password = db.Column(db.String(128))

    def __repr__(self):
        return '<Database {}\\{}>'.format(self.server, self.database)

class ConfigFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True)
    path = db.Column(db.String(256))

    def __repr__(self):
        return '<ConfigFile {} ({})>'.format(self.name, self.path)
