import iso8601
import StringIO
import sqlite3
import random
import redis
import json
import time
import datetime
import re
from config import Config
from instrumentation import *
import os.path
import _mysql

class PointdataUpdate:
    def __init__(self, server_hostname):
        self._db = None
        self.server_hostname = server_hostname
        self.config = Config()
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))

    @property
    def db(self):
        if self._db:
            return self._db
        self._db = _mysql.connect(self.config.get("mysql-hostname"), self.config.get("mysql-username"), self.config.get("mysql-password"), self.config.get("mysql-database"))
        return self._db

    def escape(self, string):
        if string is None:
            return "null"
        return "'"+_mysql.escape_string(str(string))+"'"

    @timing("pointdata.update.add")
    def add(self, timestamp, system, username, data=None, remote_ip=None, server_ip=None, is_utc=False, tzinfo=None):
        r_k = "pointdata-%s-%s-%s-%s-%s-%s" % (timestamp, system, username, data, remote_ip, server_ip)
        if self.redis.exists(r_k):
            statsd.incr("pointdata.update.add.already_exists")
            return
        statsd.incr("pointdata.update.add.process")

        if is_utc:
            is_utc=1
        else:
            is_utc=0

        now = datetime.datetime.now()

        username = username.lower()
        if "@" in username:
            if self.redis.exists("email-to-username-%s" % username):
                tmp = self.redis.get("email-to-username-%s" % username)
                if tmp is not None and len(tmp) > 3:
                    username = tmp
        query = "INSERT INTO pointdata VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (self.escape(timestamp), self.escape(now), self.escape(system), self.escape(username), self.escape(remote_ip), self.escape(server_ip), self.escape(data), self.escape(tzinfo), is_utc)
        self.db.query(query)
        r = self.db.store_result()
        self.redis.set(r_k, True)

        if remote_ip is not None:
            self.redis.rpush("ip-resolve-queue", remote_ip.split(":")[0])
        if server_ip is not None:
            self.redis.rpush("ip-resolve-queue", server_ip.split(":")[0])

def is_valid_hostname(hostname):
    if len(hostname) > 255:
        return False
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

@timing("pointdata.update.main")
def application(environ, start_response):
    statsd.incr("pointdata.update.main.counter")
    start_response("200 OK", [("Content-Type", "text/plain")])
    query_string = environ["QUERY_STRING"]
    query_string = query_string.split("&")
    hostname = False
    for item in query_string:
        item = item.split("=")
        if len(item) == 2:
            if item[0] == "server":
                if is_valid_hostname(item[1]):
                    hostname = item[1]
    if not hostname:
        return ["Invalid hostname"]

    pointdata_update = PointdataUpdate(hostname)
    try:
        data = json.load(environ["wsgi.input"])
    except:
        return ["Invalid input"]
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                pointdata_update.add(item.get("timestamp"), item.get("system"), item.get("username"), item.get("data"), item.get("remote_ip"), item.get("server_ip"), item.get("is_utc"), item.get("tzinfo"))
    elif isinstance(data, dict):
        pointdata_update.add(data.get("timestamp"), data.get("system"), data.get("username"), data.get("data"), data.get("remote_ip"), data.get("server_ip"), item.get("is_utc"), item.get("tzinfo"))
    else:
        return ["Invalid data type"]    
    return ["OK"]

