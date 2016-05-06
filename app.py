# coding=utf-8
from gevent import monkey

monkey.patch_all()

import time
import os
import psycopg2
import psycopg2.extras

from threading import Thread
from flask import Flask, render_template, session, request, jsonify
from flask.ext.socketio import SocketIO, emit, join_room, leave_room, \
    close_room, disconnect

from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.admin import Admin
from flask.ext.admin.contrib.sqla import ModelView
import sys
import re
from logging.handlers import SMTPHandler
import logging
import json

from time import strftime, localtime
from datetime import timedelta, date, timedelta
import calendar

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'media')
app = Flask(__name__, static_folder=tmpl_dir)
app.debug = False
app.config['SECRET_KEY'] = '123456790'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://Administrator:mac8.6@10.206.131.1/PromotionDB_New'

db = SQLAlchemy(app)
admin = Admin(app)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)
thread = None

ADMINS = ['xianyu_wang@trendmicro.com.cn']


class basicinfo_productsconfig(db.Model):
    can_create = False
    id = db.Column(db.Integer, primary_key=True)
    appid = db.Column(db.BigInteger)
    companyid = db.Column(db.BigInteger)
    account = db.Column(db.VARCHAR)
    password = db.Column(db.VARCHAR)
    appname = db.Column(db.VARCHAR)
    latest_version = db.Column(db.VARCHAR)
    daappname = db.Column(db.VARCHAR)


class basicinfo_regions(db.Model):
    can_create = False
    id = db.Column(db.Integer, primary_key=True)
    RegionName_text = db.Column(db.VARCHAR)


class ConfigAdmin(ModelView):
    column_filters = ('appname',)

admin.add_view(ConfigAdmin(basicinfo_productsconfig, db.session))

from datetime import datetime
import json
import redis

redis_queue = redis.StrictRedis(host='10.206.131.29', port=6379, db=0)


def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        try:
            count += 1
            event_items = redis_queue.smembers('get_memory_clean_geo')
            totoal_memory_clean_time = redis_queue.get('tatal_memory_clean_time')
            redis_queue.delete('get_memory_clean_geo')
            if event_items is not None:
                time.sleep(5)
                event_number = len(event_items)
                result_response_json = {}
                data = []
                for event_item in event_items:
                    item_json = {}
                    event_item_json = json.loads(event_item)
                    try:
                        item_json['abbrev'] = event_item_json['country_code']
                        item_json['parentState'] = event_item_json['country_code']
                        item_json['capital'] = event_item_json['country_code']
                        item_json['lat'] = event_item_json['latitude']
                        item_json['lon'] = event_item_json['longitude']
                        item_json['value'] = 150
                    except:
                        print "error:", sys.exc_info()[0]
                    data.append(item_json)
                    # print event_item, type(event_item)

                result_response_json['data'] = data
                result_response_json['totalUser'] = event_number
                result_response_json['totoal_memory_clean_time'] = totoal_memory_clean_time
                result_response_json['currentTime'] = str(datetime.now())
                print result_response_json
                socketio.emit('my response', result_response_json, namespace='/test')
        except:
            print "error:", sys.exc_info()[0]
            # socketio.emit('my response',
            #               {'data': 'Server generated event', 'count': event_number, 'result_response_json':result_response_json},
            #               namespace='/test')


@app.route('/')
def index():
    global thread
    if thread is None:
        thread = Thread(target=background_thread)
        thread.start()
    return render_template('index.html')


@socketio.on('my event', namespace='/test')
def test_message(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': message['data'], 'count': session['receive_count']})


@socketio.on('my broadcast event', namespace='/test')
def test_broadcast_message(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': message['data'], 'count': session['receive_count']},
         broadcast=True)


@socketio.on('join', namespace='/test')
def join(message):
    join_room(message['room'])
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': 'In rooms: ' + ', '.join(request.namespace.rooms),
          'count': session['receive_count']})


@socketio.on('leave', namespace='/test')
def leave(message):
    leave_room(message['room'])
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': 'In rooms: ' + ', '.join(request.namespace.rooms),
          'count': session['receive_count']})


@socketio.on('close room', namespace='/test')
def close(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response', {'data': 'Room ' + message['room'] + ' is closing.',
                         'count': session['receive_count']},
         room=message['room'])
    close_room(message['room'])


@socketio.on('my room event', namespace='/test')
def send_room_message(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': message['data'], 'count': session['receive_count']},
         room=message['room'])


@socketio.on('disconnect request', namespace='/test')
def disconnect_request():
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my response',
         {'data': 'Disconnected!', 'count': session['receive_count']})
    disconnect()


@socketio.on('connect', namespace='/test')
def test_connect():
    emit('Connected', {'data': 'Connected'})
    totoal_memory_clean_time = redis_queue.get('tatal_memory_clean_time')
    result_response_json = {}
    data = []
    item_json = {}
    item_json['abbrev'] = 'AL'
    item_json['parentState'] = 'Alabama'
    item_json['capital'] = 'Alabama'
    item_json['lat'] = 32.380120
    item_json['lon'] = -86.300629
    item_json['value'] = 150
    data.append(item_json)
    result_response_json['data'] = data
    result_response_json['totalUser'] = 1
    result_response_json['totoal_memory_clean_time'] = totoal_memory_clean_time
    result_response_json['currentTime'] = str(datetime.now())
    # init_response_json = {"data": [{"abbrev":"AL","parentState":"Alabama","capital":"Montgomery","lat":32.380120,"lon":-86.300629,"value":150}],"totalUser": 1,"currentTime": "2015-07-14T20:14:20.401518"}
    emit('my response', result_response_json, namespace='/test')


@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected')


# @app.route('/get_retention_rate')
# def get_retention_rate():
#     conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
#     cur=conn.cursor()
#     geo_list = ["0", "us", "it", "cn", "jp", "gb", "de", "fr"]
#     country_list = {}
#     geo_str = str(geo_list)
#     geo_str = geo_str.replace("[", "(")
#     geo_str = geo_str.replace("]", ")")
#     result = {}
#     mgeo_data = {}
#     today = date.today()
#     result['currentDate'] = today.strftime("%Y-%m-%d")
#     geo_data = {}
#     max = 0
#     datelist = {}
#     appname = request.args.get('appname', '%')
#     strsql1 = "select distinct statdate from ga_retention_rate_geo_new WHERE appname like '%s' ORDER BY statdate ASC" % (appname)
#     strsql2 = "select statdate, retentionday, rate , inserttime, geo from ga_retention_rate_geo_new where geo IN %s AND retentionday <= 30 and appname like '%s'" %(geo_str.upper(), appname)
#     strsql3 = "select country_short_name, country_name from ga_country where country_short_name IN %s" %(geo_str)

#     try:
#         r=cur.execute(strsql3)
#         for (CountryShortName_string, CountryName_text) in cur:
#             country_list[CountryShortName_string] = CountryName_text
#         country_list["0"] = "All country"
#     except psycopg2.DatabaseError, e:
#         print 'got databaseerror: {}'.format(e.message)


#     try:
#         r=cur.execute(strsql2)
#         for datestr in datelist:
#             geo_data[datestr[0].strftime("%Y-%m-%d")] = {}
#         for (statdate, retentionday, rate, inserttime, geo) in cur:
#             print statdate, retentionday, rate, geo
#     except psycopg2.DatabaseError, e:
#         print 'got databaseerror: {}'.format(e.message)


#     try:
#         r=cur.execute(strsql1)
#         datelist = cur.fetchall()

#         if len(datelist) > 30:
#             max = 30
#         else:
#             max = len(datelist)
#     except psycopg2.DatabaseError, e:
#         print 'got databaseerror: {}'.format(e.message)

#     try:
#         r=cur.execute(strsql2)
#         for geo in geo_list:
#             geo_data[geo] = {}
#             for datestr in datelist:
#                 geo_data[geo][datestr[0].strftime("%Y-%m-%d")] = {}

#         for (statdate, retentionday, rate, inserttime, geo) in cur:
#             print statdate.strftime("%Y-%m-%d")
#             if not(geo_data[geo.lower().strip()][statdate.strftime("%Y-%m-%d")].has_key("statdate")):
#                 geo_data[geo.lower().strip()][statdate.strftime("%Y-%m-%d")]["0"] = statdate.strftime("%Y-%m-%d")
#             mrate = round(rate, 4) * 100
#             geo_data[geo.lower().strip()][statdate.strftime("%Y-%m-%d")][str(retentionday)] = str(mrate) + '%'
#             print statdate, retentionday, rate, geo
#     except psycopg2.DatabaseError, e:
#         print 'got databaseerror: {}'.format(e.message)

#     # for data in geo_data:
#     #     mgeo_data.
#     #         (geo_data[data])

#     result['dayinterval'] = max;
#     result['countrylist'] = country_list;
#     result['data'] = geo_data;
#     return json.dumps(result)


@app.route('/get_retention_rate')
def get_retention_rate():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    country_name = request.args.get('country-name', '0')
    app_name = request.args.get('app-name', 'DrCleaner')
    start_date = request.args.get('start-time', (date.today() - timedelta(days=11)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end-time', (date.today()).strftime('%Y-%m-%d'))
    days_num = request.args.get('days-num', '10')
    # geo_list = ["0", "us", "it", "cn", "jp", "gb", "de", "fr"]
    cur.execute("SELECT statdate, rate, statdate, retentionday FROM ga_retention_rate_geo_new_fixed\
                 WHERE statdate>='%s' AND statdate<='%s' AND retentionday<='%s'\
                        AND geo='%s' AND appname='%s'\
                 ORDER BY statdate" % (start_date, end_date, days_num, country_name, app_name))

    series = []
    date_list = []
    for item in cur.fetchall():
        print item
        if item[0] not in date_list:
            series.append({"name": item[0].strftime('%Y-%m-%d-%a'), "data": []})
            date_list.append(item[0])
        series[-1]['data'].append(round(item[1] * 100, 2))
    return jsonify(code=200, series=series)


@app.route('/get_version_list')
def get_version_list():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    version_list = []
    result = {}
    strsql = "select distinct version from ga_activeuserhr_version Order By version Asc"

    try:
        r = cur.execute(strsql)
        for version in cur:
            if (str(version).count('.') < 3):
                version_list.append(version)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    result['versionList'] = version_list
    return json.dumps(result)


@app.route('/get_country_list')
def get_country_list():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    country_list = []
    result = {}
    strsql = "select geo from ga_download_number_new group by geo order by sum(num) desc limit 20"

    try:
        r = cur.execute(strsql)
        for geo in cur:
            if (str(geo).find("try") == -1):
                country_list.append(geo)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    result['countryList'] = country_list
    return json.dumps(result)


@app.route('/get_app_list')
def get_app_list():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    app_list = []
    result = {}
    strsql = "select appname from ga_download_number_new group by appname order by sum(num) desc limit 20"

    try:
        r = cur.execute(strsql)
        for geo in cur:
            if (str(geo).find("try") == -1):
                app_list.append(geo)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    result['applist'] = app_list
    return json.dumps(result)


@app.route('/get_rating_distribution')
def get_rating_distribution():
    conn = psycopg2.connect("dbname='testapp' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    time_interval = request.args.get('time_interval', 'Hour');
    begindate = datetime.strptime(request.args.get('begindate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 00:00:00");
    enddate = datetime.strptime(request.args.get('enddate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 23:00:00");
    begindate_date = request.args.get('begindate')
    enddate_date = request.args.get('enddate')
    star = request.args.get('star');
    appname = request.args.get('appname', '%')
    product = {"DrCleaner": 921458519, "DuplicateFilesCleaner": 1031826818, "DrCleanerPro": 1037994372, '%': '%'}
    result = {}
    dataArr = []
    sql_str = {}

    sql = "select {}, inserttime from rating_monitor where app_name='{}' and inserttime >= '{}' and inserttime <= '{}'".format(star, appname, begindate, enddate)

    try:
        r = cur.execute(sql)

        re = cur.fetchall()

        for count, inserttime in re:
            dataObj = {}
            dataObj["stattime"] = str(inserttime)
            dataObj["activeuser"] = count
            dataObj["star"] = star
            dataArr.append(dataObj)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    total_count = len(dataArr)
    result['meta'] = dataArr
    result['timeInterval'] = time_interval
    result['totalCount'] = total_count
    return jsonify(result)


@app.route('/get_active_user')
def get_active_user():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    time_interval = request.args.get('time_interval', 'Hour');
    begindate = datetime.strptime(request.args.get('begindate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 00:00:00");
    enddate = datetime.strptime(request.args.get('enddate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 23:00:00");
    begindate_date = request.args.get('begindate')
    enddate_date = request.args.get('enddate')
    version = request.args.get('version');
    appname = request.args.get('appname', '%')
    product = {"DrCleaner": 921458519, "DuplicateFilesCleaner": 1031826818, "DrCleanerPro": 1037994372, '%': '%'}
    result = {}
    dataArr = []
    sql_str = {}
    if version == 'Total Active User': version = '%'
    if version == 'Total User Number':
        sql_str['Hour'] = ""
        sql_str[
            'Day'] = "select \"StaticTime\", sum(\"Units\") from basicinfo_accumulativesalesinfo where country_id = '999' and \"StaticTime\" between '%s' and '%s' and \"appid\" like '%s' group by \"StaticTime\"" % (
            begindate_date, enddate_date, product[appname])
        sql_str[
            'Week'] = "select \"StaticTime\", sum(\"Units\") from basicinfo_accumulativesalesinfo where country_id = '999' and \"StaticTime\" between '%s' and '%s' and \"appid\" like '%s' and EXTRACT(DOW FROM \"StaticTime\") = 1 group by \"StaticTime\" order by \"StaticTime\"" % (
            begindate_date, enddate_date, product[appname])
        sql_str[
            'Month'] = "select \"StaticTime\", sum(\"Units\") from basicinfo_accumulativesalesinfo where country_id = '999' and \"StaticTime\" between '%s' and '%s' and \"appid\" like '%s' and EXTRACT(DAY FROM \"StaticTime\") = 1 group by \"StaticTime\" order by \"StaticTime\"" % (
            begindate_date, enddate_date, product[appname])
    else:
        sql_str[
            'Hour'] = "select stattime, sum(activeuser) from ga_activeuserhr_version WHERE stattime >= '%s' and stattime <= '%s' and version like '%s' and appname like '%s' group by stattime ORDER BY stattime ASC" % (
            begindate, enddate, version, appname)
        sql_str[
            'Day'] = "select statdate, sum(activeuser) from ga_activeuserhr_version  where stattime >= '%s' and stattime <= '%s'and version like '%s' and appname like '%s' group by statdate order by statdate" % (
            begindate, enddate, version, appname)
        sql_str[
            'Week'] = "select statdate, sum(activeuser) from ga_activeuserweek_version where statdate >= '%s' and statdate <= '%s' and version like '%s' and appname like '%s' group by statdate order by statdate" % (
            begindate, enddate, version, appname)
        sql_str[
            'Month'] = "select statdate, sum(activeuser) from ga_activeusermonth_version where statdate >= '%s' and statdate <= '%s' and version like '%s' and appname like '%s' group by statdate order by statdate" % (
            begindate, enddate, version, appname)
    try:
        r = cur.execute(sql_str[time_interval])
        # print sql_str[time_interval]

        for stattime, activeuser in cur.fetchall():
            dataObj = {}
            dataObj["stattime"] = str(stattime)
            dataObj["activeuser"] = activeuser
            dataObj["version"] = version
            dataArr.append(dataObj)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    total_count = len(dataArr)
    result['meta'] = dataArr
    result['timeInterval'] = time_interval
    result['totalCount'] = total_count
    return jsonify(result)


@app.route('/get_download_number')
def get_download_number():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    begindate = datetime.strptime(request.args.get('begindate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 00:00:00");
    enddate = datetime.strptime(request.args.get('enddate', datetime.today()), "%Y-%m-%d").strftime(
        "%Y-%m-%d 23:00:00");
    countrys = request.args.get('countryArr', 'US');
    appname = request.args.get('appname', '%')
    countryList = countrys.split(',')
    countryListStr = str(countryList).replace('[', '(').replace(']', ')').replace('u', '')
    result = {}
    dataArr = []

    # data of target country
    for geo in countryList:
        strsql = "select MAX(statdate) , SUM(num) from ga_download_number_fixed where geo = '%s' and statdate >= '%s' and statdate <= '%s' and appname like '%s' group by statdate order by statdate desc" % (
            geo, begindate, enddate, appname)

        try:
            r = cur.execute(strsql)

            for max, sum in cur:
                dataObj = {}
                dataObj["statdate"] = str(max)
                dataObj["number"] = int(sum)
                dataObj["country"] = get_country_name(geo)[0]
                dataObj["geo"] = geo
                dataArr.append(dataObj)

        except psycopg2.DatabaseError, e:
            print 'got databaseerror: {}'.format(e.message)

    # data of other country
    strsql = "select MAX(statdate) , SUM(num) from ga_download_number_fixed where geo not in %s and statdate >= '%s' and statdate <= '%s' and appname like '%s' group by statdate order by statdate desc" % (
        countryListStr, begindate, enddate, appname)
    try:
        r = cur.execute(strsql)

        for max, sum in cur:
            dataObj = {}
            dataObj["statdate"] = str(max)
            dataObj["number"] = int(sum)
            dataObj["country"] = "Other"
            dataObj["geo"] = "Other"
            dataArr.append(dataObj)

    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    total_count = len(dataArr)
    result['meta'] = dataArr
    result['totalCount'] = total_count
    return jsonify(result)


def get_country_name(countrycode):
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    strsql = "select distinct country_name from ga_country where country_short_name = lower('%s')" % (countrycode)
    country_name = countrycode
    try:
        r = cur.execute(strsql)

        for country_name in cur:
            country_name = country_name
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    return country_name


@app.route('/get_newuser')
def get_ip_country_name():
    redis_con = redis.StrictRedis(host='10.206.131.29', port=6379, db=0)
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()

    result = {}
    result['currentTime'] = datetime.now().isoformat()
    totaluser = 0
    geo_data = []
    # while True:
    # date = datetime.strptime("8/07/15 00:00", "%d/%m/%y %H:%M")
    # timestamp=timezone('Asia/Shanghai').localize(datetime.datetime.now() - datetime.timedelta(minutes=150))
    timestamp = datetime.now() - timedelta(seconds=60)

    query_date_stamp = timestamp
    i = 0
    # "INSERT INTO  ga_activeuser (stattime,activeuser)  VALUES (\'%s\', %s) " %(timestamp,activeuser
    strsql = "select stattime, geo, activeuser from ga_geo_newuser where stattime > (\'%s\') ORDER BY stattime DESC" % (
        timestamp)
    try:
        r = cur.execute(strsql)
        for (stattime, geo, activeuser) in cur:
            if i == 0:
                query_date_stamp = stattime
                i = i + 1
            if query_date_stamp != stattime:
                break
            # print("{}, {}, {}".format(stattime, geo, activeuser))
            totaluser += activeuser
            geo_dic = {}
            geo_dic['code'] = geo.lower()
            geo_dic['value'] = activeuser
            print geo, stattime, timestamp
            is_exist = redis_con.hexists('country_codes', geo)
            if (is_exist):
                country_name = redis_con.hget('country_codes', geo)
                geo_dic['name'] = country_name
            else:
                geo_dic['name'] = 'None'
            geo_data.append(geo_dic)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    result['data'] = geo_data
    result['totalUser'] = totaluser
    print result
    # conn.discard()
    request.args.get('callback')
    return request.args.get('callback') + '(' + json.dumps(result) + ')'


@app.route('/find_user', methods=['GET', 'POST'])
def find_user():
    conn = psycopg2.connect("dbname='testapp' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    uuid = request.args.get('uuid')
    user_mac = None
    user_itis = None
    try:
        cur.execute("SELECT * FROM myusermaclatest WHERE \"user_uuid\" = '%s'" % (uuid))
        user_mac = cur.fetchone()

        cur.execute("SELECT * FROM myuseritislatest WHERE \"user_uuid\"='%s'" % (uuid))
        user_itis = cur.fetchall()
        user_mac = list(user_mac)
        user_mac[4] = user_mac[4].decode('utf-8')
    except Exception, e:
        return "Not Found User, Sorry"

    print user_mac, user_itis
    return render_template('user_info.html',
                           user_mac=user_mac,
                           user_itis=user_itis)


@app.route('/get_exposure_rate')
def get_exposure_rate():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    exposure_table = None
    country_name = None
    yesterday = datetime.today().date() - timedelta(days=2)

    # currently use hard code : country_id = '1' when excuting sql query
    country_id = request.args.get('geoCode', '1')
    sql0 = 'select "CountryName_text" from basicinfo_country where id = \'1\''
    sql2 = 'select MAX(date(\"UpdateTime\")) from basicinfo_competitorkeywordsearchrank where country_id = \'1\''

    try:
        cur.execute(sql2)
        yesterday = cur.fetchone()[0]
    except Exception, e:
        print e

    sql1 = 'select a.keyword, a.max, a.total_count, b.exposure_count, round(b.exposure_count/cast(a.total_count as FLOAT)::numeric, 2) as exposure_rate ' \
           'from (select keyword, max(\"UpdateTime\"), count(product_id) as \"total_count\" from basicinfo_competitorkeywordsearchrank where \"UpdateTime\" >= \'{}\' and "country_id"=\'1\' ' \
           'GROUP BY keyword order by keyword)a,(select keyword, max(\"UpdateTime\") as \"UpdateTime\",count(product_id) as exposure_count from basicinfo_competitorkeywordsearchrank where \"UpdateTime\" >= \'{}\' and "country_id"=\'1\' and product_id ' \
           'in (select basicinfo_product.id as \"product_id\" from basicinfo_product join basicinfo_couterparts on  basicinfo_product.pdappid_bigint=cast(basicinfo_couterparts.appid as BIGINT)) GROUP BY keyword order by keyword)b where a.keyword=b.keyword ORDER BY exposure_rate desc'.format(
        yesterday, yesterday)

    try:
        cur.execute(sql0)
        country_name = cur.fetchone()[0]
    except Exception, e:
        print e

    try:
        cur.execute(sql1)
        exposure_table = cur.fetchall()
        exposure_table = list(exposure_table)
    except Exception, e:
        print e

    return render_template('exposure_rate.html',
                           exposure_table=exposure_table,
                           country_name=country_name)


@app.route('/get_app_keyword_info')
def get_app1_keyword_info():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()

    # currently use hard code : country_id = '1' when excuting sql query
    country_id = request.args.get('geoCode', '1')
    app_id1 = request.args.get('appid1', '0')
    app_id2 = request.args.get('appid2', '0')
    product_id1 = None
    product_id2 = None
    update_date1 = None
    update_date2 = None
    app_name1 = None
    keyword_table1 = None

    """
    grab pre-params from db
    app_name1:appid1's appname
    product_id1: app1's product_id
    product_id2: app2's product_id
    update_date1: the date when app1's latest keyword infos were updated into db
    update_date2: the  date when app2's latest keyword infos were updated into db
    """
    app_name1 = get_app_name(app_id1)

    product_id1 = get_product_id(app_id1)
    product_id2 = get_product_id(app_id2)

    if (product_id1 is not None) and (product_id2 is not None):
        update_date1 = get_latest_date(product_id1)
        update_date2 = get_latest_date(product_id2)

    if (app_name1 is None) or (product_id1 is None) or (product_id2 is None) or (update_date1 is None) or (
                update_date2 is None):
        return render_template('app_keyword_info.html',
                               app_name1=None,
                               keyword_table1=None)
    else:
        # unique keyword info of appid
        sql = 'select keyword, \"RankNm\" from basicinfo_competitorkeywordsearchrank where country_id=\'1\' and \"UpdateTime\">=\'{}\' and product_id = \'{}\' and ' \
              'keyword not in ((select keyword from basicinfo_competitorkeywordsearchrank where country_id=\'1\' and \"UpdateTime\">=\'{}\' and product_id = \'{}\')) ORDER BY \"RankNm\"'.format(
            update_date1, product_id1, update_date2, product_id2)

        try:
            cur.execute(sql)
            keyword_table1 = cur.fetchall()
            keyword_table1 = list(keyword_table1)
        except Exception, e:
            print e

        if keyword_table1 is None:
            return render_template('app_keyword_info.html',
                                   app_name1=None,
                                   keyword_table1=None)
        else:
            return render_template('app_keyword_info.html',
                                   app_name1=app_name1,
                                   keyword_table1=keyword_table1)


@app.route('/get_common_keyword_info')
def get_common_keyword_info():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    # currently use hard code : country_id = '1' when excuting sql query
    country_id = request.args.get('geoCode', '1')
    app_id1 = request.args.get('appid1', '')
    app_id2 = request.args.get('appid2', '')
    app_name1 = None
    app_name2 = None
    product_id1 = None
    product_id2 = None

    """
    grab pre-params from db
    app_name1: app1's name
    app_name2: app2's name
    product_id1: app1's product_id
    product_id2: app2's product_id
    update_date1: the date when app1's latest keyword infos were updated into db
    update_date2: the  date when app2's latest keyword infos were updated into db
    """

    app_name1 = get_app_name(app_id1)
    app_name2 = get_app_name(app_id2)

    product_id1 = get_product_id(app_id1)
    product_id2 = get_product_id(app_id2)

    if (product_id1 is not None) and (product_id2 is not None):
        update_date1 = get_latest_date(product_id1)
        update_date2 = get_latest_date(product_id2)

    if (app_name1 is None) or (app_name2 is None) or (product_id1 is None) or (product_id2 is None) or (
                update_date1 is None) or (update_date2 is None):
        return render_template('common_keyword_info.html',
                               app_name1=None,
                               app_name2=None,
                               keyword_table1=None)

    else:
        sql = 'SELECT A.keyword , A.\"RankNm\" as \"RankNmA\", B.\"RankNm\" as \"RankNmB\" from (select keyword, \"RankNm\", product_id from basicinfo_competitorkeywordsearchrank where country_id=\'1\' and "UpdateTime">=\'{}\' and product_id = \'{}\') A ' \
              'Join (select keyword, \"RankNm\", product_id from basicinfo_competitorkeywordsearchrank where country_id=\'1\' and \"UpdateTime\">=\'{}\' and product_id = \'{}\')B on B.keyword = A.keyword'.format(
            update_date1, product_id1, update_date2, product_id2)

        try:
            cur.execute(sql)
            keyword_table1 = cur.fetchall()
            keyword_table1 = list(keyword_table1)
        except Exception, e:
            print e

        if keyword_table1 is None:
            return render_template('common_keyword_info.html',
                                   app_name1=None,
                                   app_name2=None,
                                   keyword_table1=None)

        else:
            return render_template('common_keyword_info.html',
                                   app_name1=app_name1,
                                   app_name2=app_name2,
                                   keyword_table1=keyword_table1)


@app.route('/get_counter_app_list')
def get_counter_app_list():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()

    dbname = request.args.get('type', 'dc')
    app_list_dc = ['Dr. Cleaner: Clean Disk and Memory, Uninstall Apps and Optimize System',
                'Duplicate File Cleaner - Find and Delete Your Duplicates',
                'Disk Doctor - Clean Your Drive and Free Up Space', 'Gemini: The Duplicate Finder',
                'Disk Diet - Clean your drive', 'The Duplicate Finder',
                'Disk Map - Visualize Hard Drive Usage and Free Up Space', 'Duplicate File Cleaner',
                'Duplicate File Detector',
                'Disk Aid - Drive Cleaning, System Optimization & Protection Tool',
                'Duplicate Detective - Find and Delete Duplicate Files',
                'Memory Clean - Monitor and Free Up Memory', 'Memory Monitor - Speed up your system with a simple click'
        , 'MemoryFreer']

    app_list_ds = ['Dr. Safety: Virus & Malware Detection, System Security Protection',
                'AntiVirus Sentinel Pro - Adware & Virus Scanner - Network Monitor & Protection'
        , 'PrivacyScan', '1Password - Password Manager and Secure Wallet', 'Bitdefender Virus Scanner',
                'Dashlane - Password Manager App & Secure Digital Wallet.', 'eWallet'
        , 'Hotspot Shield VPN -Best VPN for WiFi Security, Privacy, Unblock Sites',
                'Keeper® Password Manager & Digital Vault - Secure and encrypted data storage for your passwords, files, photos and notes.'
        , 'VPN Unlimited - Encrypted, Secure & Private Internet Connection for Anonymous Web Surfing',
                'DataVault Password Manager'
        , 'GoVPN - Free VPN for WiFi Security, Unblock Sites', 'Hack RUN', 'Cookie',
                'AtHome Video Streamer - Video surveillance for home security'
        , 'mSecure', 'SecurePDF : Add, Remove and Modify PDF Password Security in batch', 'Security Camera',
                'Max Total Security with Anti-Virus'
        , 'Encrypto: Encrypt the Files You Send', 'VPN Asia - Speed and Security',
                'SaferVPN - Fast & Easy VPN for Security and Privacy Online'
        , 'PDF Security', 'Virus Scanner Plus', 'BitMedic AntiVirus - Malware & Adware Security', 'Screensaver + free',
                'AntiVirus Security Scanner - Privacy Protection, Network Safety, Virus Detection'
        , 'Keys - Password Manager, Organizer, Vault for Ultimate Safe Secure Personal Secret Credential',
                'SAASPASS | Single Sign On Proximity Login with Two-Factor Authentication Mobile Security & Authenticator Two-Step Verification Software Token']


    result = {}
    # strsql = 'select DISTINCT pdname_string as appname from basicinfo_product where pdappid_bigint in (select appid from basicinfo_couterparts) order by appname'
    #
    # try:
    #     cur.execute(strsql)
    #     for appname in cur:
    #         app_list.append(appname)
    # except psycopg2.DatabaseError, e:
    #     print 'got databaseerror: {}'.format(e.message)

    if dbname == "ds":
        result['app_list'] = app_list_ds
    else:
        result['app_list'] = app_list_dc
    return json.dumps(result)
    pass


@app.route('/get_app_keyword_info_ui')
def get_app1_keyword_info_ui():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    app_name1 = request.args.get('app_name1', '').replace("\"", "")
    app_name2 = request.args.get('app_name2', '').replace("\"", "")
    db_type = request.args.get('type', 'dc')
    latest_date = None

    # app_name1 = app_name1[1: len(app_name1) - 1]
    # app_name2 = app_name2[1: len(app_name2) - 1]

    if db_type == 'ds':
        db_name = 'basicinfo_allkeywordsranks_4_drsafety'
    else:
        db_name = 'basicinfo_allkeywordsranks_4_drcleaner'

    strsql1 = 'select max(update_date) from {}'.format(db_name)

    try:
        cur.execute(strsql1)
        latest_date = cur.fetchone()[0]
    except Exception, e:
        print e

    if latest_date is None:
        latest_date = datetime.now().date()

    strsql2 = 'select DISTINCT keyword, rank from {} where appname like \'{}\' and update_date = \'{}\' and ' \
              'keyword not in (select DISTINCT keyword from {} where appname like \'{}\')'.format(
        db_name, app_name1, latest_date, db_name, app_name2)

    try:
        cur.execute(strsql2)
        keyword_table1 = cur.fetchall()
        keyword_table1 = list(keyword_table1)
    except Exception, e:
        print e

    if keyword_table1 is None:
        return render_template('app_keyword_info.html',
                               app_name1=None,
                               keyword_table1=None)
    else:
        return render_template('app_keyword_info.html',
                               app_name1=app_name1,
                               keyword_table1=keyword_table1)



@app.route('/get_app_names')
def get_app_names():
    conn = psycopg2.connect("dbname='testapp' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    app_list = []
    result = {}
    strsql = "select DISTINCT app_name from rating_monitor"

    try:
        r = cur.execute(strsql)
        for geo in cur:
            if (str(geo).find("try") == -1):
                app_list.append(geo)
    except psycopg2.DatabaseError, e:
        print 'got databaseerror: {}'.format(e.message)

    result['applist'] = app_list
    return json.dumps(result)


@app.route('/get_common_keyword_info_ui')
def get_common_keyword_info_ui():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    app_name1 = request.args.get('app_name1', '').replace("\"", "")
    app_name2 = request.args.get('app_name2', '').replace("\"", "")
    db_type = request.args.get('type', 'dc')
    latest_date = None
    db_name = None
    # app_name1 = app_name1[1: len(app_name1) - 1]
    # app_name2 = app_name2[1: len(app_name2) - 1]

    if db_type == 'ds':
        db_name = 'basicinfo_allkeywordsranks_4_drsafety'
    else:
        db_name = 'basicinfo_allkeywordsranks_4_drcleaner'

    strsql1 = 'select max(update_date) from {}'.format(db_name)

    try:
        cur.execute(strsql1)
        latest_date = cur.fetchone()[0]
    except Exception, e:
        print e

    if latest_date is None:
        latest_date = datetime.now().date()

    strsql2 = 'select DISTINCT A.keyword, A.\"rank\" as \"RankA\", B.\"rank\" as \"RankB\" from (select keyword , rank from {} where appname like \'{}\' and update_date = \'{}\')A ' \
              'join (select keyword , rank from {} where appname like \'{}\' and update_date = \'{}\')B on A.keyword=B.keyword'.format(
        db_name, app_name1, latest_date, db_name, app_name2, latest_date)

    try:
        cur.execute(strsql2)
        keyword_table1 = cur.fetchall()
        keyword_table1 = list(keyword_table1)
    except Exception, e:
        print e

    if keyword_table1 is None:
        return render_template('common_keyword_info.html',
                               app_name1=None,
                               app_name2=None,
                               keyword_table1=None)

    else:
        return render_template('common_keyword_info.html',
                               app_name1=app_name1,
                               app_name2=app_name2,
                               keyword_table1=keyword_table1)


@app.route('/fuzzysearch')
def get_search_result():
    pass


def get_app_name(app_id):
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    app_name = None

    if app_id is None:
        app_id = 'null'

    sql = 'select pdname_string from basicinfo_product where pdappid_bigint = {}'.format(app_id)

    try:
        cur.execute(sql)
        app_name = cur.fetchone()[0]
    except Exception, e:
        print e

    return app_name


def get_product_id(app_id):
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    product_id = None
    sql = 'SELECT id from basicinfo_product where pdappid_bigint = {}'.format(app_id)

    try:
        cur.execute(sql)
        product_id = cur.fetchone()[0]
    except Exception, e:
        print e

    return product_id


def get_latest_date(product_id):
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    cur = conn.cursor()
    latest_date = None
    sql = 'select date(max(\"UpdateTime\")) from basicinfo_competitorkeywordsearchrank where country_id=\'1\' and product_id = {}'.format(
        product_id)

    try:
        cur.execute(sql)
        latest_date = cur.fetchone()[0]
    except Exception, e:
        print e

    return latest_date


def fuzzyfinder(user_input, collection):
    suggestions = []
    pattern = '.*'.join(user_input)  # Converts 'djm' to 'd.*j.*m'
    regex = re.compile(pattern)  # Compiles a regex.
    for item in collection:
        match = regex.search(item)  # Checks if the current item matches the regex.
        if match:
            suggestions.append((len(match.group()), match.start(), item))
    return [x for _, _, x in sorted(suggestions)]

@app.route('/getfocusedcountry')
def get_focused_country():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    sql = 'select "CountryName_text", "CountryShortName_string", "IsFocusOn", id from basicinfo_country where "IsFocusOn"=True ORDER BY "CountryName_text"'

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["CountryName_text"] = item[0]
            result["CountryShortName_string"] = item[1]
            result["IsFocusOn"] = item[2]
            result["id"] = item[3]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)

@app.route('/getproductlist')
def get_product_list():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    sql = 'select "IsCompetitor", "IsCompetitor_autodetect", id, "pdappid_bigint", "pdcompany_string", "pdcompanyid_int", "pdcurversion_string", ' \
          '"pdname_string", "pduniqueid_string" from basicinfo_product where "pdcompany_string"=\'Trend Micro\''

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["IsCompetitor"] = item[0]
            result["IsCompetitor_autodetect"] = item[1]
            result["id"] = item[2]
            result["pdappid_bigint"] = item[3]
            result["pdcompany_string"] = item[4]
            result["pdcompanyid_int"] = item[5]
            result["pdcurversion_string"] = item[6]
            result["pdname_string"] = item[7]
            result["pduniqueid_string"] = item[8]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)


@app.route('/getdailydownnm')
def get_daily_downnm():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()
    limit = request.args.get('limit', '0')
    country = request.args.get('country', 0)
    startDate = request.args.get('StaticTime__gte', '')
    endDate = request.args.get('StaticTime__lte', '')
    appId = request.args.get('appId', '')


    sql = 'select "StaticTime", "Units", "Version", "appId", "country_id", id from basicinfo_dailysalesinfo ' \
          'where "Type_id" = \'F1\' and "appId" = \'{}\' and "country_id" = {} and "StaticTime" >= \'{}\' and "StaticTime" <= \'{}\''.format(appId, country, startDate, endDate)


    sqlall = 'select "StaticTime", "Units", "Version", "appId", "country_id", id from basicinfo_dailysalesinfo ' \
          'where "Type_id" = \'F1\' and "appId" = \'{}\' and "StaticTime" >= \'{}\' and "StaticTime" <= \'{}\''.format(appId, startDate, endDate)

    try:
        if country != 0:
            cur.execute(sql)
        else:
            cur.execute(sqlall)
        for item in cur.fetchall():
            result = {}
            result["StaticTime"] = str(item[0])
            result["Units"] = item[1]
            result["Version"] = item[2]
            result["appId"] = item[3]
            result["country"] = item[4]
            result["id"] = item[5]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)


@app.route("/getallfreerank")
def get_all_free_rank():
    #allfreerank/?limit=0&UpdateTime__gte=2016-04-28&UpdateTime__lte=2016-05-06&product=1&format=json
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    limit = request.args.get('limit', '0')
    startDate = request.args.get('UpdateTime__gte', '')
    startDate_Fixed = datetime.strptime(startDate, "%Y-%m-%d") - timedelta(days=1)
    startDate = datetime.strftime(startDate_Fixed, "%Y-%m-%d")
    endDate = request.args.get('UpdateTime__lte', '')
    productId = request.args.get('product', '')
    country = request.args.get('country', 0)

    if country == 0:
        sql = 'select "Ranknm", "UpdateTime", "country_id", "genre_search", "product_id" from basicinfo_categoryrank ' \
            'where "genre_search"=\'all\' and "product_id" = \'{}\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\''.format(productId, startDate, endDate)
    else:
        sql = 'select "Ranknm", "UpdateTime", "country_id", "genre_search", "product_id" from basicinfo_categoryrank ' \
            'where "genre_search"=\'all\' and country_id = \'{}\' and "product_id" = \'{}\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\''.format(country, productId, startDate, endDate)


    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["Ranknm"] = item[0]
            result["UpdateTime"] = datetime.strftime(item[1] + timedelta(hours=8), "%Y-%m-%dT%H:%m:%s")
            result["country"] = item[2]
            result["genre_search"] = item[3]
            result["product"] = item[4]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)


@app.route("/getallfreerankbyhr")
def get_all_free_rankbyhr():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    limit = request.args.get('limit', '0')
    startDate = request.args.get('UpdateTime__gte', '')
    startDate_Fixed = datetime.strptime(startDate, "%Y-%m-%d") - timedelta(days=1)
    startDate = datetime.strftime(startDate_Fixed, "%Y-%m-%d")
    endDate = request.args.get('UpdateTime__lte', '')
    productId = request.args.get('product', '')
    country = request.args.get('country', 0)

    sql = 'select "Ranknm", "UpdateTime", "country_id", "genre_search", "product_id" from basicinfo_categoryrankhr ' \
          'where "genre_search"=\'all\' and "product_id" = \'{}\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\' and "country_id" = \'{}\''.format(productId, startDate, endDate,country)

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["Ranknm"] = item[0]
            result["UpdateTime"] = datetime.strftime(item[1] + timedelta(hours=8), "%Y-%m-%dT%H:%m:%s")
            result["country"] = item[2]
            result["genre_search"] = item[3]
            result["product"] = item[4]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)


@app.route("/getutilityfreerank")
def get_utility_freerank():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    limit = request.args.get('limit', '0')
    startDate = request.args.get('UpdateTime__gte', '')
    endDate = request.args.get('UpdateTime__lte', '')
    productId = request.args.get('product', '')
    country = request.args.get('country', 0)

    sql = 'select "Ranknm", "UpdateTime", "country_id", "genre_search", "product_id" from basicinfo_categoryrank ' \
          'where "genre_search"=\'utilities\' and "product_id" = \'{}\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\' and "country_id" = \'{}\''.format(productId, startDate, endDate,country)

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["Ranknm"] = item[0]
            result["UpdateTime"] = datetime.strftime(item[1], "%Y-%m-%dT%H:%m:%s")
            result["country"] = item[2]
            result["genre_search"] = item[3]
            result["product"] = item[4]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)

@app.route("/getutilityfreerankbyhr")
def get_utility_freerankbyhr():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    limit = request.args.get('limit', '0')
    startDate = request.args.get('UpdateTime__gte', '')
    endDate = request.args.get('UpdateTime__lte', '')
    productId = request.args.get('product', '')
    country = request.args.get('country', 0)

    sql = 'select "Ranknm", "UpdateTime", "country_id", "genre_search", "product_id" from basicinfo_categoryrankhr ' \
          'where "genre_search"=\'utilities\' and "product_id" = \'{}\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\' and "country_id" = \'{}\''.format(productId, startDate, endDate,country)

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["Ranknm"] = item[0]
            result["UpdateTime"] = datetime.strftime(item[1], "%Y-%m-%dT%H:%m:%s")
            result["country"] = item[2]
            result["genre_search"] = item[3]
            result["product"] = item[4]
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)

@app.route("/getcompeteallrank")
def get_compete_allrank():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()

    limit = request.args.get('limit', '0')
    startDate = request.args.get('UpdateTime__gte', '')
    startDate_Fixed = datetime.strptime(startDate, "%Y-%m-%d") - timedelta(days=1)
    startDate = datetime.strftime(startDate_Fixed, "%Y-%m-%d")
    endDate = request.args.get('UpdateTime__lte', '')
    country = request.args.get('country', 0)
    product_list = []
    sql0 = 'select "id","pdappid_bigint" ,"pdname_string"from basicinfo_product where "IsCompetitor" = True'

    try:
        cur.execute(sql0)
        for item in cur.fetchall():
            product_dict = {}
            product_dict["id"] = item[0]
            product_dict["pdappid_bigint"] = str(item[1])
            product_dict["pdname_string"] = str(item[2])

            product_list.append(product_dict)
    except Exception, e:
        print e


    for i in range(0,len(product_list)):

        sql1 = 'select "Ranknm", "UpdateTime", "product_id", "genre_search","country_id" from basicinfo_categoryrank ' \
          'where "product_id" = \'{}\' and "genre_search"=\'all\' and "UpdateTime" >= \'{}\' and "UpdateTime" <= \'{}\' and "country_id" = \'{}\' and ("feedtype_id" = 1 OR "feedtype_id" = 4)'.format(product_list[i]["id"], startDate, endDate,country)

        try:
            cur.execute(sql1)
            for item in cur.fetchall():
                result = {}
                result["Ranknm"] = item[0]
                result["UpdateTime"] = datetime.strftime(item[1] + timedelta(hours=8), "%Y-%m-%dT%H:%m:%s")
                result["product"] = {}
                result["product"] = product_list[i]
                result["genre_search"] = item[3]
                result["country"] = item[4]
                objects.append(result)
        except Exception, e:
            print e

    results["objects"] = objects

    return jsonify(results)


@app.route("/getcompetitorproduct")
def get_competitor_product():
    conn = psycopg2.connect("dbname='PromotionDB_New' user='Administrator' host='10.206.132.19' password='mac8.6'")
    results = {}
    objects = []
    cur = conn.cursor()


    sql = 'select "pdname_string", "id" from basicinfo_product where "IsCompetitor"=True'

    try:
        cur.execute(sql)
        for item in cur.fetchall():
            result = {}
            result["pdname_string"] = item[0]
            result["id"] = str(item[1])
            objects.append(result)
    except Exception, e:
        print e

    results["objects"] = objects

    return jsonify(results)



if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    socketio.run(app, host='0.0.0.0', port=5002)
