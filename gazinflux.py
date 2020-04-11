#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import locale
import math
from itertools import chain, groupby
from dateutil.relativedelta import relativedelta
from influxdb import InfluxDBClient
import gazpar
import json

import argparse
import logging
import pprint
from envparse import env

PFILE = "/.params"
DOCKER_MANDATORY_VARENV=['GRDF_USERNAME','GRDF_PASSWORD','INFLUXDB_HOST','INFLUXDB_DATABASE','INFLUXDB_USERNAME','INFLUXDB_PASSWORD']
DOCKER_OPTIONAL_VARENV=['INFLUXDB_PORT', 'INFLUXDB_SSL', 'INFLUXDB_VERIFY_SSL']


# Sub to return format wanted by linky.py
def _dayToStr(date):
    return date.strftime("%d/%m/%Y")

# Open file with params for influxdb, GRDF API
def _openParams(pfile):
    # Try to load environment variables
    if set(DOCKER_MANDATORY_VARENV).issubset(set(os.environ)):
        return {'grdf': {'username': env(DOCKER_MANDATORY_VARENV[0]),
                         'password': env(DOCKER_MANDATORY_VARENV[1])},
                'influx': {'host': env(DOCKER_MANDATORY_VARENV[2]),
                           'port': env.int(DOCKER_OPTIONAL_VARENV[0], default=8086),
                           'db': env(DOCKER_MANDATORY_VARENV[3]),
                           'username': env(DOCKER_MANDATORY_VARENV[4]),
                           'password': env(DOCKER_MANDATORY_VARENV[5]),
                           'ssl': env.bool(DOCKER_OPTIONAL_VARENV[1], default=True),
                           'verify_ssl': env.bool(DOCKER_OPTIONAL_VARENV[2], default=True)}}
    # Try to load .params then programs_dir/.params
    elif os.path.isfile(os.getcwd() + pfile):
        p = os.getcwd() + pfile
    elif os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + pfile):
        p = os.path.dirname(os.path.realpath(__file__)) + pfile
    else:
        if (os.getcwd() + pfile != os.path.dirname(os.path.realpath(__file__)) + pfile):
            logging.error('file %s or %s not exist', os.path.realpath(os.getcwd() + pfile) , os.path.dirname(os.path.realpath(__file__)) + pfile)
        else:
            logging.error('file %s not exist', os.getcwd() + pfile )
        sys.exit(1)
    try:
        f = open(p, 'r')
        try:
            array = json.load(f)
        except ValueError as e:
            logging.error('decoding JSON has failed', e)
            sys.exit(1)
    except IOError:
        logging.error('cannot open %s', p)
        sys.exit(1)
    else:
        f.close()
        return array


# Sub to get StartDate depending today - daysNumber
def _getStartDate(today, daysNumber):
    return today - relativedelta(days=daysNumber)

# Get the midnight timestamp for startDate
def _getStartTS(daysNumber):
    date = (datetime.datetime.now().replace(hour=12,minute=0,second=0,microsecond=0) - relativedelta(days=daysNumber))
    return date.timestamp()

# Get startDate with influxDB lastdate +1
def _getStartDateInfluxDb(client,measurement):
    result = client.query("SELECT * from " + measurement + " ORDER BY time DESC LIMIT 1")
    try:
        data = list(result.get_points())
        return datetime.datetime.strptime(data[0]['time'], '%Y-%m-%dT%H:%M:%SZ') + relativedelta(days=1)
    except:
        logging.error("There is no data in '%s' database on host %s", params['influx']['db'], params['influx']['host'])

# Let's start here !

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d",  "--days",    type=int, help="Number of days from now to download", default=1)
    parser.add_argument("-l",  "--last",    action="store_true", help="Check from InfluxDb the number of missing days", default=False)
    parser.add_argument("-v",  "--verbose", action="store_true", help="More verbose", default=False)
    args = parser.parse_args()

    pp = pprint.PrettyPrinter(indent=4)
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    params = _openParams(PFILE)

    # Try to log in InfluxDB Server
    try:
        logging.info("logging in InfluxDB Server Host %s...", params['influx']['host'])
        client = InfluxDBClient(params['influx']['host'], params['influx']['port'],
                    params['influx']['username'], params['influx']['password'],
                    params['influx']['db'], ssl=params['influx']['ssl'], verify_ssl=params['influx']['verify_ssl'])
        logging.info("logged in InfluxDB Server Host %s succesfully", params['influx']['host'])
    except:
        logging.error("unable to login on %s", params['influx']['host'])
        sys.exit(1)

    # Try to log in GRDF API
    try:
        logging.info("logging in GRDF URI %s...", gazpar.API_BASE_URI)
        token = gazpar.login(params['grdf']['username'], params['grdf']['password'])
        logging.info("logged in successfully!")
    except:
        logging.error("unable to login on %s : %s", gazpar.API_BASE_URI, exc)
        sys.exit(1)

    # Calculate start/endDate and firstTS for data to request/parse
    if args.last:
        logging.info("looking for last value date on InfluxDB 'conzo_gaz' on host %s...", params['influx']['host'])
        startDate = _getStartDateInfluxDb(client,"conso_gaz")
        if not startDate:
            logging.info('I will get the first date on GRDF portal')
            startDate = datetime.datetime.strptime(gazpar.get_start_date(token), "%d/%m/%Y").date()
            logging.info('found %s', startDate)
        startDateString = _dayToStr(startDate)
        logging.info("found last fetch date %s on InfluxDB 'conzo_gaz' on host %s...",
                     startDateString, params['influx']['host'])
        firstTS = datetime.datetime.combine(startDate,datetime.time(12,0,0)).timestamp()
    else :
        logging.warn("GRDF will perhaps has not all data for the last %s days ",args.days)
        startDate = _getStartDate(datetime.date.today(), args.days)
        firstTS =  _getStartTS(args.days)

    endDate = datetime.date.today()

    periodWanted = endDate - startDate
    nbCallsToMake = math.ceil(periodWanted.days/12)
    if periodWanted.days > 12:
        logging.info("more than 12 days (%d) are wanted, I will need to make %d calls to GRDF to get all data", periodWanted.days, math.ceil(periodWanted.days/12)) 

    logging.info("will use %s as startDate and %s as endDate", _dayToStr(startDate), _dayToStr(endDate))

    # Try to get data from GRDF API
    resGrdf = []
    try:
        startDateDownload = startDate
        endDateDownload = startDateDownload + relativedelta(days=12)
        for _ in range(nbCallsToMake):
            if endDateDownload > endDate:
                endDateDownload = endDate
            logging.info("get Data from GRDF from {0} to {1}".format(
                startDateDownload, endDateDownload))
            # Get result from GRDF by day
            resGrdf.append(gazpar.get_data_per_day(token, _dayToStr(startDateDownload), _dayToStr(endDateDownload)))

            if (args.verbose):
                pp.pprint(resGrdf)
            
            startDateDownload = startDateDownload + relativedelta(days=12)
            endDateDownload = endDateDownload + relativedelta(days=12)
    except:
        logging.error("unable to get data from GRDF")
        sys.exit(1)

    # When we have all values let's start parse data and pushing it
    jsonInflux = []
    i = 0
    for d in chain.from_iterable(resGrdf):
        t = datetime.datetime.strptime(d['date'] + " 12:00", '%d-%m-%Y %H:%M')
        logging.info(("found value : {0:3} kWh / {1:7.2f} m3 at {2}").format(d['kwh'], d['mcube'], t.strftime('%Y-%m-%dT%H:%M:%SZ')))
        if t.timestamp() >= firstTS:
            logging.info(("value added to jsonInflux as {0} >= {1}").format(t.strftime('%Y-%m-%d %H:%M'), datetime.datetime.fromtimestamp(firstTS).strftime('%Y-%m-%d %H:%M')))
            jsonInflux.append({
                           "measurement": "conso_gaz",
                           "tags": {
                               "fetch_date" : endDate
                           },
                           "time": t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                           "fields": {
                               "kwh": d['kwh'],
                               "mcube": d['mcube']
                           }
                         })
        else:
            logging.info(("value NOT added to jsonInflux as {0} > {1}").format(t.timestamp(), firstTS))
        i=+1
    if (args.verbose):
        pp.pprint(jsonInflux)
    logging.info("trying to write {0} points to influxDB".format(len(jsonInflux)))
    try:
        client.write_points(jsonInflux)
    except:
        logging.info("unable to write data points to influxdb")
    else:
        logging.info("done")
