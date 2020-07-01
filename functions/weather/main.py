import os
import json
from google.cloud import bigquery
from google.cloud import error_reporting
from google.api_core import retry
from google.cloud import firestore
from xml.etree import ElementTree
import traceback
import logging
import requests
import datetime
import pytz
import pandas as pd
import dateutil

PROJECT_ID = os.getenv("GCP_PROJECT")
BQ_DATASET = 'vta_vs'
BQ_TABLE = 'weather_forecast'
BQ = bigquery.Client()
DB = firestore.Client()
client = error_reporting.Client()


def weather(request):
    """
    Responds to a request from Cloud Scheduler. When invoked, gets the weather forecast
    for the (constant) list of lat/long combinations and stores the result in a BigQuery table.
    :param request:
    :return: None
    """

    # get the forecast
    lat_lon_str_escaped = os.getenv("LAT_LON_STR")
    forecast_url = (
        """https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?"""
        """whichClient=NDFDgenLatLonList"""
        """&listLatLon={}"""
        """&product=time-series"""
        """&Unit=m"""
        """&temp=temp"""
        """&pop12=pop12"""
        """&Submit=Submit""").format(lat_lon_str_escaped)
    response = requests.get(forecast_url)
    if response.status_code == 200:
        logging.info("Downloaded forecast.")
        response_xml = ElementTree.fromstring(response.content)
        forecast_time = response_xml.find('head').find('product').find('creation-date').text
    else:
        logging.error("Non-success return code from NDFD request")
        raise RuntimeError('Non-success return code from NDFD request')

    # see if we have already seen this record
    logging.info("Checking for duplicates.")
    db_ref = DB.document(u'weather_forecasts/%s' % forecast_time)
    if _was_already_ingested(db_ref):
        logging.warning('Duplication attempt streaming file \'%s\'' % db_ref.id)
        return
    else:
        try:
            logging.info("Inserting into BigQuery.")
            _insert_into_bigquery(response_xml, forecast_time)
            _handle_success(db_ref)
        except Exception:
            logging.error("Could not insert into BigQuery")
            _handle_error(db_ref)


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and status.to_dict()['success']


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


def _handle_success(db_ref):
    message = 'Forecast \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming forecast \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    logging.error(message)


def _insert_into_bigquery(weather_xml, forecast_time):
    logging.info("Processing frame")
    tree = weather_xml.find('data')
    time_layouts_df = pd.DataFrame()
    for time_layout in tree.findall('time-layout'):
        time_layouts = []
        time_layout_key = time_layout.find('layout-key').text
        for index, start_time in enumerate(time_layout.findall('start-valid-time')):
            time_layouts.append({'time_layout': time_layout_key,
                                 'time_index': index,
                                 'time': dateutil.parser.parse(start_time.text)})
        time_layouts_df = pd.concat([time_layouts_df, pd.DataFrame(time_layouts)])

    parameters_df = pd.DataFrame()
    for parameter in tree.findall('parameters'):
        point_name = parameter.attrib['applicable-location']
        for observation in parameter:
            observations = []
            units = observation.attrib['units']
            time_layout = observation.attrib['time-layout']
            observation_name = "{} ({})".format(observation.find('name').text, units)
            for time_index, value in enumerate(observation.findall('value')):
                observations.append({"point_name": point_name,
                                     "time_layout": time_layout,
                                     "time_index": time_index,
                                     observation_name: value.text
                                     })
            observation_df = pd.DataFrame(observations)
            observation_df = observation_df.merge(time_layouts_df)
            observation_df.drop(["time_layout", "time_index"], axis=1, inplace=True)
            observation_df = observation_df.set_index("time").resample("H").first().ffill()
            parameters_df = pd.concat([parameters_df, observation_df])
    parameters_df = parameters_df.groupby(['point_name', 'time']).last().reset_index().dropna()
    parameters_df['time'] = parameters_df.time.apply(lambda x: x.astimezone('UTC'))
    parameters_df['forecast_time'] = forecast_time
    parameters_df['temperature_c'] = parameters_df['Temperature (Celsius)']
    parameters_df['pop12'] = parameters_df['12 Hourly Probability of Precipitation (percent)']

    rows = json.loads(parameters_df[[
        'point_name',
        'time',
        'forecast_time',
        'temperature_c',
        'pop12'
    ]].to_json(orient='records'))
    row_ids = [forecast_time]
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    logging.info("Starting insert into BigQuery")
    errors = BQ.insert_rows_json(table,
                                 json_rows=rows,
                                 row_ids=row_ids,
                                 retry=retry.Retry(deadline=30))
    if errors:
        logging.error(errors)
        raise BigQueryError(errors)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
