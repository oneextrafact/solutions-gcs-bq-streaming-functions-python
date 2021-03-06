# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import json
import logging
import os
import traceback
from datetime import datetime
from io import BytesIO
import pandas as pd
import numpy as np
import zipfile

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'vta_vs'
BQ_TABLE = 'vehicle_state'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_error_topic')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_success_topic')
DB = firestore.Client()
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()


def streaming(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref = DB.document(u'streaming_files/%s' % file_name)
    if _was_already_ingested(db_ref):
        _handle_duplication(db_ref)
    else:
        try:
            _insert_into_bigquery(bucket_name, file_name)
            _handle_success(db_ref)
        except Exception:
            _handle_error(db_ref)


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and status.to_dict()['success']


def _handle_duplication(db_ref):
    dups = [_now()]
    data = db_ref.get().to_dict()
    if 'duplication_attempts' in data:
        dups.extend(data['duplication_attempts'])
    db_ref.update({
        'duplication_attempts': dups
    })
    logging.warning('Duplication attempt streaming file \'%s\'' % db_ref.id)


def _insert_into_bigquery(bucket_name, file_name):
    blob = CS.get_bucket(bucket_name).blob(file_name)

    # extract the ZipFile and read the columns we need from it.
    try:
        zf = zipfile.ZipFile(BytesIO(blob.download_as_string()))

        schema = {
            'BUSTOOLS_VERSION': np.str,
            'ROUTE_ID': np.str,
            'STOP_SEQUENCE': np.float,
            'BUS_ID': np.str,
            'EVENT_TIME': np.str,
            'EVENT_TIME_UTC': np.str,
            'ODOMETER_DISTANCE': np.float,
            'OPERATOR_ID': np.str,
            'EVENT_TYPE': np.float,
            'PASSENGER_LOAD': np.float,
            'BLOCK_ID': np.float,
            'TRIP_KEY': np.float,
            'TRIP_START_TIME': np.str,
            'ODOMETER_CUMULATIVE': np.float,
            'WORK_SCHED_DATE': np.str,
            'STATEOFCHARGE': np.float
        }

        df = pd.read_csv(zf.open(zf.infolist()[0]), delimiter=',',
                         error_bad_lines=False,
                         usecols=schema.keys(),
                         dtype=schema,
                         skiprows=[1])
        df['EVENT_TIME'] = df.EVENT_TIME.apply(lambda x: pd.to_datetime(
            x, format='%y%m%d%H%M%S', errors='coerce')).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['EVENT_TIME_UTC'] = df.EVENT_TIME_UTC.apply(lambda x: pd.to_datetime(
            x, format='%y%m%d%H%M%S', errors='coerce')).dt.strftime('%Y-%m-%d %H:%M:%SZ')
        df['TRIP_START_TIME'] = df.TRIP_START_TIME.apply(lambda x: pd.to_datetime(
            x, format='%y%m%d%H%M%S', errors='coerce')).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['WORK_SCHED_DATE'] = df.WORK_SCHED_DATE.apply(lambda x: pd.to_datetime(
            x, format='%y%m%d', errors='coerce')).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.rename({'STATEOFCHARGE': 'STATE_OF_CHARGE'}, axis=1, inplace=True)
    except IOError as e:
        raise PandasError(e)
    except zipfile.BadZipFile as e:
        raise PandasError(e)

    rows = json.loads(df.to_json(orient='records'))
    row_ids = ["{}_{}".format(file_name, str(i).rjust(5,'0')) for i, _ in enumerate(rows)]
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=rows,
                                 row_ids=row_ids,
                                 retry=retry.Retry(deadline=30))
    if errors:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.error(message)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


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


class PandasError(Exception):
    '''Exception raised for an inability to parse the file'''
    def __init__(self, errors):
        super().__init__(self._format(errors))

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
