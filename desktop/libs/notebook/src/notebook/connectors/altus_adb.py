#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import json
import re

from django.urls import reverse
from django.utils.translation import ugettext as _

from metadata.workload_analytics_client import WorkfloadAnalyticsClient

from notebook.connectors.altus import AnalyticDbApi
from notebook.connectors.base import Api, QueryError


LOG = logging.getLogger(__name__)


RUNNING_STATES = ('QUEUED', 'RUNNING', 'SUBMITTING')


class AltusAdbApi(Api):

  def __init__(self, user, cluster_name, interpreter=None, request=None):
    Api.__init__(self, user, interpreter=interpreter, request=request)
    self.cluster_name = cluster_name


  def execute(self, notebook, snippet):
    statement = snippet['statement']

    handle = AnalyticDbApi(self.user).submit_hive_job(self.cluster_name, statement, params=None, job_xml=None)
    job = handle['jobs'][0]

    if job['status'] not in RUNNING_STATES:
      raise QueryError('Submission failure', handle=job['status'])

    return {
      'id': job['jobId'],
      'crn': job['crn'],
      'has_result_set': False,
    }


  def check_status(self, notebook, snippet):
    response = {'status': 'running'}

    job_id = snippet['result']['handle']['id']

    handle = AnalyticDbApi(self.user).list_jobs(job_ids=[job_id])
    job = handle['jobs'][0]

    if job['status'] in RUNNING_STATES:
      return response
    elif job['status'] in ('failed', 'terminated'):
      raise QueryError(_('Job was %s') % job['status'])
    else:
      response['status'] = 'available'

    return response


  def fetch_result(self, notebook, snippet, rows, start_over):
    return {
        'data':  [[_('Job successfully completed.')]],
        'meta': [{'name': 'Header', 'type': 'STRING_TYPE', 'comment': ''}],
        'type': 'table',
        'has_more': False,
    }


  def cancel(self, notebook, snippet):
    if snippet['result']['handle'].get('id'):
      job_id = snippet['result']['handle']['id']
      AnalyticDbApi(self.user).terminate_job(job_id=job_id)
      response = {'status': 0}
    else:
      response = {'status': -1, 'message': _('Could not cancel because of unsuccessful submition.')}

    return response


  def get_log(self, notebook, snippet, startFrom=0, size=None):
    logs = WorkfloadAnalyticsClient(self.user).get_mr_task_attempt_log(
        operation_execution_id='cedb71ae-0956-42e1-8578-87b9261d4a37',
        attempt_id='attempt_1499705340501_0045_m_000000_0'
    )

    return ''.join(re.findall('(?<=>>> Invoking Beeline command line now >>>)(.*?)(?=<<< Invocation of Beeline command completed <<<)', logs['stdout'], re.DOTALL))


  def progress(self, snippet, logs):
    return 50


  def get_jobs(self, notebook, snippet, logs):
    ## 50cf0e00-746b-4d86-b8e3-f2722296df71
    job_id = snippet['result']['handle']['id']
    return [{
        'name': job_id,
        'url': reverse('jobbrowser.views.apps') + '#!' + job_id,
        'started': True,
        'finished': False # Would need call to check_status
      }
    ]


  def autocomplete(self, snippet, database=None, table=None, column=None, nested=None):
    url_path = '/notebook/api/autocomplete'
    url_parameters = []

    if database is not None:
      url_path = '%s/%s' % (url_path, database)
      url_parameters = []

    return HueQuery(self.user, cluster_crn=self.cluster_name).do_post(url_path=url_path)['payload']


class HueQuery():
  def __init__(self, user, cluster_crn):
    self.user = user
    self.cluster_crn = cluster_crn
    self.api = AnalyticDbApi(self.user)

  def do_post(self, url_path):
    payload = '''{"method":"POST","url":"https://localhost:8888''' + url_path +'''","httpVersion":"HTTP/1.1","headers":[{"name":"Accept-Encoding","value":"gzip, deflate, br"},{"name":"Content-Type","value":"application/x-www-form-urlencoded; charset=UTF-8"},{"name":"Accept","value":"*/*"},{"name":"X-Requested-With","value":"XMLHttpRequest"},{"name":"Connection","value":"keep-alive"}],"queryString":[],"postData": {
        "mimeType": "application/x-www-form-urlencoded; charset=UTF-8",
        "text": "snippet=%7B%22type%22%3A%22impala%22%2C%22source%22%3A%22data%22%7D&cluster=%22default-romain%22",
        "params": [
          {
            "name": "snippet",
            "value": "%7B%22type%22%3A%22impala%22%2C%22source%22%3A%22data%22%7D"
          },
          {
            "name": "cluster",
            "value": "%22default-romain%22"
          }
        ]
      }}''' 
    
    return self.api.submit_hue_query(self.cluster_crn, payload)
