import os.path
import re
import shutil
import uuid
import zipfile

from django.http import HttpResponse
import requests
import opal_importer.config
import time


def import_data(request):
  response_url = post_exporter_request()['responseUrl']

  retries = opal_importer.config.env.int('MAX_NUMBER_OF_RETRIES')
  timeout = opal_importer.config.env.int('TIMEOUT_IN_SECONDS')
  response_message = 'Working!'

  response_status_url = get_response_status_url(response_url)
  is_ready = False;
  error = False

  while not is_ready and not error and retries > 0:
    r = get_response_status(response_status_url)
    if r.status_code ==  200:
      data = r.json()
      if data == 'OK':
        is_ready = True
      elif data == 'RUNNING':
        time.sleep(timeout)
        retries -= 1
      else:
        error = True
        response_message = 'Error!'
  if not error:
    downloaded_files_directory = download_files(response_url)
    send_files_in_directory_to_opal(downloaded_files_directory)
    shutil.rmtree(downloaded_files_directory)

  return HttpResponse(response_message)


def post_exporter_request():
  url = create_exporter_url()
  params = create_params()
  headers = create_headers()
  return requests.post(url=url, params=params, headers=headers).json()


def create_exporter_url():
  return opal_importer.config.env('EXPORTER_URL') + '/request'


def create_params():
  return {'query': 'Patient', 'query-format': 'FHIR_QUERY',
          'template-id': 'ccp', 'output-format': 'CSV'}


def create_headers():
  return {'x-api-key': opal_importer.config.env('EXPORTER_API_KEY')}


def get_response_status_url(response_url):
  return response_url.replace("response", "status")

def get_response_status(response_status_url):
  return requests.get(url=response_status_url, headers=create_headers())

def download_files(response_url):
  r = requests.get(response_url)
  content_disposition = r.headers.get('Content-Disposition')
  filename = re.findall('filename=(.+)', content_disposition).__getitem__(0)
  temporal_directory = create_temporal_directory()
  file_path = os.path.join(temporal_directory, filename)
  open(file_path, 'wb').write(r.content)
  if '.zip' in filename:
    with zipfile.ZipFile(file_path,'r') as zip_ref:
      zip_ref.extractall(temporal_directory)
    os.remove(file_path)
  return temporal_directory

def send_files_in_directory_to_opal(directory):

  return

def create_temporal_directory():
  path = opal_importer.config.env('TEMP_DIRECTORY')
  temporal_directory_name = 'temp-' + str(uuid.uuid1())
  temporal_directory_path = os.path.join(path, temporal_directory_name)
  os.mkdir(temporal_directory_path)
  return temporal_directory_path
