import os.path
import re
import shutil
import time
import uuid
import zipfile

import requests
from django.http import HttpResponse

import opal_importer.config


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
    if r.status_code == 200:
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
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
      zip_ref.extractall(temporal_directory)
    os.remove(file_path)
  return temporal_directory


def send_files_in_directory_to_opal(directory):

  r = os.system(get_opal_project_command())
  if r != 0:
    os.system(create_opal_project_command())

  for file in os.listdir(directory):
    file_path = os.path.join(directory, file)
    os.system(create_opal_upload_command(file_path))
    os.system(create_opal_import_command(file))
    os.system(create_opal_delete_file_command(file))

  return

def get_opal_project_command():
  server_url = opal_importer.config.env('OPAL_URL')
  user = opal_importer.config.env('OPAL_USER')
  password = opal_importer.config.env('OPAL_PASSWORD')
  project = opal_importer.config.env('OPAL_PROJECT')

  return 'opal project --opal ' + server_url + ' --user ' + user + ' --password ' + password + ' --name ' + project


def create_opal_project_command():
  server_url = opal_importer.config.env('OPAL_URL')
  user = opal_importer.config.env('OPAL_USER')
  password = opal_importer.config.env('OPAL_PASSWORD')
  project = opal_importer.config.env('OPAL_PROJECT')
  database = opal_importer.config.env('OPAL_DB')

  return 'opal project --opal ' + server_url + ' --user ' + user + ' --password ' + password + ' --add --name ' + project + ' --database ' + database

def create_opal_upload_command(file_path):
  server_url = opal_importer.config.env('OPAL_URL')
  user = opal_importer.config.env('OPAL_USER')
  password = opal_importer.config.env('OPAL_PASSWORD')
  directory = opal_importer.config.env('OPAL_DIRECTORY')
  return 'opal file --opal ' + server_url + ' --user ' + user + ' --password ' + password + ' -up ' + file_path + ' ' + directory;

def create_opal_delete_file_command(file):
  server_url = opal_importer.config.env('OPAL_URL')
  user = opal_importer.config.env('OPAL_USER')
  password = opal_importer.config.env('OPAL_PASSWORD')
  directory = opal_importer.config.env('OPAL_DIRECTORY')
  file_path = os.path.join(directory, file)
  return 'opal file --opal ' + server_url + ' --user ' + user + ' --password ' + password + ' -dt ' + file_path + ' --force';


def create_opal_import_command(file):
  server_url = opal_importer.config.env('OPAL_URL')
  user = opal_importer.config.env('OPAL_USER')
  password = opal_importer.config.env('OPAL_PASSWORD')
  destination = opal_importer.config.env('OPAL_PROJECT')
  directory = opal_importer.config.env('OPAL_DIRECTORY')
  file_path = os.path.join(directory,file)
  tables = fetch_type(file)
  separator = opal_importer.config.env('OPAL_SEPARATOR')
  entity_type = tables
  return 'opal import-csv --opal ' + server_url + ' --user ' + user + ' --password ' + password + ' --destination ' + destination +' --path ' + file_path + ' --tables '+tables+' --separator "'+separator+'" --type '+ entity_type;

def fetch_type(filename):
  index = filename.index('-')
  return filename[:index]

def create_temporal_directory():
  path = opal_importer.config.env('TEMP_DIRECTORY')
  temporal_directory_name = 'temp-' + str(uuid.uuid1())
  temporal_directory_path = os.path.join(path, temporal_directory_name)
  os.mkdir(temporal_directory_path)
  return temporal_directory_path
