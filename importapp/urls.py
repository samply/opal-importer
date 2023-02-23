from django.urls import path
from . import data_reader

# URL Conf module
urlpatterns = [
  path('import/', data_reader.import_data),
]
