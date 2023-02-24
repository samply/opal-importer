from django.urls import path
from . import importer

# URL Conf module
urlpatterns = [
  path('import/', importer.import_data),
]
