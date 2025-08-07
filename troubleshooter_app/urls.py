from django.urls import path
from . import views

app_name = 'troubleshooter_app' # Namespace for your app's URLs

urlpatterns = [
    path('', views.troubleshooter_view, name='troubleshooter'),
]