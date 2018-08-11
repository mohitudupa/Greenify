from django.contrib import admin
from django.urls import path, include


app_name = 'sense'


urlpatterns = [
    path('api/', include('sense.api.urls')),
]
