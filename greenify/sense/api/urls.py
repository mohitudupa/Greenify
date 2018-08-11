from django.contrib import admin
from django.urls import path
from . import views

urlpatterns = [
    path('put/data/<int:s1>/<int:s2>/<int:s3>/<int:s4>/<int:s5>/<int:s6>/', views.PutData.as_view(), name="put_data"),
    path('peek/chunk/', views.PeekChunk.as_view(), name="peek_chunk"),
    path('get/chunk/', views.GetChunk.as_view(), name="get_chunk"),
]
