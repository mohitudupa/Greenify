from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions
from bson.objectid import ObjectId
from io import StringIO
import pymongo, time
import pandas as pd
import numpy as np


client = pymongo.MongoClient()
db = client["greenify"]
sense = db["senses"]
chunk_size = 10
ID = ObjectId("5b68adf89441db0d18269a7e")

view = sense.find_one({"_id": ID})
end_chunk = view["end_chunk"]
checkpoint = view["checkpoint"]
chunk_fill = len(sense.find_one({"chunk_index": end_chunk})["ts"])


class PutCSV(APIView):
    permission_classes = (permissions.AllowAny,)

    def post(self, request, format=None):
        text_data = ""
        for chunk in request.data["data"].chunks():
            text_data += chunk.decode()

        upload_data = np.array(pd.read_csv(StringIO(text_data)), dtype=np.float64).tolist()

        timelist = [time.time()] * len(upload_data)

        sense.update_one({"_id": ID}, {"$push" : { "data": {"$each": upload_data}, "ts": {"$each": timelist}}})
        return Response({"success": "Data uploaded successfully"})


"""class PutList(APIView):
    permission_classes = (permissions.AllowAny,)

    def post(self, request, format=None):

        upload_data = np.array(request.data["data"], dtype=np.float64).tolist()

        timelist = [time.time()] * len(upload_data)

        sense.update_one({"_id": ID}, {"$push" :{ "data": {"$each": upload_data}, "ts": {"$each": timelist}}})
        return Response({"success": "Data uploaded successfully"})"""


class PutList(APIView):
    permission_classes = (permissions.AllowAny,)

    def post(self, request, format=None):

        data = np.array(request.data["data"], dtype=np.float64).tolist()
        ts = [time.time()]

        sense.update_one({"chunk_index": end_chunk}, {"$push" :{ "data": {"$each": data}, "ts": {"$each": ts}}})
        chunk_fill += 1
        if chunk_fill == chunk_size:
            chunk_fill, end_chunk = 0, end_chunk + 1
            sense.insert_one({"chunk_index": end, "data": [], "ts": []})
            sense.update_one({"_id": ID}, {"end_chunk": end_chunk})

        return Response({"success": "Data uploaded successfully"})


class PeekChunk(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, format=None):

        checkpoint_chunk_index = int(checkpoint / 256)
        checkpoint_offset = checkpoint % 256

        chunk = sense.find_one({"chunk_index": checkpoint_chunk_index})

        return Response({"data": chunk["data"][checkpoint_offset:], "time_stamp": view["ts"][checkpoint_offset:]})


class GetChunk(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, format=None):


        checkpoint_chunk_index = int(checkpoint / 256)
        checkpoint_offset = checkpoint % 256

        chunk = sense.find_one({"chunk_index": checkpoint_chunk_index})

        sense.update_one({"_id": ID}, {"$set": {"checkpoint": (checkpoint_chunk_index + 1) * 256}})

        return Response({"data": chunk["data"][checkpoint_offset:], "time_stamp": view["ts"][checkpoint_offset:]})

        view = sense.find_one({"_id": ID})
        
        data = view["buffer_data"] + view["data"]
        ts = view["buffer_ts"] + view["ts"]
        i, end = 0, view["end"]

        while(len(data[i: (i + chunk_size)]) == chunk_size):
            sense.insert_one({"index": end, "data": data[i: (i + chunk_size)], "ts": ts[i: (i + chunk_size)]})
            i, end = i + chunk_size, end + 1

        sense.update_one({"_id": ID}, {"$set": {"end": end, 
                                                "data": [], 
                                                "ts": [], 
                                                "buffer_data": data[i: (i + chunk_size)], 
                                                "buffer_ts": ts[i: (i + chunk_size)]}})

        return Response({"data": view["data"], "time_stamp": view["ts"]})