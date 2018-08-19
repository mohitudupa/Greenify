from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions
from bson.objectid import ObjectId
import pymongo, time


client = pymongo.MongoClient()
db = client["greenify"]
sense = db["senses"]
"""
    Database Structure:
        Data is stores in the senses collection inside the greenifi database.

        The first document is used to hold data on the number of chunks. It holds the following fields.
            end_chunk: integer index of last chunk
            checkpoint: integer offset of lasted retrived data entry

        The following documents have the following fields:
            chunk_index: integer chunk number
            s1: array of data from sensor 1 (max size 256)
            s2: array of data from sensor 1 (max size 256)
            s3: array of data from sensor 1 (max size 256)
            s4: array of data from sensor 1 (max size 256)
            s5: array of data from sensor 1 (max size 256)
            s6: array of data from sensor 1 (max size 256)
            ts: array of timestamps for individual data upload actions (max size 256)

"""


# ID of the first document in the collection
ID = ObjectId("5b68adf89441db0d18269a7e") 
# Getting the first document to retrieve checkpoint and last chunk data.
view = sense.find_one({"_id": ID})

# Chunk size. Default: 256
chunk_size = 3
end_chunk = view["end_chunk"]
checkpoint = view["checkpoint"]
chunk_fill = len(sense.find_one({"chunk_index": end_chunk})["s1"])


class PutData(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, s1, s2, s3, s4, s5, s6, format=None):

        """
            Class PutData
            Request type:   GET
            URL:            /sense/api/put/data/<int:s1>/<int:s2>/<int:s3>/<int:s4>/<int:s5>/<int:s6>/
            Docs:           s1, s2, s3, s4, s5, s6 are integer data from sensor 1 to sensor 6.
                            All 6 integer values will be pushed into the database under the collection sense 
                            along with a timestamp.
                            Documents in the sense collection hold data in chunks of size defined by chuk_size, Default: 256

        """

        global end_chunk, chunk_fill, chunk_size
        
        sense.update_one({"chunk_index": end_chunk}, {"$push" :{ "s1": s1, "s2": s2, "s3": s3, "s4": s4, "s5": s5, "s6": s6, "ts": time.time()}})
        chunk_fill += 1
        
        if chunk_fill == chunk_size:
            chunk_fill, end_chunk = 0, end_chunk + 1
            sense.insert_one({"chunk_index": end_chunk, "s1": [], "s2": [],"s3": [],"s4": [],"s5": [],"s6": [],"ts": []})
            sense.update_one({"_id": ID}, {"$set": {"end_chunk": end_chunk}})

        return Response({"success": "Data uploaded successfully"})


class GetChunk(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, get, format=None):

        """
            Class GetChunk
            Request type:   GET
            URL:            /sense/api/get/chunk/
            Docs:           The Get Chunk function is used to retrieve new data from the last retrieved chunk. It marks 
                            the returned data as old and new data will be returned on subsequent calls.

                            Only new data from the chunk is returned not the entire chunk

                            The returned data is of size 0 to 256 entries.
        """

        if get not in ("get", "peek"):
        	return Response({"error": "only get and peek are allowed"})

        global checkpoint, chunk_size
        cc_index, cc_offset = divmod(checkpoint, chunk_size)

        chunk = sense.find_one({"chunk_index": cc_index})
        del chunk["_id"], chunk["chunk_index"]
        for key in chunk.keys():
            chunk[key] = chunk[key][cc_offset:]

        if get == "get":
            checkpoint += len(chunk["s1"])
            sense.update_one({"_id": ID}, {"$set": {"checkpoint": checkpoint}})

        return Response(chunk)
