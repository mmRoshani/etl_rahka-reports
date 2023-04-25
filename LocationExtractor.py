import pymongo


class Extractlocation:
    _mongoclient = pymongo.MongoClient(
        "mongodb://localhost:27017/", connectTimeoutMS=50000000
    )
    _mongoDb = _mongoclient["gps"]
    _mongoCollection = _mongoDb["trackerrawlogs"]
    pipelineResult = {}

    def __init__(self, tag):
        self._pipeline = [
            {
                "$match": {
                    "data.tag": tag,
                }
            },
            {"$project": {"imei": 1, "type": 1, "data": 1}},
            {
                "$match": {
                    "type": "location",
                    "data.type": {"$not": {"$in": ["rssi", "connection", "engine"]}},
                }
            },
            {
                "$project": {
                    "data.rssi": 0,
                    "data.altitude": 0,
                    "data.altitude": 0,
                    "data.satelliteInView": 0,
                    "data.satelliteInUsed": 0,
                    "data.fixMode": 0,
                }
            },
        ]

    def executePipeline(self):
        self.pipelineResult = self._mongoCollection.aggregate(
            self._pipeline, maxTimeMS=5000000
        )
        return self

    def transform(self):
        trip = []
        for item in self.pipelineResult:
            trip.append(item["data"])
        return trip

    def printPipelineResult(self):
        for item in self.pipelineResult:
            print(item)
