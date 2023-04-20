from datetime import datetime, timezone
import pymongo

class ExtractTags:
    _mongoclient =  pymongo.MongoClient("mongodb://localhost:27017/")
    _mongoDb = _mongoclient["gps"]
    _mongoCollection = _mongoDb["trackerrawlogs"]
    pipelineResult = {}

    def __init__(self, imei):
        self._pipeline = [
        {
            '$match': {
                'imei': imei,
                'data.altitude': {
                    '$exist': True
                },
                'data.altitude': {
                    '$ne': None
                },
                'data.longitude': {
                    '$exist': True
                },
                'data.longitude': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'type': 1,
                'imei': 1,
                'data': 1,
                'createdAt': 1
            }
        }, {
            '$addFields': {
                'latitude': '$data.latitude',
                'longitude': '$data.longitude',
                'speed': '$data.speed',
                'data_engine': '$data.type',
                'tag': '$data.tag',
                'date': '$data.date',
                'zeroD': '$data.zeroD'
            }
        }, {
            '$match': {
                'data_engine': {
                    '$not': {
                        '$in': [
                            'rssi', 'connection', 'engine'
                        ]
                    }
                },
                'tag': {
                    '$regex': '^t_'
                }
            }
        }, {
            '$match': {
                'date': {
                    '$gte': datetime(2023, 3, 1, 7, 43, 14, tzinfo=timezone.utc)
                }
            }
        }, {
            '$group': {
                '_id': '$tag'
            }
        }
    ]

    def executePipeline(self):
        self.pipelineResult = self._mongoCollection.aggregate(self._pipeline)
        return self

    def transform(self):
        tags = []
        for item in self.pipelineResult:
            tags.append(item["_id"])
        return tags

    def printPipelineResult(self):
        for item in self.pipelineResult:
            print(item)