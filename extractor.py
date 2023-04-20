import re
from datetime import datetime, timezone

import pymongo
import pandas as pd



class Extract:
    _imei = ''
    _mongoclient =  pymongo.MongoClient("mongodb://localhost:27017/")
    _mongoDb = _mongoclient["gps"]
    _mongoCollection = _mongoDb["trackerrawlogs"]
    _pipeline = []
    _tripStartEnd = []
    pipelineResult = {}
    locationPipelineResult = []


    def printPipeline(self):
        print(self._pipeline)

    def executePipeline(self):
        self.pipelineResult = self._mongoCollection.aggregate(self._pipeline)

    def printPipelineResult(self):
        for item in self.pipelineResult:
            print(item)

    def tripBound(self):
        trip = []
        counter = 0
        for item in self.pipelineResult:
            counter +=1
            trip.append(item.get('createdAt'))

            if counter %2 == 0:
                # end = trip.pop().isoformat()
                # start =trip.pop().isoformat()
                end = trip.pop()
                start = trip.pop()
                self._tripStartEnd.append([start,end])

    def extractTrips(self):
        for i,item in enumerate(self._tripStartEnd):
            print('Going to fetch trip No. {}'.format(i))

            self._locationPipline.append({'$match': {'imei': self._imei,
                                                     'createdAt': {'$gte': item[0],
                                                                   '$lte': item[1]}}})
            self._locationPipline.append({'$sort': {"date": 1}})

            aggrigation = self._mongoCollection.aggregate(self._locationPipline)
            self._locationPipline.pop()
            self._locationPipline.pop()
            singleTrip = []

            for j,subItem in enumerate(aggrigation):
                singleTrip.append({
                    'imei': subItem.get('imei'),
                    'createdAt': subItem.get('createdAt'),
                    'date': subItem.get('date'),
                    'latitude': subItem.get('latitude'),
                    'longitude': subItem.get('longitude'),
                    'speed': subItem.get('speed'),
                    'zeroD': subItem.get('zeroD')
                }
                )

            self.locationPipelineResult.append(singleTrip)
        return self.locationPipelineResult

    def __init__(self, imei):
        self._imei = imei
        self._pipeline =[
        {
            '$match': {
                'type': 'event'
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
                'altitude': '$data.altitude',
                'longitude': '$data.longitude',
                'data_engine': '$data.type',
                'tag': '$data.tag'
            }
        }, {
            '$match': {
                'data_engine': 'engine',
                'tag': {
                    '$regex': re.compile(r"^t_")
                }
            }
        },
            {
            '$match': {
                'imei': imei
            }
        },
            {
            '$sort': {
                'createdAt': 1
            }
        }
        ]


    _locationPipline = [
        {
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
                'zeroD': '$data.zeroD',
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
                    '$regex': re.compile(r"^t_")
                }
            }
        },
        {   '$project': {
        'data': 0,
        }
        }
        # {
        #     '$match': {
        #         'imei': '867717034058660',
        #         'createdAt': {
        #             '$gte': datetime(2023, 3, 1, 4, 13, 11, tzinfo=timezone.utc),
        #             '$lte': datetime(2023, 3, 11, 4, 28, 15, tzinfo=timezone.utc)
        #         }
        #     }
        # }
    ]