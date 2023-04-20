import requests
import haversine as hs


class LocationTransformer():
    imei = ''
    tripNumber = 0
    locations = []

    averageSpeed = 0
    averageDistance = 0

    startDateJalali = ''
    endDateJalali = ''

    startPositionAddress = ''
    endPositionAddress = ''

    _parsiHost= "https://api.parsimap.ir"
    _parsiToken= 'p1be1846ea2518472b811debaae1140082141d20fe'

    def __init__(self, imei, tripNumber, locations, ):
        self.imei = imei
        self.tripNumber = tripNumber
        self.locations = locations


    def startDate(self):
        self.startDateJalali = 'tojalali'
        return self

    def endDate(self):
        self.endDateJalali = 'tojalali'
        return self

    def positionTranslator(self):
        host = self._parsiHost + '/geocode/reverse'

        _startLocation = self.locations[0]
        _endLocation = self.locations[len(self.locations -1)]
        self.startPositionAddress = requests.get(url=host, params={
            'key': self._parsiToken,
            'location': "{},{}".format(_startLocation.get('longitude'), _startLocation.get('latitude')),
            # GeoJson
            'local_address': 'false',
            'approx_address': 'true',
            'subdivision': 'true',
            'plate': 'false',
            'request_id': 'false',
        }).json()
        self.endPositionAddress = requests.get(url=host, params={
            'key': self._parsiToken,
            'location': "{},{}".format(_endLocation.get('longitude'), _endLocation.get('latitude')),
            # GeoJson
            'local_address': 'false',
            'approx_address': 'true',
            'subdivision': 'true',
            'plate': 'false',
            'request_id': 'false',
        }).json()