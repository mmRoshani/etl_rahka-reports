import requests
import haversine as hs
# from persiantools.jdatetime import sdJalaliDate

class Transform:
    _imei = ''
    _parsiHost= "https://api.parsimap.ir"
    _parsiToken= 'p1be1846ea2518472b811debaae1140082141d20fe'
    _parsiEnd = {}
    _parsiStart = {}
    _distance = 0
    _speedAvg = 0
    _trips = []
    totalReport = []

    def __init__(self,trips):
        self._trips = trips

    def _extractStartEndLocatio(self, trip):
        self._startLocation = trip[0]
        self._endLocation = trip[len(trip) - 1]
        return self

    def _addressTranslator(self):

        host = self._parsiHost + '/geocode/reverse'
        if self._startLocation.get('longitude') is not None and self._startLocation.get('latitude') is not None:
            self._parsiStart = requests.get(url=host, params={
                'key': self._parsiToken,
                'location': "{},{}".format(self._startLocation.get('longitude'), self._startLocation.get('latitude')),# GeoJson
                'local_address': 'false',
                'approx_address': 'true',
                'subdivision': 'true',
                'plate': 'false',
                'request_id': 'false',
            }).json()
        if self._endLocation.get('longitude') is not None and self._endLocation.get('latitude') is not None:
            self._parsiEnd = requests.get(url=host, params={
                'key': self._parsiToken,
                'location': "{},{}".format(self._endLocation.get('longitude'), self._endLocation.get('latitude')),  # GeoJson
                'local_address': 'false',
                'approx_address': 'true',
                'subdivision': 'true',
                'plate': 'false',
                'request_id': 'false',
            }).json()

    def trasformation(self):
        if len(self._trips) >0 :
            print('transforming on trip with count: {} ...'.format(len(self._trips)))
            self._imei = self._trips[0][0].get('imei')
            for ctner,_trip in enumerate(self._trips):
                if len(_trip) == 0:
                    break
                print('Going to transform trip No. {}'.format(ctner))
                self._extractStartEndLocatio(_trip)._addressTranslator()
                distance = 0
                speed = 0
                speedCount = 0
                for i,record in enumerate(_trip):
                    if (record.get('speed') is not None) and (record.get('zeroD') == 0):
                        speed += record.get('speed')
                        speedCount += 1
                    if i+1 < len(_trip):
                        if (_trip[i].get('latitude') is not None) and (_trip[i].get('longitude')  is not None) and (_trip[i+1].get('latitude') is not None) and (_trip[i+1].get('longitude') is not None):
                            loc1 = (_trip[i].get('latitude'), _trip[i].get('longitude'))
                            loc2 = (_trip[i+1].get('latitude'), _trip[i+1].get('longitude'))
                            distance += hs.haversine(loc1,loc2)

                self._distance = distance
                if speed != 0:
                    self._speedAvg = speed / speedCount
                print('     *       distance: {}'.format(distance))
                if distance is not None and distance > 0.2:
                    self.exporter()

    def exporter(self):

        res = {
        # 'شروع (تاریخ)': JalaliDate(self.__startLocation.get('date')),
        # 'پایان (تاریخ)': JalaliDate(self._endLocation.get('date')),
        'imei': self._imei,
        'شروع (تاریخ)': self._startLocation.get('date'),
        'پایان (تاریخ)': self._endLocation.get('date'),
        'مسافت (KM)': self._distance,
        'میانگین سرعت (KM/H)': self._speedAvg,
        }

        if self._parsiStart.get('subdivisions') is not None and  self._parsiStart.get('subdivisions').get('ostan') is not None:
            startOstan = self._parsiStart.get('subdivisions').get('ostan').get('title')
        else:
            startOstan = 'ادرسی یافت نشد'
        if self._parsiEnd.get('subdivisions') is not None and self._parsiEnd.get('subdivisions').get(
                'ostan') is not None:
            endOstan = self._parsiEnd.get('subdivisions').get('ostan').get('title')
        else:
            endOstan =  'ادرسی یافت نشد'
        startAddress = self._parsiStart.get('address'),
        endAddress = self._parsiEnd.get('address'),

        res['استان مقصد'] = endOstan
        res['استان مبدا']= startOstan
        if startAddress is not None:
            res['مبدا']=startAddress
        else:
            res['مبدا'] = 'ادرسی یافت نشد'
        if endAddress is not None:
            res['مقصد']= endAddress
        else:
            res['مقصد'] = 'ادرسی یافت نشد'

        self._parsiEnd = {},
        self._parsiStart = {}
        self._distance = 0
        self._speedAvg = 0

        self.totalReport.append(res)
        print('     *       report with count: {}'.format(self.totalReport))
