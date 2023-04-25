from datetime import datetime
import requests
import haversine as hs
from jdatetime import datetime as jdatetime


class LocationTransformer:
    imei = ""
    tripNumber = 0
    locations = []

    averageSpeed = 0
    distance = 0

    startDateJalali = ""
    endDateJalali = ""

    startPositionAddress = ""
    endPositionAddress = ""

    _parsiHost = "https://api.parsimap.ir"
    _parsiToken = "p1be1846ea2518472b811debaae1140082141d20fe"

    def __init__(
        self,
        imei,
        tripNumber,
        locations,
    ):
        self.imei = imei
        self.tripNumber = tripNumber
        self.locations = locations

    def startDate(self):
        dt_format = datetime.strptime(
            str(self.locations[0]["date"]), "%Y-%m-%d %H:%M:%S"
        )
        timestamp = dt_format.timestamp()
        self.startDateJalali = jdatetime.fromtimestamp(timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return self

    def endDate(self):
        dt_format = datetime.strptime(
            str(self.locations[len(self.locations) - 1]["date"]), "%Y-%m-%d %H:%M:%S"
        )
        timestamp = dt_format.timestamp()

        self.endDateJalali = jdatetime.fromtimestamp(timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return self

    def positionTranslator(self):
        host = self._parsiHost + "/geocode/reverse"

        _startLocation = self.locations[0]
        _endLocation = self.locations[len(self.locations) - 1]
        self.startPositionAddress = requests.get(
            url=host,
            params={
                "key": self._parsiToken,
                "location": "{},{}".format(
                    _startLocation.get("longitude"), _startLocation.get("latitude")
                ),
                # GeoJson
                "local_address": "false",
                "approx_address": "true",
                "subdivision": "true",
                "plate": "false",
                "request_id": "false",
            },
        ).json()
        self.endPositionAddress = requests.get(
            url=host,
            params={
                "key": self._parsiToken,
                "location": "{},{}".format(
                    _endLocation.get("longitude"), _endLocation.get("latitude")
                ),
                # GeoJson
                "local_address": "false",
                "approx_address": "true",
                "subdivision": "true",
                "plate": "false",
                "request_id": "false",
            },
        ).json()

        return self

    def distanceCalculator(self):
        distance = 0
        speed = 0
        speedCount = 0
        _trip = self.locations
        for i, item in enumerate(_trip):
            if (item.get("speed") is not None) and (item.get("zeroD") == 0):
                speed += item.get("speed")
                speedCount += 1

            if i + 1 < len(_trip):
                if (
                    (_trip[i].get("latitude") is not None)
                    and (_trip[i].get("longitude") is not None)
                    and (_trip[i + 1].get("latitude") is not None)
                    and (_trip[i + 1].get("longitude") is not None)
                ):
                    loc1 = (_trip[i].get("latitude"), _trip[i].get("longitude"))
                    loc2 = (_trip[i + 1].get("latitude"), _trip[i + 1].get("longitude"))
                    distance += hs.haversine(loc1, loc2)
        if speed != 0:
            self.averageSpeed = speed / speedCount
        self.distance = distance

        return self

    def exporter(
        self, firstName, lastName, driverName, plate, phoneNumber, userId, organId, imei
    ):
        result = {
            "tripNumber": self.tripNumber,
            "averageSpeed": self.averageSpeed,
            "distance": self.distance,
            "startDateJalali": self.startDateJalali,
            "endDateJalali": self.endDateJalali,
            "startPositionAddress": self.startPositionAddress,
            "endPositionAddress": self.endPositionAddress,
            # form pg
            "firstName": firstName,
            "lastName": lastName,
            "name": driverName,
            "plate": plate,
            "phoneNumber": phoneNumber,
            "userId": userId,
            "organId": organId,
            "imei": imei,
        }
        return result
