from tagExtractor import ExtractTags
from LocationExtractor import Extractlocation
from LocationTransformer import LocationTransformer
from psqlJointer import PsqlJointer
from Load import Load


def log(text):
    print(text)


def main():
    loader = Load()
    imeiList = [
        "868183038526153",
        "867717033930547",
        "867857034979794",
        "867717034019779",
        "867717033767436",
        "868183038496241",
        "867857034980420",
        "867857034978416",
        "867717033814857",
        "867717033830242",
        "867717033795726",
        "867717033789760",
        "867857034979471",
        "867717033806671",
        "867717033930588",
        "867717034015983",
        "867717033755589",
        "867857034982368",
        "867717034046343",
        "867717033781502",
        "867717034040981",
        "867717033946824",
        "867717033789695",
        "868183038525320",
        "867857034983077",
        "867717039297396",
        "867717034056276",
        "867717033945578",
        "867717033780207",
        "867857034979117",
    ]

    for imei in imeiList:
        e = ExtractTags(imei)
        tags = e.executePipeline().transform()

        # join to psql
        psql = PsqlJointer()
        driverVehicle = psql.joinByImei(imei)

        for tripNumber, tag in enumerate(tags):
            locationExtractor = Extractlocation(tag)
            tripInfo = locationExtractor.executePipeline().transform()

            # for data in tripInfo:
            transform = LocationTransformer(
                imei=imei, tripNumber=tripNumber, locations=tripInfo
            )
            # if data.get("longitude") is not None and data.get("latitude"):

            transformed = (
                transform.startDate()
                .endDate()
                .positionTranslator()
                .distanceCalculator()
                .exporter(
                    driverVehicle[0],
                    driverVehicle[1],
                    driverVehicle[2],
                    driverVehicle[3],
                    driverVehicle[4],
                    driverVehicle[5],
                    driverVehicle[6],
                    driverVehicle[7],
                )
            )

            loader.loadToMongo(transformed)
            print(">>>>>>>>{}>>>>>DONE {}".format(imei, tripNumber))


if __name__ == "__main__":
    main()
