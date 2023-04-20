from extractor import Extract
from tagExtractor import ExtractTags
from LocationExtractor import Extractlocation
from LocationTransformer import LocationTransformer


def log(text):
    print(text)


def main():
    imeiList = [
        "867857034979224",
        "867717034058660",
        "867717033755357",
        "867717033755589",
        "867717033767436",
        "867717033772444",
        "867717033772865",
        "867717033779498",
        "867717033780488",
        "867717033780678",
        "867717033781502",
        "867717033781734",
        "867717033781924",
        "867717033789695",
        "867717033792186",
        "867717033792426",
        "867717033797102",
        "867717033801698",
        "867717033802308",
        "867717033809519",
        "867717033814857",
        "867717033815060",
        "867717033817595",
        "867717033825978",
        "867717033826026",
        "867717033834491",
        "867717033892994",
        "867717033920852",
        "867717033930547",
        "867717033930588",
        "867717033930802",
        "867717033932535",
        "867717033944936",
        "867717033945305",
        "867717033945339",
        "867717033946535",
        "867717033946824",
        "867717033952780",
        "867717033962532",
        "867717033962714",
        "867717033974123",
        "867717033975369",
        "867717033987794",
        "867717033989634",
        "867717033995490",
        "867717034000662",
        "867717034000761",
        "867717034001884",
        "867717034002437",
        "867717034002692",
        "867717034007253",
        "867717034013137",
        "867717034014853",
        "867717034015983",
        "867717034019779",
        "867717034019951",
        "867717034022765",
        "867717034044306",
        "867717034046343",
        "867717034046368",
        "867717034051129",
        "867717034051137",
        "867857034978416",
        "867717034058785",
        "867717034067356",
        "867717034099110",
        "867717034111105",
        "867717034112053",
        "867717034118191",
        "867717034119249",
        "867717034120353",
        "867717034120379",
        "867717034121104",
        "867717039268561",
        "867717039268587",
        "867717039268611",
        "867717039268637",
        "867717039268678",
        "867717039270302",
        "867717039270310",
        "867717039283396",
        "867717039283560",
        "867717039297396",
        "867717039297404",
        "867717039297487",
        "867717039297958",
        "867717039327169",
        "867717039327201",
        "867717039329371",
        "867717039335501",
        "867717039335584",
        "867717039335709",
        "867717039335733",
        "867717039335782",
        "867717039336046",
        "867717039336558",
        "867717039336624",
        "867717039336905",
        "867378033979589",
        "867190038938362",
    ]

    for imei in imeiList:
        # extract = Extract(imei)
        #
        # log("Executing the aggregation query...")
        # extract.executePipeline()
        # log("Executing the aggregation query => DONE")
        # extract.tripBound()
        # log("Executing the trip aggregation query...")
        # trips = extract.extractTrips()
        # log("Executing the trip aggregation query => DONE")
        # # => transform
        # log("Transforming...")
        # transform = Transform(trips)
        # transform.trasformation()
        # res = transform.totalReport
        # log("Transformation => DONE")
        # # => Loading
        # log("Loading...")
        # load.exportToExcel(res)
        # log("Loading => DONE")
        #!!!!!!!!!!!!!!!!!!!!!!!!!!1
        e = ExtractTags(imei)
        tags = e.executePipeline().transform()
        for tripNumber, tag in enumerate(tags):
            locationExtractor = Extractlocation(tag)
            tripInfo = locationExtractor.executePipeline().transform()

            for data in tripInfo:
                transform = LocationTransformer(
                    imei=imei, tripNumber=tripNumber, locations=data
                )
                if data.get("longitude") is not None and data.get("latitude"):
                    transform.startDate().endDate().positionTranslator()


if __name__ == "__main__":
    main()
