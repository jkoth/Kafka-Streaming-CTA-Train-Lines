"""Defines trends calculations for stations"""
import logging
import faust


logger = logging.getLogger(__name__)
kafka_source_topic_name = "cta.trains.monitor.stations"

# Faust will ingest records from Kafka in this format
# Model for Station events from DB
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
# Model for TransformedStation events
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream"
	       , broker="kafka://localhost:9092"
	       , store="memory://")

# Define the input Kafka Topic - Kafka Connect 
kafka_source_topic = app.topic(kafka_source_topic_name
                             , value_type=Station)

# Define the output Kafka Topic - changelog topic for Faust table
transformed_station_topic = app.topic(f"{kafka_source_topic_name}.transformed"
                            , value_type=TransformedStation
                            , partitions=1
                            )

# Define a Faust Table
table_name = f"{kafka_source_topic_name}.transformed"
transformed_station_table = app.Table(table_name
                    , default=TransformedStation
                    , changelog_topic=transformed_station_topic
                    , help="store transformed Stations data in Faust Table"
                    )


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#


if __name__ == "__main__":
    app.main()
