import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

# Bookings
@dlt.table(name="stage_bookings")
def stage_bookings():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/") \
        .drop("_rescued_data")
    return df

@dlt.table(name="trans_bookings")
def trans_bookings():
    df = dlt.readStream("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("modifiedDate", current_timestamp()) \
        .withColumn("bookingdate", to_date(col("booking_date")))
    return df

@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    return dlt.readStream("trans_bookings")

####################################################################################################
# Flights
@dlt.view(name="trans_flights")
def trans_flights():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    return df.drop("_rescued_data") \
             .withColumn("modifiedDate", current_timestamp())

dlt.create_streaming_table(name="silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

####################################################################################################
# Passengers
@dlt.view(name="trans_passengers")
def trans_passengers():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    return df.drop("_rescued_data") \
             .withColumn("modifiedDate", current_timestamp())

dlt.create_streaming_table(name="silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

####################################################################################################
# Airports
@dlt.view(name="trans_airports")
def trans_airports():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    return df.drop("_rescued_data") \
             .withColumn("modifiedDate", current_timestamp())

dlt.create_streaming_table(name="silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

####################################################################################################
# Business View
@dlt.table(name="silver_business")
def silver_business():
    bookings = dlt.readStream("silver_bookings")
    flights = dlt.read("silver_flights")
    passengers = dlt.read("silver_passengers")
    airports = dlt.read("silver_airports")

    return bookings \
        .join(flights, "flight_id", "left") \
        .join(passengers, "passenger_id", "left") \
        .join(airports, "airport_id", "left") \
        .drop("modifiedDate")