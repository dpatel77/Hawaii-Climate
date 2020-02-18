# %matplotlib inline
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt
from sqlalchemy import func, and_
from scipy import stats

import numpy as np
import pandas as pd
from flask import Flask, jsonify

import datetime as dt
import calendar
from datetime import datetime, timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func



#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
conn = engine.connect()
# metadata = engine.MetaData()

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# We can view all of the classes that automap found
Base.classes.keys()

# Save references to each table
#Map measurement class
Measurement = Base.classes.measurement
#Map station class
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


measurement = pd.read_sql("SELECT * FROM Measurement", conn)
measurement.head()
measurement['date'] =  pd.to_datetime(measurement['date'])
# measurement['station']=measurement['station'].astype('str')
measurement.head()

station = pd.read_sql("SELECT * FROM Station", conn)
station.head()

# Exploratory Climate Analysis

# Design a query to retrieve the last 12 months of precipitation data and plot the results

# Calculate the date 1 year ago from the last data point in the database
maxdate=measurement.sort_values(by=['date'],ascending=False).head(1)
maxdate=maxdate.loc[:,['date']].reset_index(drop=True)
maxdate

maxdateminusone = maxdate.apply(lambda x: x - pd.DateOffset(years=1))
maxdateminusone
maxdateminusone_time=maxdateminusone.iloc[0]['date']
maxdateminusone_time


# use the maxdate minus a year in this
last12months=measurement[measurement['date']>=(maxdateminusone_time)]
last12months.count()

# Perform a query to retrieve the data and precipitation scores
retrievedata=last12months[['date','prcp','tobs']]
# Save the query results as a Pandas DataFrame and set the index to the date column
retrievedata=pd.DataFrame(retrievedata)
retrievedata.rename(columns={'prcp':'Precipitation'},inplace=True)

# Save the query results as a Pandas DataFrame and set the index to the date column
sorteddate=retrievedata.sort_values('date',ascending=True)
sorteddate['Precipitation']=sorteddate['Precipitation'].fillna(0)

sorteddate.head()

ax = sorteddate.plot.bar(x='date', y='Precipitation')

# Gray shades can be given as a string encoding a float in the 0-1 range, e.g.:
# color = '0.75'
plt.grid(which='major',axis='both',color='0.25', linestyle='-', linewidth=2)
ax

# Use Pandas Plotting with Matplotlib to plot the data


summ_stats=pd.DataFrame(retrievedata['Precipitation'].describe())
summ_stats

# Use Pandas to calcualte the summary statistics for the precipitation data


# Design a query to show how many stations are available in this dataset?
measurement['station'].nunique()

# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
ncount=measurement.groupby(['station']).nunique().sort_values('id',ascending=False)
mostactive=ncount[['id']]
mostactive.head()
mostactive=mostactive.index[0]
y = np.array(mostactive, dtype=object)
mostactive

allmostactive=measurement[measurement['station']==mostactive]

allmostactive=pd.DataFrame(allmostactive)



# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature of the most active station?
tobs_min=allmostactive['tobs'].min()
tobs_max=allmostactive['tobs'].max()
tobs_avg=allmostactive['tobs'].mean()
print("Min: ",tobs_min,", Max: ",tobs_max,", Average: ",tobs_avg)



# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram

#this gives you the max count
measurement['station'].value_counts().max()
# this gives you the id with the max count
maxid=measurement['station'].value_counts().idxmax()

maxid_station=measurement[measurement['station']==maxid]
maxid_sta_12mon=maxid_station[maxid_station['date']>=maxdateminusone_time]
maxid_sta_12mon
histo=plt.hist(maxid_sta_12mon['tobs'],bins=12, align=('mid'),color=['steelblue'],label=['tobs'])
plt.title('')
plt.xlabel('')
plt.ylabel('Frequency')
plt.legend()



#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all routes that are available."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start_end"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
      # Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
      # Return the JSON representation of your dictionary.
    session = Session(engine)
    results = session.query(Measurement.date, Measurement.prcp).all()
    session.close()
    all_precipitation = []
    for date, prcp in results:
        precipitation_dict = {}
        precipitation_dict["date"] = date
        precipitation_dict["prcp"] = prcp
        all_precipitation.append(precipitation_dict)

    return jsonify(all_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    # Return a JSON list of stations from the dataset.
    station = session.query(Station.station).all()
    return jsonify(station)

@app.route("/api/v1.0/tobs")
def tobs():
    # query for the dates and temperature observations from a year from the last data point.
    # Return a JSON list of Temperature Observations (tobs) for the previous year.
    session = Session(engine)
    date_tobs = session.query(Measurement.date,Measurement.tobs).\
        filter(Measurement.date>='2016-08-23').group_by(Measurement.date).all()
    session.close()
    all_tobs = []
    for date, tobs in date_tobs:
        tobs_dict = {}
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        all_tobs.append(tobs_dict)
    return jsonify(all_tobs)

@app.route("/api/v1.0/start")
def start():
  # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
  # When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date.
    session = Session(engine)
    start='2017-01-01'
    inbetween_min = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
        filter(Measurement.date>=start)
    session.close()
    start_end = []
    for min,avg,max in inbetween_min:
        tobs_dict = {}
        tobs_dict["min"] = min
        tobs_dict["avg"] = avg
        tobs_dict["max"] = max
        start_end.append(tobs_dict)
    return jsonify(start_end)


@app.route("/api/v1.0/start_end")
def start_end():
     # When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive.
    session = Session(engine)
    start='2017-01-01'
    end='2017-01-15'
    inbetween_min = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
        filter(Measurement.date>=start).\
        filter(Measurement.date<=end)
    session.close()
    start_end = []
    for min,avg,max in inbetween_min:
        tobs_dict = {}
        tobs_dict["min"] = min
        tobs_dict["avg"] = avg
        tobs_dict["max"] = max
        start_end.append(tobs_dict)
    return jsonify(start_end)

    

if __name__ == '__main__':
    app.run(debug=True)