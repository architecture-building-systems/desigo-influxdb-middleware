
# API Data Collection Script for 3for2 Project
# METER DATA COLLECTION FOR THE 'BMS' DATABASE
# Developed by Clayton Miller -- miller.clayton@gmail.com
# Date implemented: Jan 2015
# THIS SCRIPT PROVIDES DATA FOR HORSEBROKEDOWN dashboard

# This scipt includes the work-in-progress code to query from the Siemens API and push to the influxdb node. 
# Eventually this code will be worked into a self-contained script or library that will run on a cloud-based or 
# UWC network-based instance and be maintained as the middleware in the 3for2 Data collection process

# The libraries necessary to use this script -- several are related to web-services

from __future__ import division
import requests
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import re
import math

import json
import timeit

import datetime
import time
from urllib2 import Request, urlopen, URLError
import csv
import sys
import pytz
import pycurl
import certifi



requests.packages.urllib3.disable_warnings()

meta = pd.read_csv("mapping-point-list.csv", index_col="Unnamed: 0")
apilink = #The REST API link for the Desigo CC goes here
tz = pytz.timezone('Asia/Singapore')
timeoutsecs = 60
tokenurl = #token url goes here

def authenticate(timeoutsecs):
    payload = #'grant_type=password&username=login_goes_here&password=password_goes_here'
    token_response = requests.post(tokenurl, data=payload, verify=False, timeout = timeoutsecs)
    authtoken = token_response.text.split('"')[3]
    authtoken_input = str('Bearer '+authtoken)
    head = {'Authorization': authtoken_input, 'Content-Type': 'application/json'}
    return head

#Autentication process
head = authenticate(timeoutsecs)

# Set the parameters for the Influxdb
host = #influxDB url here
port = #port number here
login = #influx login here
pw = #influxpw here


# Seimens get data function
def get_seimensdata(meta, apilink, timeoutsecs):

    #Authenticate and get a token
    head = authenticate(timeoutsecs)
    payload = json.dumps(meta[(pd.notnull(meta.index))].index.tolist())
    #payload = json.dumps(meta["CollectorObjectOrPropertyId"].tolist())
    
    #Grab the instantaneous data
    #trenddata = requests.post(apilink+'values', data=payload, headers=head, verify=('uwc_bms.cer'), timeout = timeoutsecs)
    trenddata = requests.post(apilink+'values', data=payload, headers=head, verify=False, timeout = timeoutsecs)

    #Convert and flatten the json data
    raw_data = json.loads(trenddata.content)
    raw_data_flat = json_normalize(raw_data)
    
    raw_data_flat_named = raw_data_flat.join(meta["Name"], on="OriginalObjectOrPropertyId" )
    raw_data_flat_named = raw_data_flat_named.convert_objects(convert_numeric=True)
    
    raw_data_flat_named_pivoted = pd.pivot_table(raw_data_flat_named, values="Value.Value", columns="Name", index="Value.Timestamp")
    raw_data_flat_named_pivoted.index = pd.to_datetime(raw_data_flat_named_pivoted.index)
    raw_data_flat_named_pivoted = raw_data_flat_named_pivoted.tz_localize('utc').tz_convert('Asia/Singapore')
    
    raw_data_flat_named_pivoted_resampled = raw_data_flat_named_pivoted.resample('min')
    raw_data_flat_named_pivoted_resampled.index.name = 'timestamp'
        
    return raw_data_flat_named_pivoted_resampled



# Function for assembling the string to send to influx
def appendstring(tagname, pointname, meta):
    if meta[meta.Name == pointname][tagname].isnull()[0]:
        return ""
    else:
        return ","+str(tagname)+"="+str(meta[meta.Name == pointname][tagname][0])

# Preprocessing function for InfluxDB loading
def getstring(added_line, meta):
    #Get the nanosecond timestamp
    timestamp = added_line.index.tz_convert('utc').to_pydatetime()
    nan_ts = int((timestamp[0]-datetime.datetime(1970,1,1, tzinfo=pytz.utc)).total_seconds()*1000000000)

    #Create input string for influxdb
    inputstring = ""
    pointnamelist = list(added_line.columns)
    for pointname in pointnamelist:
        if math.isnan(added_line[pointname][0]):
            continue
        else:
            try:
                friendlypointname = re.sub(r'\W+','_',pointname)
                linestring = "bmspoint,name="+str(friendlypointname)+",Area="+str(meta[meta.Name == pointname].Area[0])                +",Type="+str(meta[meta.Name == pointname].Type[0])                +",PointGroup="+str(meta[meta.Name == pointname].PointGroup[0])                +appendstring("Equip", pointname, meta)                +appendstring("SubEquip1", pointname, meta)                +appendstring("SubEquip2", pointname, meta)                +",PointType="+str(meta[meta.Name == pointname].PointType[0])                +" value="+str(float(added_line[pointname][0]))                +" "+str(nan_ts)+"\n"
                inputstring += linestring
            except:
                e = sys.exc_info()[1]
                print 'Error when creating an input line:', e
                continue
                
    return inputstring


# Send to influxDB function
def sendtoinflux(host, port, login, pw, inputstring):
    apiURL = 'https://'+host+':'+port+'/write?u='+login+'&p='+pw+'&db=siemenstest'
    c = pycurl.Curl()
    c.setopt(c.URL, apiURL)
    poststring = inputstring 
    c.setopt(c.POSTFIELDS, poststring)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.perform()

# Perform virtual meter calculations before sending
def addvirtualmeters(added_line):
    try:
        added_line["ChillerPlant_KWperTon_Calculated"] = added_line["B3'Flr7'C'ReC20'Efcy'Pwr"] / added_line["B3'Flr7'C'ReC20'H'FullLd19"] 
    except:
        added_line["ChillerPlant_KWperTon_Calculated"] = 0
    try:
        added_line["TotPrimAHUPwr"] =  added_line["B10'Area3for2'Mtr'CGrpAHU'DOAS1'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'DOAS2'Pwr"]  + added_line["B10'Area3for2'Mtr'CGrpAHU'DOAS3'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'DOAS4'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'FCU1'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'FCU2'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'FCU3'Pwr"] + added_line["B10'Area3for2'Mtr'CGrpAHU'FCU4'Pwr"]
        added_line["PCBPumpElecPwrEst"] = 0
        added_line["TotElecAHUPwr"] =  (added_line["B10'Area3for2'E'MtrEl9'PwrActv"] + added_line["B10'Area3for2'E'MtrEl10'PwrActv"] + added_line["B10'Area3for2'E'MtrEl11'PwrActv"]  + added_line["B10'Area3for2'E'MtrEl12'PwrActv"] + added_line["B10'Area3for2'E'MtrEl14'PwrActv"] + added_line["B10'Area3for2'E'MtrEl15'PwrActv"] + added_line["B10'Area3for2'E'MtrEl16'PwrActv"] + added_line["B10'Area3for2'E'MtrEl17'PwrActv"])*0.001
        added_line["ChillerPlant_COP"] = 3.5169 / added_line["ChillerPlant_KWperTon_Calculated"]
        added_line["TotElecChillerEst"] = (added_line["TotPrimAHUPwr"] + added_line["B10'Area3for2'Mtr'CGrpPCB'TotPrim'Fl"]) / added_line["ChillerPlant_COP"]
        added_line["TotElecLighting"] = np.abs(added_line["B10'Area3for2'E'MtrEl18'PwrActv"]/1000)
        added_line["TotElecPlugs"] =  (np.abs(added_line["B10'Area3for2'E'MtrEl1'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl2'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl3'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl4'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl5'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl6'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl7'PwrActv"]) + np.abs(added_line["B10'Area3for2'E'MtrEl8'PwrActv"]))*0.001
        added_line["TotalElectricalUse"] = added_line["TotElecChillerEst"] + added_line["TotElecAHUPwr"] + added_line["TotElecLighting"] + added_line["TotElecPlugs"] + added_line["PCBPumpElecPwrEst"]
    except:
        print "virtual meter creation error"
        return added_line
    return added_line


# Loop for collecting and sending data
callAPI = True
callfreq = 120
timedelta = 0
#filename = datetime.datetime.now(tz).strftime('%Y_%m_%d_%H_%M_%S')
#filename_log = filename+"_log.csv"
#firstline = get_seimensdata(meta, apilink)
#firstline.to_csv(filename+".csv")#, date_format="%Y-%m-%d %H:%M:%S"

#with open(filename_log, "a") as logfile:
while callAPI:
    if timedelta > 0:
        time.sleep(callfreq-timedelta)

    start_time = timeit.default_timer()
    #rawdata = pd.read_csv(filename+".csv", index_col='timestamp', parse_dates=True)
    #Get the data from the Siemens API
    try:
        added_line = get_seimensdata(meta, apilink, timeoutsecs)
        added_line = addvirtualmeters(added_line)
        #rawdata = rawdata.append(added_line)
        #rawdata.to_csv(filename+".csv")#, date_format="%Y-%m-%d %H:%M:%S"
    except:
        e = sys.exc_info()[1]
        logstring = 'Error when calling Siemens API:'+str(e)
        #logfile.write(logstring + '\n')
        timedelta = timeit.default_timer() - start_time
        print logstring
        continue

    #Send to Influxdb
    try:
        inputstring = getstring(added_line, meta)
        sendtoinflux(host, port, login, pw, inputstring)
    except:
        e = sys.exc_info()[1]
        logstring = 'Error when sending data to Influxdb:'+str(e)
        timedelta = timeit.default_timer() - start_time
        #logfile.write(logstring + '\n')
        print logstring
        continue   

    numberofcols = len(added_line.T.dropna())    
    timedelta = timeit.default_timer() - start_time
    #logstring = "Retrieved "+str(numberofcols)+" pts in "+str(timedelta)+" sec from query of "+str(len(meta))+" pts at "+str(datetime.datetime.now(tz))
    #logfile.write(logstring + '\n')
    #print logstring

