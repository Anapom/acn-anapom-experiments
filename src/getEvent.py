import pytz
import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

from acnportal import acnsim
from acnportal.acnsim.models.ev import EV
from acnportal.acndata import DataClient
from acnportal.acnsim.events.acndata_events import _datetime_to_timestamp
from acnportal.acnsim.models.battery import Battery
from acnportal.acnsim.events.event import PluginEvent
from acnportal.acnsim.events.event_queue import EventQueue
from utility import _pandas_toEvent, getEVENTS_DIR

class EventCreator:
    def __init__(self, site, eventIntervals, API_KEY, periods=5, voltage=208):       
        self.site           = site
        self.EVENTS_DIR     = getEVENTS_DIR(site)
    
        self.eventIntervals = eventIntervals
        self.API_KEY        = API_KEY
        
        self.periods        = periods
        self.voltage        = voltage
    
    def createEvent(self): 
        if self.site in ['jpl', 'caltech', 'office1']:
            self._createDF_ACN()
        else:
            print(f"Please provide useable site i.e., ['jpl', 'caltech', 'office1']")        
    
        
    def _createDF_ACN(self):
        def getDataACN():
            # only appicible for ACN sites, ["jpl", "caltech", "office1"]
            client      = DataClient(self.API_KEY)
            docs        = client.get_sessions_by_time(self.site, timezone.localize(start), timezone.localize(end))
            df          = pd.DataFrame.from_dict(docs)
            return df
        
        def prepTrueValueDF(DF: pd.DataFrame):
            df = DF.copy()
            df['month'] = date[0].month
            df['estimated_departure'] = df['disconnectTime']
            df['estimated_requested_energy'] = df['kWhDelivered']
            df = df.dropna(subset=['userInputs'])
            # save to dfACN dict
            dfACN["TrueValue"][str(month)] = df
                    
        def prepUserInputsDF(DF: pd.DataFrame):
            def get_estimated_time_fromUserInputs(userInputs, connectionTime, disconnectTime):
                if userInputs is not None:
                    return connectionTime + timedelta(minutes= userInputs[0]['minutesAvailable'])
                else:
                    return disconnectTime

            def get_estimated_kWhDelivered_fromUserInputs(userInputs, kWhDelivered):
                if userInputs is not None:
                    return userInputs[0]['kWhRequested']
                else:
                    return kWhDelivered

            def find_session_duration(connectionTime, disconnectTime):
                duration = disconnectTime - connectionTime
                return duration.total_seconds()/3600

            def get_estimated_minute_fromuserInputs(userInputs, connectionTime, disconnectTime):
                if userInputs is not None:
                    return userInputs[0]['minutesAvailable'] / 60
                else:
                    return np.NaN
        
            df = DF.copy()
            df['month'] = date[0].month
            df = df.dropna(subset=['userInputs'])
            
            # extract userInputs
            df['estimated_departure']           = df.apply(lambda x: get_estimated_time_fromUserInputs(x['userInputs'], x['connectionTime'], x['disconnectTime']), axis=1)
            df['estimated_requested_energy']    = df.apply(lambda x: get_estimated_kWhDelivered_fromUserInputs(x['userInputs'], x['kWhDelivered']), axis=1)
            df['estimated_session_duration']    = df.apply(lambda x: get_estimated_minute_fromuserInputs(x['userInputs'], x['connectionTime'], x['disconnectTime']), axis=1)
            df['session_duration']              = df.apply(lambda x: find_session_duration(x['connectionTime'], x['disconnectTime']), axis=1)
            # save to dfACN dict
            dfACN["userInputs"][str(month)] = df
        
     
        timezone    = pytz.timezone('America/Los_Angeles')    
        dfACN = {"TrueValue":{}, "userInputs":{}}
        print("Downloading from ACN website")
        for month, date in self.eventIntervals.items():
            print(f'{month: <9}: {date[0]} -- {date[1]}')
            start, end  = date[0], date[1]
            DF          = getDataACN()
            prepTrueValueDF(DF)
            prepUserInputsDF(DF)

        print("Saving events")
        for demand in dfACN.keys():
            for month, date in time_month.items():
                print(f'{month: <9} : {demand: <12} ')
                name = str(demand) 
                _pandas_toEvent(timezone, self.EVENTS_DIR, dfACN[demand][month], date[0], date[1], self.periods, self.voltage, ideal_battery = False, max_len=None, force_feasible=False, demand_name= name)
           

if __name__ == '__main__':
    time_month = {
        'October':[(datetime(2019,  10, 1)),(datetime(2019, 10, 2))],
        'November':[(datetime(2019, 11, 1)),(datetime(2019, 11, 2))],
        'December':[(datetime(2019, 12, 1)),(datetime(2019, 12, 2))],
    }
    API_KEY = ''
    site    = 'jpl'
    eventCreator = EventCreator(site, time_month, API_KEY)
    eventCreator.createEvent()