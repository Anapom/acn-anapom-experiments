import os
from pathlib import Path
from datetime import datetime, timedelta
import json

from acnportal import acnsim
from acnportal.acnsim.models.ev import EV
from acnportal.acnsim.events.acndata_events import _datetime_to_timestamp
from acnportal.acnsim.models.battery import Battery
from acnportal.acnsim.events.event import PluginEvent
from acnportal.acnsim.events.event_queue import EventQueue


def getEVENTS_DIR(site:str):
    curDir          = Path.cwd()
    EVENTS_DIR      = curDir.joinpath(curDir,"Output","Events",site)
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    
    return EVENTS_DIR

def getRESULT_DIR(site:str):
    curDir          = Path.cwd()
    EVENTS_DIR      = curDir.joinpath(curDir,"Output","Results",site)
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    
    return EVENTS_DIR

def _pandas_toEvent(timezone, EVENTS_DIR, df, start, end, period, voltage, ideal_battery = False, max_len=None, force_feasible=False, demand_name=""):
    def _convert_to_ev_with_estimated(
        d,
        offset,
        period,
        voltage,
        max_battery_power,
        max_len=None,
        battery_params=None,
        force_feasible=False,
    ):
        """ Convert a json document for a single charging session from acndata into an EV object.

        Args:
            d (dict): Session expressed as a dictionary. See acndata API for more details.
            offset (int): Simulation timestamp of the beginning of the simulation.
            See get_evs() for additional args.

        Returns:
            EV: EV object with data from the acndata session doc.
        """
        arrival   = _datetime_to_timestamp(d["connectionTime"], period) - offset
        departure = _datetime_to_timestamp(d["disconnectTime"], period) - offset

        if max_len is not None and departure - arrival > max_len:
            departure = arrival + max_len

        # requested_energy = d['kWhDelivered'] * 1000 * (60 / period) / voltage  # A*periods

        if force_feasible:
            delivered_energy = min(
                d["kWhDelivered"], max_battery_power * (departure - arrival) * (period / 60)
            )
        else:
            delivered_energy = d["kWhDelivered"]

        session_id = d["sessionID"]
        station_id = d["spaceID"]

        if battery_params is None:
            battery_params = {"type": Battery}
        batt_kwargs = battery_params["kwargs"] if "kwargs" in battery_params else {}
        if "capacity_fn" in battery_params:
            try:
                cap, init = battery_params["capacity_fn"](
                    delivered_energy, departure - arrival, voltage, period
                )
            except:     
                print('cant find cap init change battery to ideal')
                battery_params = {"type": Battery}
                cap = delivered_energy
                init = 0
        else:
            cap = delivered_energy
            init = 0
        batt = battery_params["type"](cap, init, max_battery_power, **batt_kwargs)

        estimated_departure = _datetime_to_timestamp(d["estimated_departure"], period) - offset
        estimated_requested_energy = d['estimated_requested_energy']

        # delivered_energy_amp_periods = delivered_energy * 1000 * (60 / period) / voltage
        return EV(arrival, departure, delivered_energy, station_id, session_id, batt, estimated_departure=estimated_departure, estimated_requested_energy=estimated_requested_energy)

    """ Gather Events from ACN-Data with a local cache."""
    event_name = f'{start.date()}_{end.date()}_{ideal_battery}_{force_feasible}_{max_len}_{demand_name}'
    path = os.path.join(EVENTS_DIR, event_name + '.json')
    print(path)
    if os.path.exists(path):
        print('File found in cache : Downloading...Event')
        with open(path, 'r') as f:
            return acnsim.EventQueue.from_json(f)

    """ Create event from df """
    offset = acnsim.events.acndata_events._datetime_to_timestamp(start, period)

    columns_backtoEV = ['_id', 'userInputs', 'userID', 'sessionID', 'stationID', 'spaceID', 'siteID',
                        'clusterID', 'connectionTime', 'disconnectTime', 'kWhDelivered',
                        'doneChargingTime', 'timezone','estimated_departure','estimated_requested_energy']
    
    docs = json.loads(df[columns_backtoEV].to_json(orient='records'))
    for rec in docs:
        rec['connectionTime'] = datetime.fromtimestamp(rec['connectionTime']/1000, tz=timezone)
        rec['disconnectTime'] = datetime.fromtimestamp(rec['disconnectTime']/1000, tz=timezone)
        rec['estimated_departure'] = datetime.fromtimestamp(rec['estimated_departure']/1000, tz=timezone)

    default_battery_power = 6.656
    if ideal_battery:
        battery_params=None
    else:
        battery_params={'type': acnsim.Linear2StageBattery,
                        'capacity_fn': acnsim.models.battery.batt_cap_fn}

    evs = []
    for d in docs:
        evs.append(
            _convert_to_ev_with_estimated(
                d,
                offset,
                period,
                voltage,
                default_battery_power,
                max_len = max_len,
                battery_params = battery_params,
                force_feasible = force_feasible
            )
        )

    events = [PluginEvent(sess.arrival, sess) for sess in evs]
    eventQueue = EventQueue(events)

    # save to cache
    if not os.path.exists(EVENTS_DIR):
        os.mkdir(EVENTS_DIR)
    with open(path, 'w') as f:
        eventQueue.to_json(f)
        print('Saving eventQueue to cache')

    return eventQueue 