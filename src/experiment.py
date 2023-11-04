from datetime import datetime
import pytz
from copy import deepcopy
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import matplotlib
import numpy as np
import pandas as pd
import seaborn as sns
import json
import os

from acnportal import acnsim, algorithms
from acnportal.acnsim import analysis
from acnportal.signals.tariffs import TimeOfUseTariff
from utility import _pandas_toEvent, getEVENTS_DIR, getRESULT_DIR
from adacharge import *

class Experiment:
    """ Wrapper for ACN-Sim Experiments including caching serialized experiment to disk. """
    def __init__(self, site, eventIntervals, scenarios, periods=5, voltage=208):
        self.site           = site
        if site in ['jpl', 'caltech', 'office1']:
            self.timezone   = pytz.timezone('America/Los_Angeles')
            
        self.EVENTS_DIR     = getEVENTS_DIR(site)
        self.RESULTS_DIR    = getRESULT_DIR(site)
        
        self.eventIntervals = eventIntervals
        self.scenarios      = scenarios
        
        self.periods        = periods
        self.voltage        = voltage

    def configure_sim(
        self,
        alg, 
        start, 
        events, 
        basic_evse=True,
        estimate_max_rate=False, 
        uninterrupted_charging=False,
        quantized=False,
        allow_overcharging=False,
        tariff_name=None,
        offline=False,
    ):
        """ Configure simulation. """
        start_time = start
            
        if estimate_max_rate:
            alg.max_rate_estimator = algorithms.SimpleRampdown()
            alg.estimate_max_rate = True

        alg.uninterrupted_charging = uninterrupted_charging
        alg.allow_overcharging = allow_overcharging

        # Some algorithms support a quantized option (discreet set of pilot signal)
        if quantized:
            try:
                alg.quantize = True
            except:
                pass
            try:
                alg.reallocate = True
            except:
                pass
        if self.site == "jpl":
            cn = acnsim.sites.jpl_acn(voltage=self.voltage, basic_evse=basic_evse) 
        elif self.site == "caltech":
            cn = acnsim.sites.caltech_acn(voltage=self.voltage, basic_evse=basic_evse) 
        else:
            pass
            
        if tariff_name is not None:
            signals = {'tariff': TimeOfUseTariff(tariff_name)}
        else:
            signals = {}
            
        sim = acnsim.Simulator(cn, alg, events, start_time, signals=signals,
                            period=self.periods, verbose=False, store_schedule_history=False)
        
        if offline:
            alg.register_events(events)
            alg.solve()
        
        return sim

    def run(self, algs, tariff_name, revenue):
        def _calc_metrics():
            """ Calculate metrics from simulation. """
            metrics = {
                'proportion_delivered': analysis.proportion_of_energy_delivered(sim) * 100,
                'demands_fully_met': analysis.proportion_of_demands_met(sim) * 100,
                'peak_current': sim.peak,
                'demand_charge': analysis.demand_charge(sim),
                'energy_cost': analysis.energy_cost(sim),
                'total_energy_delivered': analysis.total_energy_delivered(sim),
                'total_energy_requested': analysis.total_energy_requested(sim)
            }
            return metrics

        def _log_local_file(path):
            """ Write simulation, metrics and solver statistics to disk. """
            print("start Logging")
            with open(path + f'/sim.json', 'w') as f:
                sim.to_json(f)
            with open(path + f'/metrics.json', 'w') as outfile:
                json.dump(_calc_metrics(), outfile)
            with open(path + f'/solve_stats.json', 'w') as outfile:
                json.dump(sim.scheduler.solve_stats, outfile)

        def _run_and_store(path):
            """ Run experiment and store results. """
            # print(f'Starting - {path}')
            if os.path.exists(path + f'/sim.json'):
                print(f'Already Run - {path}...')
                return
            try:
                sim.run()
                if not os.path.exists(path):
                    os.makedirs(path)
                _log_local_file(path)
                print(f'Done - {path}')
            except Exception as e:
                print(f'Failed - {path}')
                print(e)
        
        simStartTime = datetime.now()
        print(f"--------------------- Start simulation at {simStartTime} ---------------------")
        for month, date in self.eventIntervals.items():
            print(f'{month: <9}: {date[0]} -- {date[1]}')
            start, end  = date[0], date[1]
            
            for demand, scenario in scenarios.items():                
                for algName, alg in algs.items(): 
                    
                    outputFile_path = self.RESULTS_DIR.joinpath(algName, f"{start.date()} {end.date()}",tariff_name, str(revenue), demand)
                    outputFile_path.mkdir(parents=True, exist_ok=True)
                    outputFile_path = str(outputFile_path)
                    
                    eventName = str(demand)
                    events    = _pandas_toEvent(self.timezone, self.EVENTS_DIR, "_", start, end, self.periods, self.voltage, ideal_battery = False, max_len=None, force_feasible=False, demand_name=eventName)

                    sim = self.configure_sim(
                        alg                 = deepcopy(alg),
                        start               = start,
                        events              = events,
                        basic_evse          = scenario['basic_evse'],
                        estimate_max_rate   = scenario['estimate_max_rate'],
                        uninterrupted_charging  = scenario['uninterrupted_charging'],
                        quantized           = scenario['quantized'],
                        tariff_name         = tariff_name,
                        offline             = scenario['offline']   
                    )
                    _run_and_store(outputFile_path)
                    simEndTime = datetime.now()
                    print(f"---------------------End simulation at {simEndTime} ---------------------")
                
if __name__ == "__main__":
    
    # event interval to simulate
    time_month = {
        'October' :[(datetime(2019,  10, 1)),(datetime(2019, 10, 2))],
        # 'November':[(datetime(2019, 11, 1)),(datetime(2019, 11, 2))],
        # 'December':[(datetime(2019, 12, 1)),(datetime(2019, 12, 2))],
    }
    
    # demand and scenarios
    scenarios = {
                'TrueValue': {
                    'estimate_max_rate'     : False,
                    'uninterrupted_charging': False,
                    'quantized'             : False,
                    'basic_evse'            : True,
                    'offline'               : False
                }, 
                # 'userInputs': {
                #     'estimate_max_rate'     : True,
                #     'uninterrupted_charging': False,
                #     'quantized'             : False,
                #     'basic_evse'            : True,
                #     'offline'               : False
                # }        
    }
    
    tariff_name = 'sce_tou_ev_4_march_2019'
    revenue     = 0.3
    site        = 'jpl'
    
    peakCurrent      = 500 #Ampere
    peakKw           = peakCurrent * 208 / 1000 # peakCurrent * voltage(default 208) / 1000 (W)
    
    # Algorithms
    def days_remaining_scale_demand_charge(rates, infrastructure, interface, baseline_peak=0, **kwargs):
        """ Demand Charge Proxy which divideds the demand charge over the remaining days in the billing period. """
        day_index = interface.current_time // ((60 / interface.period) * 24)
        days_in_month = 30 #monthrange(year, month)[1]
        day_index = min(day_index, days_in_month - 1)
        scale = 1 / (days_in_month - day_index)
        dc = demand_charge(rates, infrastructure, interface, baseline_peak,
                        **kwargs)
        return scale * dc

    ALGS = dict()
    # Profit = [
    #     ObjectiveComponent(total_energy, revenue),
    #     ObjectiveComponent(tou_energy_cost),
    #     ObjectiveComponent(days_remaining_scale_demand_charge, 1, {'baseline_peak': peakKw}),
    #     ObjectiveComponent(quick_charge, 1e-3),
    #     ObjectiveComponent(equal_share, 1e-12),
    # ]
    
    Quick_charge = [
        adacharge.ObjectiveComponent(adacharge.quick_charge),
        adacharge.ObjectiveComponent(adacharge.equal_share, 1e-12)
    ]
    
    ALGS['Quick_charge'] =  AdaptiveSchedulingAlgorithm(Quick_charge, solver='ECOS', max_recompute=1)
     
    ex = Experiment(site=site, eventIntervals=time_month, scenarios=scenarios)
    ex.run(algs=ALGS, tariff_name=tariff_name, revenue=revenue)