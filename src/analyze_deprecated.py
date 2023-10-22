import os
import json
from acnportal import acnsim
import pandas as pd

from config import *

def get_metric(results_dir, config):
    path = os.path.join(results_dir, f"{config['start']}_{config['end']}", config['tariff'], str(config['revenue']), config['scenario'], config['alg']
    , 'metrics.json')   

    if not os.path.exists(path):
        print(path)
        return {}
    with open(path) as f:
        metrics = json.load(f)
    return metrics

def get_solve_stats(results_dir, config):
    path = os.path.join(results_dir, f"{config['start']}_{config['end']}", config['tariff'], str(config['revenue']), config['scenario'], config['alg']
    , 'solve_stats.json')

    if not os.path.exists(path):
        return float('nan')
    with open(path) as f:
        return json.load(f)

def get_sim(results_dir, config):
    path = os.path.join(results_dir, f"{config['start']}_{config['end']}", config['tariff'], str(config['revenue']), config['scenario'], config['alg']
    , 'sim.json')

    if not os.path.exists(path):
        return None
    with open(path) as f:
        try:
            return acnsim.Simulator.from_json(f)
        except:
            print(path)
            
def getDFResult(scenarios, time_month, algs):
    data = []
    col_show = ['alg','proportion_delivered','demands_fully_met','peak_current','total_energy_delivered','total_energy_requested','revenue','demand_charge','energy_cost','profit']
    for month,day in time_month.items():
        startDate = day[0]
        endDate = day[1]
        for scenario in scenarios.keys():
            for row, alg in enumerate(algs):    
                config = {'scenario': scenario, 'start': startDate.date(), 'end': endDate.date(),
                        'alg': alg, 'tariff': tariff_name, 'revenue': revenue}
                metrics = get_metric(f'{RESULT_DIR_PROFIT}', config)
                metrics['scenario'] = scenario
                if alg == 'ASA-PM-Hint':
                    alg = 'ASA-PM w/ Hint'
                metrics['alg'] = alg
                metrics['month'] = month
                data.append(metrics)

                df = pd.DataFrame(data)
                df['revenue'] = df['proportion_delivered'] / 100 * df['total_energy_requested'] * revenue
                df['total_cost'] = df['demand_charge'] + df['energy_cost']
                df['profit'] = df['revenue'] - df['total_cost']
                df.set_index('month',inplace=True)
    return df[col_show]


def getSimsResult(scenario_order, time_month, algs):
    sims = dict()
    for month,day in time_month.items():
        startDate = day[0]
        endDate = day[1]
        for scenario in scenario_order:
            for row, alg in enumerate(algs):    
                config = {'scenario': scenario, 'start': startDate.date(), 'end': endDate.date(),
                        'alg': alg, 'tariff': tariff_name, 'revenue': revenue}
                sim_get = get_sim(f'{RESULT_DIR_PROFIT}', config)
                sims[str(month)+ '_' + str(alg) + '_' + str(scenario)] = sim_get
    return sims

