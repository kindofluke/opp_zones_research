#%%
import geopandas as gpd
import pandas as pd
#%%
def statewide_median_incomes():
    """
    Reads in census file of the 50 states and creates a dictionary
    of the median incomes of each of those states
    """
    column_names = {
        'HC01_VC03':'population_16_and_over',
        'HC01_VC85': 'median_income',
        "GEO.id2":"state_geoid",
        "GEO.id":'geoid',
        "HC03_VC07":"pct_unemployment",
        "HC03_VC161": "pct_poverty"
    }
    state_frame = pd.read_csv("data/states/ACS_17_5YR_DP03_with_ann.csv"
        ).rename(columns=column_names).set_index('geoid')
    state_frame = state_frame[state_frame.index.str.startswith("0400000US")]
    return state_frame[['population_16_and_over','median_income','pct_unemployment','pct_poverty']] #return only the actual states


#%%
import pandas as pd
column_names = {
    'HC01_VC03':'population_16_and_over',
    'HC01_VC85': 'median_income',
    "GEO.id2":"geoid2",
    "GEO.id":'geoid',
    "HC03_VC07":"pct_unemployment",
    "HC03_VC161": "pct_poverty"
}
tract_frame = pd.read_csv("data/tracts/ACS_17_5YR_DP03_with_ann.csv").rename(columns=column_names)
tract_frame = tract_frame[['geoid','median_income','geoid2','pct_unemployment','pct_poverty']]
tract_frame = tract_frame[tract_frame != '-']
state_frame = statewide_median_incomes()


test_state.GEOID.isin(tests.apply(lambda x: ''.join(('0',str(x))))).sum()
#%%
def get_state_geoid(geoid):
    """
    1400000US01001020100
    """
    return ''.join(('0',geoid[1:11]))
def fix_median_income(val):
    if val == '250,000+':
        return 250000
    elif val == '2,500-':
        return 2500
    else:
        return float(val)

#%%
tract_frame['state_geoid'] = tract_frame.geoid.apply(get_state_geoid)
tract_frame['poverty_above_20pct'] = tract_frame.pct_poverty.astype(float) > 20

#%%
tract_frame = tract_frame.join(state_frame, on='state_geoid', rsuffix='_state')


#%%
tract_frame['ratio_state_median_income'] = tract_frame.median_income.apply(fix_median_income)/tract_frame.median_income_state
tract_frame['median_income_less_than_80pct'] = tract_frame.ratio_state_median_income < .80

# total = tract_frame.apply(tract_could_be_opp_zone, axis=1).count()
