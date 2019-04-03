#%%
import pandas as pd 
import dask.dataframe as dd
from pathlib import Path
import geopandas as gpd
import json

#%%
def statewide_median_incomes():
    """
    Reads in census file of the 50 states and creates a dictionary
    of the median incomes of each of those states
    """
    column_names = {
        'HC01_VC03':'population_16_and_over',
        'HC01_VC114': 'median_income',
        "GEO.id2":"state_geoid",
        "GEO.id":'geoid',
        "HC03_VC07":"pct_unemployment",
        "HC03_VC161": "pct_poverty"
    }
    state_frame = pd.read_csv("data/states/ACS_15_5YR_DP03_with_ann.csv"
        ).rename(columns=column_names).set_index('geoid')
    state_frame = state_frame[state_frame.index.str.startswith("0400000US")]
    return state_frame[['population_16_and_over','median_income','pct_unemployment','pct_poverty']] #return only the actual states


def fix_median_income(val):
    if val == '250,000+':
        return 250000
    elif val == '2,500-':
        return 2500
    elif val == '(X)':
        return None
    else:
        return float(val)

def metroAreas():
    dtypes = {'FIPS State Code':str, 'FIPS County Code':str}
    csa_delineation = pd.read_csv("data/msa_delineations2015.csv", dtype=dtypes)
    csa_incomes = pd.read_csv("data/csa/ACS_15_5YR_B19113.csv").rename(columns={'HD01_VD01':'msa_median_income'})
    csa_complete_info = pd.merge(csa_delineation, csa_incomes, left_on='CSA Code', right_on='CSA',how='inner')
    csa_complete_info['state_county'] = csa_complete_info['FIPS State Code'] + csa_complete_info['FIPS County Code']
    csa_complete_info = csa_complete_info.set_index("state_county")
    return csa_complete_info
#%%
def isCensusTractQualified():
    """
    Identifies if census tract meets Low-Income Definition based on
    26 USC 45D reviewed at https://bit.ly/2OvWSed quoted below
    ```
    (e) Low-income community

For purposes of this section-
(1) In general

The term "low-income community" means any population census tract if-

(A) the poverty rate for such tract is at least 20 percent, or

(B)(i) in the case of a tract not located within a metropolitan area, the median family 
income for such tract does not exceed 80 percent of statewide median family income, or

(ii) in the case of a tract located within a metropolitan area, the median family income for
 such tract does not exceed 80 percent of the greater of statewide median family income or the metropolitan area median family income.
```
    Tract is expected to be a pandas series
"""
    column_names = {
        'HC01_VC03':'population_16_and_over',
        'HC01_VC114': 'median_income',
        "GEO.id2":"geoid2",
        "GEO.id":'geoid',
        "HC03_VC07":"pct_unemployment",
        "HC03_VC161": "pct_poverty"
    }
    tract_frame = pd.read_csv("data/tracts/ACS_15_5YR_DP03_with_ann.csv").rename(columns=column_names)
    tract_frame = tract_frame[['geoid','median_income','geoid2','pct_unemployment','pct_poverty']]
    tract_frame = tract_frame[tract_frame != '-']
    state_frame = statewide_median_incomes()
    csa_frame = metroAreas()
    def get_state_geoid(geoid):
        """
        1400000US01001020100
        """
        return ''.join(('0',geoid[1:11]))
    def get_csa_code(geoid2):
        """
        25017350103
        """
        test_geoid = str(geoid2)
        return test_geoid[0:5]
    tract_frame['state_geoid'] = tract_frame.geoid.apply(get_state_geoid)
    tract_frame['csa_code'] = tract_frame.geoid2.apply(get_csa_code)
    tract_frame = tract_frame.join(csa_frame['msa_median_income'], on='csa_code', how='left')
    tract_frame['poverty_above_20pct'] = tract_frame.pct_poverty.astype(float) > 20
    tract_frame = tract_frame.join(state_frame, on='state_geoid', rsuffix='_state')
    tract_frame['ratio_state_median_income'] = tract_frame.median_income.apply(fix_median_income)/tract_frame.median_income_state
    tract_frame['ratio_msa_median_income'] = tract_frame.median_income.apply(fix_median_income)/tract_frame.msa_median_income
    tract_frame['state_median_income_less_than_80pct'] = tract_frame.ratio_state_median_income < .80
    tract_frame['msa_median_income_less_than_80pct'] = tract_frame.ratio_msa_median_income < .80
    def tract_is_opp_zone(tract):
        if tract.poverty_above_20pct:
            return True
        elif tract.msa_median_income_less_than_80pct and  not pd.isna(tract.msa_median_income): #Nones will be false
            return True
        elif tract.state_median_income_less_than_80pct and pd.isna(tract.msa_median_income):
            return True
        else:
            return False
    tract_frame['qual_as_opp_zone'] = tract_frame.apply(tract_is_opp_zone, axis=1)
    return tract_frame

def fix_geoid(geoid2):
    return ''.join(['1400000US', str(geoid2)])


isCensusTractQualified()