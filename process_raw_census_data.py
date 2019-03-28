def statewide_median_incomes():
    """
    Reads in census file of the 50 states and creates a dictionary
    of the median incomes of each of those states
    """
    state_median_incomes = {}
    return state_median_incomes


def isCensusTractQualified(tract):
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
tract_poverty_rate = #20%
state_median_incomes = statewide_median_incomes()
state_median_income = state_median_incomes['somehowlookup via geoid']
if tract_poverty_rate > .20:  
    return True
elif tract.median_income/state_median_income > .80:
    return True
else:
    return False

