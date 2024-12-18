# Intro

The goal of this project was to build an algorithm that matches quebec high schools (specifically frnech and public ones) to dissemination areas from the Stats Can Census 2021.

All the work is in `explore.ipynb`. The first part ("Part 1: Build a new census geospatial dataset for Quebec") was an attempt to build one large geoparquet file that represents dissemination area polygons and the associated census data. The second part ("Part 2: Combine with school data") is the main code that makes the association between dissemination areas and schools.


# Output Data

Mapping between dissemination areas ("DGUID") and schools ("Code"): https://github.com/NBrown140/analyze-ecole-qc/blob/main/data/output/census_2021_qc_parsed_with_schools_fr_public_mapping.csv


# Miscellaneous

StatsCan:
- Recensement 2021 par aires de diffusion: https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger.cfm?Lang=F
- Area unit nomenclature:
    - EN: Dissemination blocks < Dissemination areas < Aggregate dissemination areas < Economic regions
    - FR: Ilots de diffusion < Aires de diffusion <  Aires de diffusion agregees < Regions economiques


Interesting data to incorporate:
- StatsCan. Elementary–Secondary Education Survey, 2022/2023: https://www150.statcan.gc.ca/n1/daily-quotidien/241010/dq241010b-eng.htm
