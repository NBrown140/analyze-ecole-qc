"""
Merge census 2021 data into geographical boundaries to get geospatial data format
"""

import geopandas as gpd
import pandas as pd

from utils.csv_indexer import AdvancedCsvIndexer



if __name__ == "__main__":
    dissemination_area_file = "data/statscan/census_2021/dissemination_areas/lda_000a21a_e/lda_000a21a_e.shp"
    census_file = "data/statscan/census_2021/98-401-X2021006_Quebec_eng_CSV/98-401-X2021006_English_CSV_data_Quebec.csv"
    fields_of_interest = []
    ID_COLUMN = "DGUID"

    # Build index of large census file
    indexer = AdvancedCsvIndexer(census_file, id_column=ID_COLUMN, index_dir="indexes")
    if not indexer.is_index_valid():
        indexer.create_index()#dtype=dtypes)

    # Read dissemination area file
    # dissemination_areas = gpd.read_file(dissemination_area_file)
    # print(dissemination_areas.head())

    dguid = "2021A000011124"
    df = indexer.load_rows_by_id(dguid)
    df.to_csv("test.csv")
    print(df)

    df2 = pd.read_csv(census_file, nrows=3000, encoding=indexer.encoding)
    df2.to_csv("test2.csv")
    print(df2)

    exit()

    # For each dissemination area
    # for i, row in dissemination_areas.iterrows():
    #     print(i)
    #     dguid = row["DGUID"]
    #     print(dguid)

        # # Find related data in 2021 census data
        # df = indexer.load_rows_by_id(dguid)

        # if len(df) > 0:
        #     print(dguid)
        #     print(df.head())
        #     break
        # Keep only desired fields

        # Add to new geodataframe

    # Save to geoparquet/geojson/geopackage

