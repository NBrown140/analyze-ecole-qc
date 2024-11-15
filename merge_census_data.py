"""
Merge census 2021 data into geographical boundaries to get geospatial data format
"""
import os

import chardet
import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from utils.csv_indexer import AdvancedCsvIndexer


# def read_rows_by_id(file_path, target_id, id_column='id', chunksize=10000, **kwargs):
#     """
#     Find rows in a large csv file that match an id without loading the whole file into memory.

#     Args:
#         file_path (str): Path to the CSV file
#         target_id (str): The ID value to match
#         id_column (str): Name of the ID column (default: 'id')
#         chunksize (int): Number of rows to read at a time (default: 10000)
#         **kwargs: Additional arguments to pass to pd.read_csv()

#     Returns:
#         pandas.DataFrame: DataFrame containing all matching rows
#     """
#     matching_rows = []

#     # Read the CSV in chunks, passing through any additional pandas read_csv arguments
#     for chunk in tqdm(pd.read_csv(file_path, chunksize=chunksize, **kwargs)):
#         # Handle numeric IDs if necessary
#         if pd.api.types.is_numeric_dtype(chunk[id_column]):
#             matches = chunk[chunk[id_column] == float(target_id)]
#         else:
#             matches = chunk[chunk[id_column] == target_id]

#         if not matches.empty:
#             matching_rows.append(matches)

#     if matching_rows:
#         return pd.concat(matching_rows, ignore_index=True)
#     else:
#         return pd.read_csv(file_path, nrows=0, **kwargs)


# def detect_encoding(file_path):
#     """
#     Detect the encoding of a file using chardet
#     """
#     # Read raw bytes from the file
#     with open(file_path, 'rb') as file:
#         # Read a sample of the file (first 1MB) to detect encoding
#         raw_data = file.read(min(1024*1024, os.path.getsize(file_path)))
#         result = chardet.detect(raw_data)

#     print(f"Detected encoding (confidence={result['confidence']:.2%}): {result['encoding']}")
#     return result['encoding']


if __name__ == "__main__":
    dissemination_area_file = "data/statscan/census_2021/dissemination_areas/lda_000a21a_e/lda_000a21a_e.shp"
    census_file = "data/statscan/census_2021/98-401-X2021006_Quebec_eng_CSV/98-401-X2021006_English_CSV_data_Quebec.csv"
    fields_of_interest = []
    ID_COLUMN = "DGUID"

    # Build index of large census file
    indexer = AdvancedCsvIndexer(census_file, id_column=ID_COLUMN, index_dir="indexes")

    # # Define data types (optional but recommended for large files)
    # dtypes = {
    #     'id': str,
    #     'value': float,
    #     'category': 'category'
    # }

    # Create index if needed
    if not indexer.is_index_valid():
        indexer.create_index()#dtype=dtypes)

    # Perform fast lookups
    result_df = indexer.load_rows_by_id("2021S051210010232")#, dtype=dtypes)
    print(result_df)






    # Read dissemination area file
    dissemination_areas = gpd.read_file(dissemination_area_file)
    print(dissemination_areas.head())

    # For each dissemination area
    for i, row in dissemination_areas.iterrows():
        print(i)
        dguid = row["DGUID"]
        print(dguid)


        a=pd.read_csv(census_file, nrows=10, encoding=detect_encoding(census_file))
        print(a.head())
        break
        # Find related data in 2021 census data
        df = read_rows_by_id(census_file, dguid, "DGUID", encoding=detect_encoding(census_file))
        print(df.head())
        break
        # Keep only desired fields

        # Add to new geodataframe

    # Save to geoparquet/geojson/geopackage

