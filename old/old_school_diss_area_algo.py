# Old matching algo between schools and diss areas

def match_schools_to_diss_areas(
        schools_gdf: gpd.GeoDataFrame,
        diss_areas_gdf: gpd.GeoDataFrame,
        buffer_increment=100,  # meters
        school_id_col="Code",
        school_num_students_col="Number of students",
        diss_area_id_col="DGUID",
        ):
    diss_areas_gdf = diss_areas_gdf.copy()
    schools_gdf["matched_diss_area_ids"] = ""

    # Process each school
    for idx, school in tqdm(schools_gdf.iterrows()):
        school_point = school.geometry
        target_students = school[school_num_students_col]
        matched_students = 0
        current_buffer = buffer_increment

        while matched_students < target_students:
            # Create buffer around school
            buffer_area = school_point.buffer(current_buffer)

            # Find diss_areas that intersect with buffer
            nearby_diss_areas = diss_areas_gdf[
                diss_areas_gdf.intersects(buffer_area)
            ].copy()

            # Calculate the number of secondary students in matched dissemination areas
            # The logic assumes that all ages from 10-19 years are equally divided.
            col_age_10_14 = "Total - Age groups of the population - 100% data > 0 to 14 years > 10 to 14 years"
            col_age_15_19 = "Total - Age groups of the population - 100% data > 15 to 64 years > 15 to 19 years"
            matched_students = (nearby_diss_areas[col_age_10_14].sum() + nearby_diss_areas[col_age_15_19].sum()) * 5 / 10  # We only want 5/10 of all kids with ages 10-19 (10), because secondary is 12-16 (5)
            current_buffer += buffer_increment

        schools_gdf.loc[idx, "matched_diss_area_ids"] = ",".join(nearby_diss_areas[diss_area_id_col].values)

    return schools_gdf