import os
import pandas as pd
import numpy as np

def load_eia_data_old(filter_region = None, base_dir = '..', version = ""):
    DATA_PATH = os.path.join(base_dir)
    REPO_DATA_PATH = os.path.join(base_dir)

    EIA__GEN_FILE = os.path.join(DATA_PATH,  f"3_1_Generator_{version}.xlsx" )
    EIA_PLANT_FILE = os.path.join(DATA_PATH, f"2___Plant_{version}.xlsx" )
    EIA_STORAGE_FILE = os.path.join(DATA_PATH, f"3_4_Energy_Storage_{version}.xlsx" )

    EIA_TECH_FILE = os.path.join(REPO_DATA_PATH,'eia_mappings', "eia_tech_mapping.csv" )
    EIA_FUEL_FILE = os.path.join(REPO_DATA_PATH,'eia_mappings', "eia_fuel_mapping.csv" )
    EIA_PRIMEMOVER_FILE = os.path.join(REPO_DATA_PATH,'eia_mappings',"eia_primemover_mapping.csv" )


    gen_cols = ["Plant Code", "Plant Name", "Generator ID", "Operating Year", "Nameplate Capacity (MW)","Summer Capacity (MW)", "Winter Capacity (MW)", "Minimum Load (MW)", "Energy Source 1", "Technology", "Status", "Prime Mover"]
    plant_cols = ["Plant Code",'NERC Region', 'Balancing Authority Code', "State", "Latitude" ,"Longitude"]
    storage_cols = ['Plant Code','Generator ID', 'Nameplate Energy Capacity (MWh)','Maximum Charge Rate (MW)', 'Maximum Discharge Rate (MW)','Storage Technology 1',]

    eia_data_operable = pd.read_excel(EIA__GEN_FILE, sheet_name="Operable", skiprows = 2, usecols=gen_cols) #, dtype=str)
    eia_storage = pd.read_excel(EIA_STORAGE_FILE, sheet_name="Operable", skiprows = 2, usecols=storage_cols)

    eia_loc = pd.read_excel(EIA_PLANT_FILE, sheet_name="Plant", skiprows = 2, usecols=plant_cols)
    eia_loc.rename(columns={'Plant Code': 'plant_id_eia'}, inplace=True)

    eia_tech_map = pd.read_csv(EIA_TECH_FILE, index_col = "Technology")
    eia_fuel_map = pd.read_csv(EIA_FUEL_FILE, index_col = "Energy Source 1")
    eia_primemover_map = pd.read_csv(EIA_PRIMEMOVER_FILE, index_col = "Prime Mover")
    tech_dict = dict(zip(eia_tech_map.index, eia_tech_map.primary_fuel.values))
    fuel_dict = dict(zip(eia_fuel_map.index, eia_fuel_map.primary_fuel.values))
    primemover_dict = dict(zip(eia_primemover_map.index, eia_primemover_map.prime_mover.values))

    #modify storage data
    eia_storage['plant_id_eia'] = eia_storage['Plant Code'].astype(int)
    eia_storage.rename(columns={'Plant Name': 'plant_name_eia','Generator ID':'generator_id','Nameplate Energy Capacity (MWh)':'energy_capacity_mwh','Maximum Charge Rate (MW)':'max_charge_rate_mw', 'Maximum Discharge Rate (MW)':'max_discharge_rate_mw'}, inplace=True)
    eia_storage.drop(columns = ['Plant Code',], inplace=True)
    eia_storage['energy_capacity_mwh'] = eia_storage['energy_capacity_mwh'].replace(' ', np.nan).astype(float)
    eia_storage['max_charge_rate_mw'] = eia_storage['max_charge_rate_mw'].replace(' ', np.nan).astype(float)
    eia_storage['max_discharge_rate_mw'] = eia_storage['max_discharge_rate_mw'].replace(' ', np.nan).astype(float)

    #modify data operable
    eia_data_operable.dropna(how='all', inplace=True)
    eia_data_operable['tech_type'] = eia_data_operable['Technology'].map(tech_dict)
    eia_data_operable['fuel_type'] = eia_data_operable['Energy Source 1'].map(fuel_dict)
    eia_data_operable['prime_mover'] = eia_data_operable['Prime Mover'].map(primemover_dict)
    eia_data_operable['plant_id_eia'] = eia_data_operable['Plant Code'].astype(int)
    eia_data_operable.rename(columns={'Plant Name': 'plant_name_eia','Generator ID':'generator_id', 'Nameplate Capacity (MW)':'capacity_mw','Summer Capacity (MW)':'summer_capacity_mw','Winter Capacity (MW)':'winter_capacity_mw','Minimum Load (MW)':'p_nom_min','Operating Year':'operating_year'}, inplace=True)
    eia_data_operable.drop(columns = ['Technology', 'Energy Source 1', 'Plant Code','Prime Mover'], inplace=True)
    eia_data_operable['capacity_mw'] = eia_data_operable['capacity_mw'].replace(' ', 0).fillna(0).astype(float)
    eia_data_operable['summer_capacity_mw'] = eia_data_operable['summer_capacity_mw'].replace(' ', 0).fillna(0).astype(float)
    eia_data_operable['winter_capacity_mw'] = eia_data_operable['winter_capacity_mw'].replace(' ', 0).fillna(0).astype(float)
    eia_data_operable['p_nom_min'] = eia_data_operable['p_nom_min'].replace(' ', 0).fillna(0).astype(float)
    eia_data_operable['operating_year'] = eia_data_operable['operating_year'].replace(' ', -1).fillna(-1).astype(int)

    # Merge locations and plant data
    eia_plants_locs = pd.merge(eia_data_operable, eia_loc, on='plant_id_eia', how='inner')
    if filter_region is not None:
        eia_plants_locs = eia_plants_locs[eia_plants_locs['NERC Region']== filter_region]
    eia_plants_locs.plant_id_eia =eia_plants_locs.plant_id_eia.astype(int)

    eia_plants_locs = pd.merge(eia_plants_locs, eia_storage, on=['plant_id_eia', 'generator_id'], how='left')


    return eia_data_operable, eia_storage, eia_loc, eia_plants_locs


def standardize_col_names(columns, prefix="", suffix=""):
    """Standardize column names by removing spaces, converting to lowercase, removing parentheses, and adding prefix and suffix."""
    return [prefix + col.lower().replace(" ", "_").replace("(", "").replace(")", "") + suffix for col in columns]

def convert_mixed_types_and_floats(df):
    """
    Convert columns with mixed types to string type, and columns with float types
    without decimal parts to integer type in a DataFrame.
    
    Parameters:
    - df: pandas.DataFrame - The DataFrame to process.
    
    Returns:
    - pandas.DataFrame: The DataFrame with mixed type columns converted to strings
      and float columns without decimals converted to integers.
    """
    # Attempt to standardize types where possible
    df = df.infer_objects()
    
    # Function to check and convert mixed type column to string
    def convert_if_mixed(col):
        # Detect if the column has mixed types (excluding NaN values)
        if not all(col.apply(type).eq(col.apply(type).iloc[0])):
            return col.astype(str)
        return col
    
    # Apply the conversion function to each column in the DataFrame
    df = df.apply(convert_if_mixed)
    
    # # Convert float columns without decimal parts to integers
    # for col in df.select_dtypes(include=['float']).columns:
    #     if df[col].dropna().apply(float.is_integer).all():
    #         df[col] = df[col].astype(pd.Int64Dtype())
    
    return df



