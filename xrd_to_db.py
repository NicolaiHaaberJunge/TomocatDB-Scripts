"""
Script which submits xrd data to database. To run the script:
xrd_refinementToDB.py <dir> <dried and sealed (y/n)> <temperature>

<dir> = directory for refinement results files. Can be '.' or a specific folder.
<dried and sealed (1/0)> = 1 or 0  (integers)
<temperature> = sealing and drying temperature (integer)
"""

from numpy.core.numeric import zeros_like
from tomocatdb.data_model import *
import numpy as np
import sqlalchemy as sq
from sqlalchemy.orm import Session
import os
import pandas as pd
import sys
from datetime import datetime
import json

conn_string = 'postgresql+psycopg2://nicolai:tomocat@localhost/tomocat'  #  Database connection string
engine = sq.create_engine(conn_string)  #  Creating SQLAlchemy Engine

fit_param_kw = 'fitparams.txt'  #  Here we can set the fit parameter file keyword
xrd_kw = 'avg.xy'  #  Here we can set the xrd .xy file keyword
xrd_ref_kw = 'calc.xy'  #  Here we can set the ref xrd .xy file keyword

def xrd_to_db(engine, xrd_file, xrd_json, xrd_calc_json, parsed_ref, params):
    """
    Function that pushes xrd_exsitu data to database
    """
    dry_and_sealed, drying_temp = params
    folder, file = xrd_file.split('\\')  # Getting the folder name where the data files are stored

    material_name, reactor_sample, rs_layer_code, descr = get_file_metadata(file)  #  Getting the metadata stored in the xrd file name

    stmt_zeo = sq.select(Zeolites.internal_id).where(Zeolites.internal_id == material_name)  # Query statement to search for a present zeolite material
    stmt_ex = sq.select(Extrudates.internal_id).where(Extrudates.internal_id == material_name)  # Query statement to search for a present extrudate material
    stmt_sample = sq.select(ReactorSamples.layer_code).where(ReactorSamples.layer_code == rs_layer_code)  # Query statement to search for a reactor sample

    with Session(engine) as session:  # Commiting data to database

        zeo = session.execute(stmt_zeo).first()  # Getting any zeolite parent material if found 
        ext = session.execute(stmt_ex).first()  # Getting any extrudate parent material if found 
        reactor_samp = session.execute(stmt_sample).first()  # Getting any sample material if found

        # Creating the xrd ORM object and populating with data
        xrd_anal = xrdExSituAnalysis(
            dry_and_sealed = int(dry_and_sealed),
            drying_temp = drying_temp,
            creation_date =  datetime.now().date().strftime("%Y.%m.%d"),
            data_loc = os.path.join(os.getcwd(), folder),
            ref_res = parsed_ref,
            ref_xrd = xrd_calc_json,
            xrd = xrd_json
        )

        # Checking for a sample material.
        
        if reactor_samp:  # This takes priority over zeolite and extrudate
            xrd_anal.reactor_sample_id = reactor_samp.layer_code
        elif zeo:
            xrd_anal.zeolite_id = zeo.internal_id
        elif ext:
            xrd_anal.extrudate_id = ext.internal_id
        else:
            raise AssertionError ("No parent entry (zeolite, extrudate, or reactor sample) found in database!")
        #print(xrd_anal)
        session.add(xrd_anal)  # Added xrd object to session
        session.commit()  # Committing to DB

    return

def read_xrd(file):
    """
    Function to read the xrd .xy file
    """
    store = {'X':[], 'Yexp':[]}  # Dictionary to store the data

    with open(file, 'r') as f:
        i = 0
        for row in f:
            if i == 0:
                pass
            else:
                X, Yexp = row.split()
                store['X'].append(float(X))
                store['Yexp'].append(float(Yexp))
            i += 1

    json_data_string = json.dumps(store)  #  Serializing dictionary
    json_data  = json.loads(json_data_string)  #  Loading serialized dictionary as json-object

    return json_data

def read_xrd_calc(file):
    """
    Function to read the xrd refinement .xy file
    """
    store = {'X':[], 'Ycalc':[]}

    with open(file, 'r') as f:
         for row in f:
             X, Ycalc = row.split()
             store['X'].append(float(X))
             store['Ycalc'].append(float(Ycalc))

    json_data_string = json.dumps(store)  #  Serializing dictionary
    json_data  = json.loads(json_data_string)  #  Loading serialized dictionary as json-object

    return json_data

def get_file_metadata(file):
    """
    Function to obtain metadata from datafile name
    """
    split_sign = '_'
    material, reactor_sample, layer, descr = file.split(split_sign)
    reactor_layer_code = material + split_sign + reactor_sample + split_sign + layer

    return material, reactor_sample, reactor_layer_code, descr


def main():
    """
    Main program.
    """

    if sys.argv[1] == '.': # Option to process multiple folders
        folders = os.listdir(sys.argv[1])  # List of folders
    else:  # Option to process a single folder
        folders = [sys.argv[1]]

    #  Main program loop
    for folder in folders:
        all_files = os.listdir(folder)

        for file in all_files:  # Searching for the fitparams.txt file
            if fit_param_kw in file:
                ref_file = os.path.join(folder, file)
                ref_df = pd.read_csv(ref_file, skiprows=1, sep='\t', encoding = 'unicode_escape', engine ='python')  # Reading file into a dataframe
                ref_json = ref_df.to_json(orient="records")  # Converting df to json.¨
                parsed_ref = json.loads(ref_json)[0]
                
            elif xrd_kw in file:  #  Searching for the xrd exp .xy file
                xrd_file = os.path.join(folder, file)
                xrd_json = read_xrd(os.path.join(folder, file))

            elif xrd_ref_kw in file:  #  Searching for the ref xrd .xy file
                xrd_calc_json = read_xrd_calc(os.path.join(folder, file))


        dry_and_sealed = sys.argv[2]
        drying_temp = int(sys.argv[3])

        params = [dry_and_sealed, drying_temp]

        xrd_to_db(engine, xrd_file, xrd_json, xrd_calc_json, parsed_ref, params)

if __name__ == '__main__':
    main()

