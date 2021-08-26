"""
Script which submits xrd data to database. To run the script:
xrd_refinementToDB.py <dir> <coke>

<dir> = directory for refinement results files. Can be '.' or a specific folder.
<coke> = coke (Optional, type "coke" if the sample contains coke)
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

def read_tg_dat(file):

    ### READING METADATA
    file_metadata = {}

    with open(file, 'r') as f:
        
        i = 0
        for row in f:
            if not row == '\n':
                row_data = row.split(';')
                key, val = row_data
                new_key, _ = key[1:].split(':')
                file_metadata[new_key] = val[:-1]
                i += 1
            else:
                break
    # Wanted data from the total metadata
    wanted_keys = ['SAMPLE', 'DATE/TIME', 'SAMPLE', 'TEMPCAL', 'SENSITIVITY',
     'SAMPLE MASS /mg', 'SAMPLE CRUCIBLE MASS /mg']

    keys = list(file_metadata.keys()) 

    segment_keys = [seg for seg in keys if 'SEG.' in seg]  # Getting all segment keys
    wanted_keys = wanted_keys + segment_keys  # The total wanted keys
    for key in keys:
        if key not in wanted_keys:
            del file_metadata[key]
        if key == 'DATE/TIME':
            val = file_metadata[key].split(' ')[0] # Extracting only the date from the timestamp
            val = '.'.join(val.split('.')[::-1])
            file_metadata[key] = val

    ## Cleaning up key labels   
    keys = list(file_metadata.keys())
    for key in keys:
        val = file_metadata[key].split()[0]
        file_metadata[key] = val
        if '/' in key:  # Removing / and ' ' from label names
            new_key = key.split('/')[0]
            if 'DATE' not in new_key:
                new_key = new_key[:-1]
            file_metadata[new_key] = file_metadata.pop(key)

    segs = {}  # placeholder dictionary
    keys = list(file_metadata.keys())
    for key in keys:
        file_metadata[key.lower()] = file_metadata.pop(key)
        if 'SEG.' in key:
            key = key.lower()
            segs[key] = file_metadata[key]
            del file_metadata[key]
    
    file_metadata['segments'] = segs

    file_metadata['sample crucible mass'] = float(file_metadata['sample crucible mass'])
    file_metadata['sample mass'] = float(file_metadata['sample mass'])

    json_metadata = json.loads(json.dumps(file_metadata))

    ### READING MEASURMENT DATA
    df_meas = pd.read_csv(file, skiprows=i, sep=';', skipfooter=1, encoding = 'unicode_escape', engine='python')  #  Reading to dataframe

    columns = df_meas.columns.to_list()  #  Extracting columns
    columns = [c.split('/')[0] for c in columns]
    columns[0] = columns[0][2:-1]  # Removing characters ##Temp. --> Temp
    columns[-1] = columns[-1][:-1]  # Removing characters Sensit. --> Sensit
    df_meas.columns = columns  #  Setting new column names

    df_meas = df_meas.apply(pd.to_numeric, errors='ignore')

    json_meas = df_meas.to_json(orient="split", index=False)  #  Converting to json
    json_meas_parsed = json.loads(json_meas)  #  Returning as json object
    return json_metadata, json_meas_parsed

def read_tg_res(file):

    with open(file, 'r') as f:

        lines = f.readlines()

        j = 0  # Row index Counter
        for row in lines:
            if row == '\n':
                j += 1
                break  # We break the line if we encounter '\n'
            else:
                j += 1

        i = 0  # Counter
        data = []
        res = []
        for line in lines[j:-1]:  # Loop over the lines related to measurment data
            if i == 0:  #  Here we set the index for the following dataframe by reading the first row
                index = line.split(';')
                index[-1] = index[-1][:-1]
                index = [h.split()[0] for h in index]
                index = index[1:]
            else:  # Here we read the data and columns
                dat = line.split(';')
                dat = [h.split()[0] for h in dat]
                res.append(dat[0][2:])
                dat = dat[1:]  #  The column name for the data
                data.append(dat)
            i += 1
        # -- creating dataframe
        dfs = []
        for i in range(len(res)):
            df = pd.DataFrame(data[i], index=index, columns=[res[i]])  # We create single dfs for each row.
            dfs.append(df)  
        df_res = pd.concat([f for f in dfs], axis=1) #  Concatenate them to a single df.
        df_res.drop(['File', 'Segment', 'Unit', 'Range_Xmin', 'Range_Xmax', 'Range_Ymin', 'Range_Ymax'], axis=0, inplace=True)  # Dropping columns we don't need

        cols  = df_res.columns.tolist()
        cols[0] = 'Peak_DSC'  #  Changing to more conveniet names
        cols[1] = 'Mass_H2O'
        cols[-2] = 'Residual_Mass'
        cols[-1] = 'Peak_DTG'

        

        df_res.columns = cols
        df_res = df_res.apply(lambda x: x.str.replace(',', '.'))  # Converting decimal commas to dot
        df_res = df_res.apply(pd.to_numeric, errors='coerce')  # Converting strings to numbers

        df_serialized = df_res.to_json(orient='columns')

        res_json = json.loads(df_serialized)
        return res_json

def tg_to_db(folder, json_res, json_meta, json_meas):

    material_name = folder.split('_')[0]
    rs_layer_code = folder

    stmt_zeo = sq.select(Zeolites.internal_id).where(Zeolites.internal_id == material_name)  # Query statement to search for a present zeolite material
    stmt_ex = sq.select(Extrudates.internal_id).where(Extrudates.internal_id == material_name)  # Query statement to search for a present extrudate material
    stmt_sample = sq.select(ReactorSamples.layer_code).where(ReactorSamples.layer_code == rs_layer_code)  # Query statement to search for a reactor sample

    with Session(engine) as session:  # Commiting data to database

        zeo = session.execute(stmt_zeo).first()  # Getting any zeolite parent material if found 
        ext = session.execute(stmt_ex).first()  # Getting any extrudate parent material if found 
        reactor_samp = session.execute(stmt_sample).first()  # Getting any sample material if found

        tg_anal = tgAnalysis(
            water_content_wpct = round(-100*float(json_res['Mass_H2O']['Result'])/float(json_meta['sample mass']), 2),
            meta = json_meta,
            results = json_res,
            data_loc = os.path.join(os.getcwd(), folder),
            data = json_meas,
            creation_date = json_meta['date'],
        )

        # Checking for a sample material.
        
        if reactor_samp:  # This takes priority over zeolite and extrudate
            tg_anal.reactor_sample_id = reactor_samp.layer_code
        elif zeo:
            tg_anal.zeolite_id = zeo.internal_id
        elif ext:
            tg_anal.extrudate_id = ext.internal_id
        else:
            raise AssertionError ("No parent entry (zeolite, extrudate, or reactor sample) found in database!")
        print(tg_anal)

        session.add(tg_anal)  # Added xrd object to session
        session.commit()  # Committing to DB


    return

def calc_coke_content(metadata, results):

    sample_mass = metadata['sample mass']
    water_content = results['Mass_H2O']['Result']
    residual_mass = results['Residual_Mass']['Y_value']

    sample_dry_mass = sample_mass + water_content
    coke_content = {'Result' : round(-1*residual_mass+water_content, 2), 'Y_value' : '-'}
    sample_dry = {'Result' : round(sample_dry_mass, 2), 'Y_value' : '-'}

    results['Mass_Coke'] = coke_content
    results['Sample_Dry_Mass'] = sample_dry
    
    return results

def main():

    if sys.argv[1] == '.':
        folders = os.listdir(sys.argv[1])
    else:
        folders = [sys.argv[1]]

    for folder in folders:
        files = os.listdir(folder)
        files_dat = [f for f in files if 'ExpDat' in f]
        files_res = [f for f in files if 'ExpRes' in f]

        tg_metadata, tg_meas = read_tg_dat(os.path.join(folder, files_dat[0]))
        json_res = read_tg_res(os.path.join(folder, files_res[0]))

        if len(sys.argv) == 3:
            if sys.argv[2].lower() == 'coke':
                json_res = calc_coke_content(tg_metadata, json_res)  #  Adds coke content to json_result

        tg_to_db(folder, json_res, tg_metadata, tg_meas)
    return


if __name__ == '__main__':
    main()