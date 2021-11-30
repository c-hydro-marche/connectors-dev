# -------------------------------------------------------------------------------------
# Libraries
import logging
import tempfile
import os
import json
import pickle

# libraries needed for function "write_file_json()" - by Darienzo 25/11/2021.
import pandas as pd
import datetime
#from numpyencoder import NumpyEncoder
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write dataframe in csv format
def write_file_csv(file_name, data_frame,
                   data_separetor=',', data_encoding='utf-8', data_index=False, data_header=True):

    var_name = list(data_frame.columns.values)

    data_frame.to_csv(file_name, sep=data_separetor, encoding=data_encoding,
                      index=data_index, index_label=False, header=data_header,
                      columns=var_name)
# -------------------------------------------------------------------------------------




# -------------------------------------------------------------------------------------
# Convert the csv file containing reservoir water levels for several dams and
# at a specific time to a json file (in a specific configuration for Dewetra platform).
def write_file_json(data_frame):
    # ************************************************************************#
    # Programmers:    Matteo Darienzo                                         #
    # Last Modified:  16/11/2021. Version v1                                  #
    # Institute:      CIMA Foundation                                         #
    # References:                                                             #
    # To do list:                                                             #
    # Comments:                                                               #
    # ************************************************************************#
    # IN:                                                                     #
    #    1. data_frame         = df with water level for all dams at one time #
    # OUT:                                                                    #
    #    1. json_dams_dewetra = json dictionary with dam levels for dewetra   #
    ###########################################################################
    #df_dighe = pd.read_csv(file_name, sep=',', decimal='.', parse_dates=True)
    df_dams = data_frame

    time = df_dams.iloc[:, 3]
    for t in range(0, len(time)):
        try:
            time_temp = datetime.datetime.strptime(str(df_dams.iloc[t]['time']), '%Y-%m-%d %H:%M:%S')
        except:
            time_temp = datetime.datetime.strptime(str(df_dams.iloc[t]['time']), '%Y-%m-%d')

        time_temp = datetime.datetime.timestamp(time_temp)
        time_temp = "{:.0f}".format(time_temp)
        time[t] = time_temp

    tot_number_of_sections = len(df_dams['code'])
    json_dams_dewetra = [{} for i in range(tot_number_of_sections)]
    tot_number_data = 1   # for instance we consider only one value
    for sect in range(0, tot_number_of_sections):
        # print(sect)
        series = [{"dateTime": str(time[sect]), "value": str("{:.2f}".format(df_dams['data'][sect]))} for i in
                  range(0, tot_number_data)]
        json_dams_dewetra[sect] = {"sectionId": str(df_dams['code'][sect]), "serie": series}

    return (json_dams_dewetra)





###########################################################################
def json2dump_dams(df, file_json_name):
###########################################################################
# Purpose: create file json from dictionary                               #
# ************************************************************************#
# Programmers:    Matteo Darienzo, Fabio Delogu                           #
# Last Modified:  16/11/2021. Version v1                                  #
# Institute:      CIMA Foundation                                         #
# References:                                                             #
# To do list:                                                             #
# Comments:                                                               #
# ************************************************************************#
# IN:                                                                     #
#    1. df               = dictionary with dam levels for each dam        #
#    2. file_json_name   = name of the output file .json with dam levels  #
# OUT:                                                                    #
#    1. create a file .json  with dam levels data                         #
###########################################################################
    class SetEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(self, obj)

    with open(file_json_name, 'w') as f:
        json.dump(df, f, indent=4, sort_keys=False, separators=(', ', ': '), ensure_ascii=False, cls=SetEncoder)








# -------------------------------------------------------------------------------------
# Method to read json file
def read_file_json(file_name):
    with open(file_name, 'r', encoding="utf-8") as file_handle:
        file_data = json.load(file_handle)
    return file_data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a tmp name
def create_filename_tmp(prefix='tmp_', suffix='.tiff', folder=None):

    if folder is None:
        folder = '/tmp'

    with tempfile.NamedTemporaryFile(dir=folder, prefix=prefix, suffix=suffix, delete=False) as tmp:
        temp_file_name = tmp.name
    return temp_file_name
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read settings file
def read_file_settings(file_name):
    env_ws = {}
    for env_item, env_value in os.environ.items():
        env_ws[env_item] = env_value

    with open(file_name, "r") as file_handle:
        json_block = []
        for file_row in file_handle:

            for env_key, env_value in env_ws.items():
                env_tag = '$' + env_key
                if env_tag in file_row:
                    env_value = env_value.strip("'\\'")
                    file_row = file_row.replace(env_tag, env_value)
                    file_row = file_row.replace('//', '/')

            # Add the line to our JSON block
            json_block.append(file_row)

            # Check whether we closed our JSON block
            if file_row.startswith('}'):
                # Do something with the JSON dictionary
                json_dict = json.loads(''.join(json_block))
                # Start a new block
                json_block = []

    return json_dict

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read data obj
def read_obj(filename):
    if os.path.exists(filename):
        data = pickle.load(open(filename, "rb"))
    else:
        data = None
    return data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write data obj
def write_obj(filename, data):
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
# -------------------------------------------------------------------------------------
