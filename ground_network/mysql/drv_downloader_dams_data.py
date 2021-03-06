# -------------------------------------------------------------------------------------
# Libraries
import logging
import os

import pandas as pd

from copy import deepcopy

from ground_network.mysql.lib_utils_io import write_file_csv, write_obj, read_obj, write_file_json, \
    json2dump_dams  # Matteo: add of functions "write_file_json, json2dump_dams"
from ground_network.mysql.lib_utils_system import fill_tags2string, make_folder, get_root_path, list_folder

from ground_network.mysql.lib_utils_db_dams import define_db_settings, get_db_credential, \
    parse_query_time, get_data_dams, organize_data_dams, order_data


# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Class driver dynamic data
class DriverData:

    def __init__(self, time_step, dams_collection=None, src_dict=None, ancillary_dict=None, dst_dict=None,
                 time_dict=None, variable_dict=None, template_dict=None, info_dict=None,
                 flag_updating_ancillary=True, flag_updating_destination=True, flag_cleaning_tmp=True):

        self.time_step = time_step
        self.dams_collection = dams_collection

        self.src_dict = src_dict
        self.ancillary_dict = ancillary_dict
        self.dst_dict = dst_dict
        self.time_dict = time_dict
        self.variable_dict = variable_dict
        self.template_dict = template_dict

        self.tag_folder_name = 'folder_name'
        self.tag_file_name = 'file_name'
        self.tag_file_active = 'active'

        self.tag_file_fields = 'fields'

        self.domain_name = info_dict['domain']
        self.variable_list = list(self.variable_dict.keys())

        self.time_range = self.collect_file_time()

        self.db_info = self.collect_db_settings(self.src_dict)
        self.db_settings = define_db_settings(self.db_info)

        self.folder_name_anc_dset_raw = self.ancillary_dict[self.tag_folder_name]
        self.file_name_anc_dset_raw = self.ancillary_dict[self.tag_file_name]
        self.file_path_anc_dset_obj = self.collect_file_list(self.folder_name_anc_dset_raw, self.file_name_anc_dset_raw)

        # Matteo: add of these 2 following 'if'. the first for reading/writing csv file with dam water levels
        # and one for writing them to json file for dewetra.
        if 'csv' in list(self.dst_dict.keys()):
            self.folder_name_dst_csv_dset_raw = self.dst_dict['csv'][self.tag_folder_name]
            self.file_name_dst_csv_dset_raw = self.dst_dict['csv'][self.tag_file_name]
            self.file_active_dst_csv = self.dst_dict['csv'][self.tag_file_active]
            self.file_path_dst_csv_dset_obj = self.collect_file_list(
                self.folder_name_dst_csv_dset_raw, self.file_name_dst_csv_dset_raw)
            self.file_fields_dst_dset = self.dst_dict['csv'][self.tag_file_fields]
        else:
            logging.warning('The csv file that will contain the dam water levels for Dewetra has not been well' +
                            'configured at the destination field of the initial configuration json file. Please check.')

        if 'json' in list(self.dst_dict.keys()):
            self.folder_name_dst_json_dset_raw = self.dst_dict['json'][self.tag_folder_name]
            self.file_name_dst_json_dset_raw = self.dst_dict['json'][self.tag_file_name]
            self.file_active_dst_json = self.dst_dict['json'][self.tag_file_active]
            self.file_path_dst_json_dset_obj = self.collect_file_list(
                self.folder_name_dst_json_dset_raw, self.file_name_dst_json_dset_raw)
            # self.file_fields_dst_dset = self.dst_dict['json'][self.tag_file_fields]
        else:
            logging.warning('The json file that will contain the dam water levels for Dewetra has not been well' +
                            'configured at the destination field of the initial configuration json file. Please check.')

        self.flag_updating_ancillary = flag_updating_ancillary
        self.flag_updating_destination = flag_updating_destination

        self.flag_cleaning_tmp = flag_cleaning_tmp

        self.folder_name_anc_main = get_root_path(self.folder_name_anc_dset_raw)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect database information
    @staticmethod
    def collect_db_settings(db_info):

        db_info_upd = deepcopy(db_info)

        if 'server_mode' not in list(db_info_upd.keys()):
            logging.error(' ===> Server mode field not defined')
            raise IOError('Check your server information.')

        if 'server_ip' not in list(db_info_upd.keys()):
            logging.error(' ===> Server ip field not defined')
            raise IOError('Check your server information.')

        if 'server_name' not in list(db_info_upd.keys()):
            logging.error(' ===> Server name field not defined')
            raise IOError('Check your server information.')

        if 'server_user' not in list(db_info_upd.keys()):
            logging.error(' ===> Server user field not defined')
            raise IOError('Check your server information.')

        if 'server_password' not in list(db_info_upd.keys()):
            logging.error(' ===> Server password field not defined')
            raise IOError('Check your server information.')

        logging.info(' ---> Search password and user ...')
        server_user = db_info_upd['server_user']
        server_password = db_info_upd['server_password']
        if (server_password is None) and (server_user is None):
            server_name = db_info_upd['server_name']

            server_user, server_password = get_db_credential(server_name)
            db_info_upd['server_user'] = server_user
            db_info_upd['server_password'] = server_password

            logging.info(' ---> Search password and user ... found in netrc file. OK')

        elif (server_password is None) and server_user:
            logging.info(
                ' ---> Search password and user ... found in configuration file (password is null). OK')
        elif server_password and server_user:
            logging.info(
                ' ---> Search password and user ... found in configuration file (user and password are defined). OK')
        else:
            logging.error(' ===> Server user and password configuration is not allowed')
            raise IOError('Check your file settings!')

        return db_info_upd

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect time(s)
    def collect_file_time(self, time_reverse=True):

        time_period = self.time_dict["time_period"]
        time_frequency = self.time_dict["time_frequency"]
        time_rounding = self.time_dict["time_rounding"]

        time_end = self.time_step.floor(time_rounding)

        time_range = pd.date_range(end=time_end, periods=time_period, freq=time_frequency)

        if time_reverse:
            time_range = time_range[::-1]

        return time_range

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect ancillary file
    def collect_file_list(self, folder_name_raw, file_name_raw):

        domain_name = self.domain_name

        file_name_obj = {}
        for variable_step in self.variable_list:
            variable_tag = self.variable_dict[variable_step]['tag']

            if variable_tag is not None:
                file_name_list = []
                for datetime_step in self.time_range:
                    template_values_step = {
                        'domain_name': domain_name,
                        'ancillary_var_name': variable_step,
                        'destination_var_name': variable_step,
                        'ancillary_datetime': datetime_step, 'ancillary_sub_path_time': datetime_step,
                        'destination_datetime': datetime_step, 'destination_sub_path_time': datetime_step}

                    folder_name_def = fill_tags2string(
                        folder_name_raw, self.template_dict, template_values_step)
                    file_name_def = fill_tags2string(
                        file_name_raw, self.template_dict, template_values_step)
                    file_path_def = os.path.join(folder_name_def, file_name_def)

                    file_name_list.append(file_path_def)

                file_name_obj[variable_step] = file_name_list

        return file_name_obj

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to download datasets
    def download_data(self):

        logging.info(' ----> Download datasets ... ')

        time_range = self.time_range
        file_path_anc_obj = self.file_path_anc_dset_obj
        file_path_dst_obj = self.file_path_dst_csv_dset_obj
        var_dict = self.variable_dict

        flag_upd_anc = self.flag_updating_ancillary
        flag_upd_dst = self.flag_updating_destination

        for var_name, var_fields in var_dict.items():

            logging.info(' -----> Variable ' + var_name + ' ... ')

            var_tag = var_fields['tag']
            var_download = var_fields['download']
            var_type = var_fields['type']

            if var_tag is not None:

                file_path_anc_list = file_path_anc_obj[var_name]
                file_path_dst_list = file_path_dst_obj[var_name]

                if var_download:

                    for time_step, file_path_anc_step, file_path_dst_step in zip(
                            time_range, file_path_anc_list, file_path_dst_list):

                        logging.info(' ------> Time Step ' + str(time_step) + ' ... ')

                        if flag_upd_anc:
                            if os.path.exists(file_path_anc_step):
                                os.remove(file_path_anc_step)

                        if flag_upd_dst:
                            if os.path.exists(file_path_dst_step):
                                os.remove(file_path_dst_step)

                        if (not os.path.exists(file_path_anc_step)) and (not os.path.exists(file_path_dst_step)):

                            time_from, time_to = parse_query_time(time_step, time_mode=var_type)
                            var_data = get_data_dams(var_tag, time_from, time_to, self.db_settings)

                            if var_data:

                                folder_name_anc_step, file_name_anc_step = os.path.split(file_path_anc_step)
                                make_folder(folder_name_anc_step)

                                write_obj(file_path_anc_step, var_data)
                                logging.info(' ------> Time Step ' + str(time_step) + ' ... DONE')
                            else:
                                logging.info(' ------> Time Step ' + str(time_step) +
                                             ' ... SKIPPED. Database request received an empty datasets')

                        elif (os.path.exists(file_path_anc_step)) and (not os.path.exists(file_path_dst_step)):
                            logging.info(' ------> Time Step ' + str(time_step) +
                                         ' ... SKIPPED. Ancillary file always exists.')
                        elif (not os.path.exists(file_path_anc_step)) and (os.path.exists(file_path_dst_step)):
                            logging.info(' ------> Time Step ' + str(time_step) +
                                         ' ... SKIPPED. Destination file always exists.')
                        else:
                            logging.error(' ===> Bad file multiple condition')
                            raise NotImplemented("File multiple condition not implemented yet")

                    logging.info(' -----> Variable ' + var_name + ' ... DONE')

                else:

                    logging.info(' -----> Variable ' + var_name + ' ... SKIPPED. '
                                                                  'Variable downloading is not activated.')

            else:

                logging.info(' -----> Variable ' + var_name + ' ... SKIPPED. Variable tag is null.')

        logging.info(' ----> Download datasets ... DONE')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize datasets
    def organize_data(self):

        logging.info(' ----> Organize datasets ... ')

        time_range = self.time_range
        dams_data = self.dams_collection

        file_path_anc_obj = self.file_path_anc_dset_obj
        file_path_dst_csv_obj = self.file_path_dst_csv_dset_obj
        file_path_dst_json_obj = self.file_path_dst_json_dset_obj

        var_dict = self.variable_dict
        var_fields_expected = self.file_fields_dst_dset

        flag_upd_dst = self.flag_updating_destination

        for var_name, var_fields in var_dict.items():

            logging.info(' -----> Variable ' + var_name + ' ... ')

            var_tag = var_fields['tag']
            var_type = var_fields['type']
            var_units = var_fields['units']
            var_valid_range = var_fields['valid_range']
            var_min_count = var_fields['min_count']
            var_scale_factor = var_fields['scale_factor']

            if var_tag is not None:

                file_path_anc_list = file_path_anc_obj[var_name]
                file_path_dst_csv_list = file_path_dst_csv_obj[var_name]
                file_path_dst_json_list = file_path_dst_json_obj[var_name]

                for time_step, file_path_anc_step, file_path_dst_csv_step, file_path_dst_json_step in zip(
                        time_range, file_path_anc_list, file_path_dst_csv_list, file_path_dst_json_list):

                    logging.info(' ------> Time Step ' + str(time_step) + ' ... ')

                    if flag_upd_dst:
                        if os.path.exists(file_path_dst_csv_step):
                            os.remove(file_path_dst_csv_step)

                    if (os.path.exists(file_path_anc_step)) and (not os.path.exists(file_path_dst_csv_step)):

                        var_data = read_obj(file_path_anc_step)

                        if var_data.__len__() > 0:

                            var_df = organize_data_dams(time_step, var_data, dams_data, data_type=var_type,
                                                        data_scale_factor=var_scale_factor,
                                                        data_min_count=var_min_count,
                                                        data_units=var_units, data_valid_range=var_valid_range)

                            if var_df is not None:
                                var_df = order_data(var_df, var_fields_expected)
                                # print(file_path_dst_csv_step)
                                # print(file_path_dst_json_step)
                                # MATTEO: add of the following two "if" statements in order to write csv file and/or json file with
                                # dam water level data.

                                # CSV:
                                if self.file_active_dst_csv:
                                    folder_name_dst_csv_dset, file_name_dst_csv_dset = os.path.split(file_path_dst_csv_step)
                                    make_folder(folder_name_dst_csv_dset)

                                    logging.info(
                                        ' ----> Saving dams water levels to csv file:' + str(file_path_dst_csv_step))
                                    write_file_csv(file_path_dst_csv_step, var_df)

                                # JSON:
                                if self.file_active_dst_json:
                                    folder_name_dst_json_dset, file_name_dst_json_dset = os.path.split(file_path_dst_csv_step)
                                    make_folder(folder_name_dst_json_dset)

                                    logging.info(
                                        ' ----> Saving dams water levels to json file:' + str(file_path_dst_json_step))
                                    # prepare dictionary with dam level data for each section:
                                    all_levels2json = write_file_json(var_df)
                                    # save dictionary to json file:
                                    json2dump_dams(all_levels2json, file_path_dst_json_step)

                                logging.info(' ------> Time Step ' + str(time_step) + ' ... DONE')

                            else:
                                logging.info(' ------> Time Step ' + str(time_step) +
                                             ' ... SKIPPED. Dumped datasets are null due to the applications of filters')
                        else:

                            logging.info(' ------> Time Step ' + str(time_step) + ' ... FAILED. ')
                            logging.warning(' ===> Data downloaded from database source service is null.')

                    elif (not os.path.exists(file_path_anc_step)) and (os.path.exists(file_path_dst_csv_step)):
                        logging.info(' ------> Time Step ' + str(time_step) +
                                     ' ... SKIPPED. Destination file always exists.')

                    elif (not os.path.exists(file_path_anc_step)) and (not os.path.exists(file_path_dst_csv_step)):
                        logging.info(' ------> Time Step ' + str(time_step) +
                                     ' ... SKIPPED. Variable is not activated or source datasets are empty.')

            else:

                logging.info(' -----> Variable ' + var_name + ' ... SKIPPED. Variable tag is null.')

        logging.info(' ----> Organize datasets ... DONE')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to clean temporary information
    def clean_tmp(self):

        file_path_anc = self.file_path_anc_dset_obj
        clean_tmp = self.flag_cleaning_tmp
        folder_name_anc_main = self.folder_name_anc_main

        if clean_tmp:

            # Remove tmp file and folder(s)
            for var_name, var_file_path_list in file_path_anc.items():
                for var_file_path_step in var_file_path_list:
                    if os.path.exists(var_file_path_step):
                        os.remove(var_file_path_step)
                    var_folder_name_step, var_file_name_step = os.path.split(var_file_path_step)
                    if var_folder_name_step != '':
                        if os.path.exists(var_folder_name_step):
                            if not os.listdir(var_folder_name_step):
                                os.rmdir(var_folder_name_step)

            # Remove empty folder(s)
            folder_name_anc_list = list_folder(folder_name_anc_main)
            for folder_name_anc_step in folder_name_anc_list:
                if os.path.exists(folder_name_anc_step):

                    file_name_tmp = os.listdir(folder_name_anc_step)
                    if file_name_tmp:
                        for file_name_step in file_name_tmp:
                            file_path_step = os.path.join(folder_name_anc_step, file_name_step)
                            if os.path.exists(file_path_step):
                                os.remove(file_path_step)

                    os.rmdir(folder_name_anc_step)

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------