#!/usr/bin/env python2

"""
cbackup collector

1. add /var/cbackup/storage/$SERVERNAME/monthly
2. search last lxd, mysql, files for each month & year
3. copy to /monthly
4. copy to /yearly
5.* remove backups older than 30 days

"""

import os
import re
from datetime import datetime
from itertools import groupby
from shutil import copy2


BASE_DIR = '/var/cbackup/storage'


def add_directory(root_path, name):
    """
    Creates folder. Handle exception in case of folder already exists.
    :param root_path: [STRING].
    :param name: [STRING].
    :return: None.
    """
    folder_path = os.path.join(root_path, name)
    try:
        os.mkdir(folder_path)
    except OSError:
        # print('{} already exists'.format(folder_path))
        pass


def find_max_date_in_month(dates_list):
    """
    Get max dates for each month. Skip dates for current month.
    :param dates_list: [LIST]. Like: ['2017-01-01--02-11-00',].
    :return: [LIST].
    """
    current_month = datetime.now().strftime('%m')
    result = []
    for _, group in groupby(sorted(dates_list), lambda x: x[:7]):
        last_date = max(group)
        if last_date[5:7] < current_month:
            result.append(last_date)
        else:
            print 'Skipped. Date from current month for "find_max_date_in_month":', last_date

    return result


def find_max_date_in_year(dates_list):
    """
    Get max dates for each year. Skip dates for current year.
    :param dates_list: [LIST]. Like: ['2017-01-01--02-11-00',].
    :return: [LIST].
    """
    current_year = datetime.now().strftime('%Y')
    result = []
    for _, group in groupby(sorted(dates_list), lambda x: x[:4]):
        last_date = max(group)
        if last_date[:4] < current_year:
            result.append(last_date)
        else:
            print 'Skipped. Date from current year for "find_max_date_in_year":', last_date

    return result


def get_backups(backup_paths, search_func):
    """
    Retrieve backups with max date for each month.
    :param backup_paths: [LIST].
    :param search_func: [FUNCTION].
    :return: [LIST].
    """
    backups_dict = {}
    for file_path in backup_paths:
        file_name = file_path.split('/')[-1]
        file_name_splited = file_name.split('_')

        if file_name_splited[1] not in backups_dict:
            backups_dict[file_name_splited[1]] = [file_name_splited[0]]
        else:
            backups_dict[file_name_splited[1]].append(file_name_splited[0])

    result = []
    for k, v in backups_dict.items():
        result.extend(["{}_{}".format(date, k) for date in search_func(v)])

    return result


def retrieve_full_backups(backup_names):
    """
    Discard interim increment backups.
    :param backup_names: [LIST].
    :return: [LIST].
    """
    result = []
    for i in get_backups(backup_names, find_max_date_in_month):

        # discard date in file name
        backup_name = i.split('_')[1]
        # prepare filter
        pattern = r'^\w+.\d{2}[.\w]+$'
        p = re.compile(pattern)

        # distinct full increment backups
        if p.match(backup_name):
            if '00' in backup_name:
                # add full backup
                result.append(i)
            else:
                print 'Skip increment backup\t', i  # log
        # not increment backups
        else:
            result.append(i)
    return result


def copy_backups(data, server_name, folder):
    backup_dst = os.path.join(BASE_DIR, server_name, folder)
    for backup_src in data:
        print backup_src, backup_dst  # log
        copy2(backup_src, backup_dst)


def normalize_storage(path, level=0, server_name=None):

    files = []

    for child in os.listdir(path):

        child_path = os.path.join(path, child)

        if os.path.isdir(child_path) and 'monthly' not in child_path and 'yearly' not in child_path:
            # BASE_DIR/$SERVER_NAME level
            if level == 0:
                # add monthly,yearly folders
                add_directory(child_path, 'monthly')
                add_directory(child_path, 'yearly')
                # remember $SERVER_NAME
                server_name = child

            server_files = normalize_storage(child_path, 1, server_name)

            if server_files:

                # copy to monthly folder
                print '\t\t-= Copy monthly backups =-'  # log
                monthly_backups = [
                    os.path.join(child_path, backup) for backup in retrieve_full_backups(server_files)]
                copy_backups(monthly_backups, server_name, 'monthly')

                # copy to yearly folder
                print '\t\t-= Copy yearly backups =-'  # log
                yearly_backups = [
                    os.path.join(child_path, backup) for backup in get_backups(monthly_backups, find_max_date_in_year)]
                copy_backups(yearly_backups, server_name, 'yearly')

        if not os.path.isdir(child_path):
            files.append(child_path)

    return files


if __name__ == '__main__':
    normalize_storage(BASE_DIR)
