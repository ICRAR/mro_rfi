#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""

"""
import argparse
import logging
import sys
from os import listdir
from os.path import abspath, exists, join, split

from configobj import ConfigObj
from sqlalchemy import create_engine

from tables import METADATA, RAW_TRAIN_DATA

LOG = logging.getLogger(__name__)


def parser_arguments():
    path_dirname, _ = split(abspath(__file__))
    config_file_name = join(path_dirname, 'mro_rfi.settings')

    parser = argparse.ArgumentParser()
    parser.add_argument('--display_python_path', action='store_true', help='Show the Python path', default=True)
    parser.add_argument('-v', '--verbose', action='count', help='The verbosity level', default=1)
    parser.add_argument('--settings_file', help='The settings file', default=config_file_name)
    parser.add_argument('data_directory', help='The settings file')

    if len(sys.argv[1:]) == 0:
        return parser.parse_args(['-h'])
    return parser.parse_args()


def load_data(**keywords):
    for arg in ['database_user',
                'database_password',
                'database_hostname',
                'database_name',
                'data_directory',
                ]:
        if keywords.get(arg) is None:
            raise RuntimeError('Missing the keyword {0}'.format(arg))

    database_login = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(
        keywords['database_user'],
        keywords['database_password'],
        keywords['database_hostname'],
        keywords['database_name']
    )
    engine = create_engine(database_login)
    METADATA.create_all(engine)

    insert = RAW_TRAIN_DATA.insert()
    for file in listdir(keywords['data_directory']):
        connection = engine.connect()
        transaction = connection.begin()

        with open(file, "r") as input_file:
            for row in input_file:
                elements = row.split('|')
                connection.execute(
                    insert.values(
                        date=elements[0],
                        column_2=elements[1],
                        latitude=elements[2],
                        longitude=elements[3],
                    )
                )

        transaction.commit()


if __name__ == "__main__":
    args = parser_arguments()

    # Check the settings file exists
    if not exists(args.settings_file):
        raise RuntimeError('No configuration file {0}'.format(args.settings_file))

    # Configure the logging levels
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.verbose)]  # capped to number of levels
    logging.basicConfig(level=level, format='%(asctime)-15s:' + logging.BASIC_FORMAT)

    if args.display_python_path:
        LOG.info('PYTHONPATH = {0}'.format(sys.path))

    keyword_dictionary = vars(args)
    keyword_dictionary.update(ConfigObj(args.settings_file_name))

    load_data(**keyword_dictionary)
