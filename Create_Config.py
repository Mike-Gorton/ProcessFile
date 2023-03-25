#!/usr/bin/python

import configparser

config = configparser.ConfigParser()

config.add_section('configuration')

config['configuration']['filelocation'] = '/nas/node-red/ghq'
config['configuration']['user'] = 'pi'
config['configuration']['passwd'] = 'pi'

config.add_section('database')

config['database']['db'] = 'IoT'

with open('IoT.ini', 'w') as configfile:
    config.write(configfile)