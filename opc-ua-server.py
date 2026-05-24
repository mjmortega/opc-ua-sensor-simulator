#Simple Python OPC-UA Server
#Sending out 2 data values
#Flo Pachinger / flopach, Cisco Systems, July 2020
#Script based on the server example https://github.com/FreeOpcUa/python-opcua
#LGPL-3.0 License

import logging
import asyncio
import pandas as pd

from asyncua import ua, Server
from asyncua.common.methods import uamethod
# --- 1. IMPORT SQLITE HISTORIAN ---
from asyncua.server.history_sql import HistorySQLite

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')

@uamethod
def func(parent, value):
    return value * 2

async def main():
    # setup our server
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://127.0.0.1:4840/opcua/')
    server.set_server_name("DevNet OPC-UA Test Server")

    # setup our own namespace, not really necessary but should as spec
    uri = 'http://devnetiot.com/opcua/'
    idx = await server.register_namespace(uri)

    # --- 2. INITIALIZE THE HISTORIAN SYSTEM ---
    historian = HistorySQLite("history.db")
    await historian.init()
    server.iserver.history_manager.set_storage(historian)

    # populating our address space
    # server.nodes, contains links to very common nodes like objects and root
    obj_vplc = await server.nodes.objects.add_object(idx, 'vPLC1')
    var_temperature = await obj_vplc.add_variable(ua.NodeId('temperature', idx), 'temperature', 0.0)
    var_pressure = await obj_vplc.add_variable(ua.NodeId('pressure', idx), 'pressure', 0.0)
    var_pumpsetting = await obj_vplc.add_variable(ua.NodeId('pumpsetting', idx), 'pumpsetting', "")

    # Read Sensor Data from Kaggle
    df = pd.read_csv("sensor.csv")
    # Only use sensor data from 03 and 01 (preference)
    sensor_data = pd.concat([df["sensor_03"], df["sensor_01"]], axis=1)

    _logger.info('Starting server!')
    async with server:
        # --- 3. BEGIN HISTORIZATION TRACKING ON THE NODES ---
        # Moving this inside the running server context ensures internal engine sub-loops are active
        await server.iserver.history_manager.historize_data_change(var_temperature, period=None, count=0)
        await server.iserver.history_manager.historize_data_change(var_pressure, period=None, count=0)
        await server.iserver.history_manager.historize_data_change(var_pumpsetting, period=None, count=0)

        # run forever and iterate over the dataframe
        while True:
            for row in sensor_data.itertuples():
                # check if row values are valid (handling potential NaNs)
                val_temp = float(row[1]) if not pd.isna(row[1]) else 0.0
                val_press = float(row[2]) if not pd.isna(row[2]) else 0.0

                # if below the mean use different setting - just for testing
                if val_temp < df["sensor_03"].mean():
                    setting = "standard"
                else:
                    setting = "speed"

                # Writing Variables (Historian records automatically upon write)
                await var_temperature.write_value(val_temp)
                await var_pressure.write_value(val_press)
                await var_pumpsetting.write_value(str(setting))
                await asyncio.sleep(1)

if __name__ == '__main__':
    #python 3.7 onwards
    asyncio.run(main())