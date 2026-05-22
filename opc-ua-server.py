import logging
import asyncio
import pandas as pd

from asyncua import ua, Server
from asyncua.common.methods import uamethod
# --- 1. IMPORT THE HISTORIAN SYSTEM ---
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

    # --- 2. INITIALIZE THE HISTORIAN SERVICE ---
    # This stores the history in an SQLite database file named 'history.db'.
    # If you prefer a temporary in-memory database, use HistorySQLite(":memory:") instead.
    historian = HistorySQLite("history.db")
    await historian.init()
    server.iserver.history_manager.set_storage(historian)

    # setup our own namespace, not really necessary but should as spec
    uri = 'http://devnetiot.com/opcua/'
    idx = await server.register_namespace(uri)

    # populating our address space
    obj_vplc = await server.nodes.objects.add_object(idx, 'vPLC1')
    var_temperature = await obj_vplc.add_variable(idx, 'temperature', 0.0)
    var_pressure = await obj_vplc.add_variable(idx, 'pressure', 0.0)
    var_pumpsetting = await obj_vplc.add_variable(idx, 'pumpsetting', "standard")

    # --- 3. REGISTER VARIABLES FOR HISTORIZATION ---
    # In asyncua, use 'server.iserver.history_manager.historize_data_change'
    # period=None tracks everything indefinitely, count=0 means no max limit limit.
    await server.iserver.history_manager.historize_data_change(var_temperature, period=None, count=0)
    await server.iserver.history_manager.historize_data_change(var_pressure, period=None, count=0)
    await server.iserver.history_manager.historize_data_change(var_pumpsetting, period=None, count=0)

    # Read Sensor Data from Kaggle
    df = pd.read_csv("sensor.csv")
    # Only use sensor data from 03 and 01 (preference)
    sensor_data = pd.concat([df["sensor_03"], df["sensor_01"]], axis=1)

    _logger.info('Starting server!')
    async with server:
        # run forever and iterate over the dataframe
        while True:
            for row in sensor_data.itertuples():
                # check if row values are valid (handling potential NaNs)
                val_temp = float(row[1]) if not pd.isna(row[1]) else 0.0
                val_press = float(row[2]) if not pd.isna(row[2]) else 0.0

                if val_temp < df["sensor_03"].mean():
                    setting = "standard"
                else:
                    setting = "speed"

                # Writing Variables (historian intercepts this automatically)
                await var_temperature.write_value(val_temp)
                await var_pressure.write_value(val_press)
                await var_pumpsetting.write_value(str(setting))
                await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())