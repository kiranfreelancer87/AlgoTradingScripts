import datetime
import json
import os
import threading
from os.path import exists
import pandas as pd
import websocket

# now if you use logger it will not log to console.

try:
    import thread
except ImportError:
    import _thread as thread
import time

endpoint = "wss://nimblewebstream.lisuns.com:4576/"
apikey = "6d3bad57-0e07-42d1-8255-87d1bb09c06c"


def Authenticate(ws):
    ws.send('{"MessageType":"Authenticate","Password":"' + apikey + '"}')


def GetHistory(ws, InstIdentifier='RELIANCE'):
    ExchangeName = "NSE"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    From = int((datetime.datetime.now() - datetime.timedelta(
        days=2000)).timestamp())  # GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    To = int(datetime.datetime.now().timestamp())  # GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    isShortIdentifier = "true"

    strMessage = {'periodicity': 'MINUTE', 'MessageType': 'GetHistory', 'Exchange': ExchangeName,
                  'InstrumentIdentifier': InstIdentifier, 'Period': 15,
                  'From': From, 'To': To, 'UserTag': 'BN',
                  'isShortIdentifier': isShortIdentifier}

    ws.send(json.dumps(strMessage))


currentPos = 0

symbolList = pd.read_csv('ind_nifty500list.csv')['Symbol'].to_list()


def on_message(ws, message):

    global currentPos

    global symbolList

    try:
        msg = json.loads(message)
        if 'Complete' in msg and 'MessageType' and msg and msg['MessageType'] == 'AuthenticateResult':
            print("Authentication Completed")
            GetHistory(ws, symbolList[currentPos])
        print(msg)
    except Exception as e:
        print(e)

    if len(str(message)) > 1000:
        with open('data/{}.json'.format(symbolList[currentPos]), 'w') as sf:
            sf.write(json.dumps(json.loads(message)['Result']))
            sf.close()
            GetHistory(ws, symbolList[currentPos])
            currentPos = currentPos + 1


def on_error(ws, error):
    print("Error")


def on_close(ws):
    print("Reconnecting...")
    websocket.setdefaulttimeout(30)
    ws.connect(endpoint)


def on_open(ws):
    # print("Connected...")
    def run(*args):
        time.sleep(1)
        print("Auth..............")
        Authenticate(ws)

    thread.start_new_thread(run, ())


websocket.enableTrace(True)

ws = websocket.WebSocketApp(endpoint,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()