	

import json
import pandas as pd
from scipy.stats import zscore
from service.analyzeService import *

def analysis(event, context):
    resource = event.resource
    if resource == "/anomal":
        result = getAnomally()
    elif resource == '/plot':
        makePlot()
        result = 'success'
    elif resource == '/graph':
        makeGraph()
        result = 'success'
    elif resource == 'group':
        result = getGroupInfo()
        
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response