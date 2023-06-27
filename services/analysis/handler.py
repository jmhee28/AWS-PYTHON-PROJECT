	

import json
import pandas as pd
from scipy.stats import zscore
from service.analyzeService import *

def analysis(event, context):

    if event.resource == "anomal":
        result = getAnomally()
        
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response