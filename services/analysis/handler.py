	

import json
import sys
sys.path.append('/var/task/service')
from analyzeService import *

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
                                        
def analysis(event, context):
    print(event)
    resource = event['resource']

    if resource == "/anomal":
        result = getAnomally()
    elif resource == '/plot':
        makePlot()
        result = 'success'
    elif resource == '/graph':
        makeGraph()
        result = 'success'
    elif resource == '/group':
        data = getGroupInfo()
        result = json.dumps(data, cls=NumpyEncoder)
        response = {
        "statusCode": 200,
        "body": result
        }
        return response
    
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response