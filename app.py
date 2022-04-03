from flask import Flask, jsonify, Response
from flask import make_response
from flask import request
import mysql.connector
from datetime import date, timedelta, datetime
from credentials import USERNAME, PASSWORD
from flask import abort
import sys

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


#################################################################

def unauthorized():
    return Response(response="Unauthorized", status=401)

def authorized(request):
    if request.authorization and request.authorization.username in USERNAME and request.authorization.password in PASSWORD:
        return True
    return False

def get_timestamp(request):
    timestamp = request.args.get("timestamp")
    if timestamp is None:
        today = date.today()
        date_string = today.strftime("%Y-%m-%d 00:00:00")
    else:
        date_string = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    return date_string
        

def retrieve_data(query):
    output = []
    output_dict = {}
    cnxn = mysql.connector.connect(user="sw_sje_crd", password="commonP@ssw0rd", host="131.202.94.119", database="sje_crd")
    cursor = cnxn.cursor()
    cursor.execute(query)
    forecast_rec = cursor.fetchall()
    cursor.close()
    cnxn.close()
    length_days = len(forecast_rec)
    for i in range(length_days):
        output_dict[str(forecast_rec[i][2])] = float(forecast_rec[i][3])
    app.logger.debug(output_dict) 
    return output_dict
#################################################################

@app.route('/unb_api/v1.0/forecastunb01da', methods=['GET'])
def get_forecastunb01day():
    
    if not authorized(request):
        return unauthorized()
        
    query = "SELECT * FROM forecastunb01da WHERE timestamp >= " + "'" + get_timestamp(request) + "'"
    return jsonify({'task': retrieve_data(query)})
   
##############################################################################
@app.route('/unb_api/v1.0/forecastunb04da', methods=['GET'])
def get_forecastunb04da():
    
    if not authorized(request):   
        return unauthorized()
    
    query = "SELECT * FROM forecastunb04da WHERE timestamp >= " + "'" + get_timestamp(request) + "'"
    return jsonify({'task': retrieve_data(query)})
  
##############################################################################
@app.route('/unb_api/v1.0/forecastunb01ma', methods=['GET'])
def get_forecastunb01ma():
    
    if not authorized(request):
        return unauthorized()
    
    query = "SELECT * FROM forecastunb01ma WHERE timestamp >= " + "'" + get_timestamp(request) + "'"
    return jsonify({'task': retrieve_data(query)})
    
##############################################################################
@app.route('/unb_api/v1.0/forecastunb01wa', methods=['GET'])
def get_forecastunb01wa():
    
    if not authorized(request):
        return unauthorized()

    query = "SELECT * FROM forecastunb01wa WHERE timestamp >= " + "'" + get_timestamp(request) + "'"
    return jsonify({'task': retrieve_data(query)})
    
##############################################################################
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)