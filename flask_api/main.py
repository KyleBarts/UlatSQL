from flask import Flask, request, jsonify, abort, json
from flask_restful import Resource, Api
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd

app = Flask(__name__)
api = Api(app)

def build_response(data):
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response



def check_getEN_inputs(args):
    en_inputs = {}
    if 'start_date' not in args:
        abort(400, 'Please supply start_date')
    else:
        str_start_date = args.get('start_date')
        start_date = datetime.strptime(str_start_date, '%Y-%m-%d %H:%M:%S')
        print(start_date)
        en_inputs['start_date'] = start_date

    if 'end_date' not in args:
        abort(400, 'Please supply end_date')
    else:
        str_end_date = args.get('end_date')
        end_date = datetime.strptime(str_end_date, '%Y-%m-%d %H:%M:%S')
        print(end_date)
        en_inputs['end_date'] = end_date
        int_end_month = end_date.month
    print('months below')
    filename_list = []
    current_date = start_date
    current_date = current_date.replace(day=1)
    delta = timedelta(days=30)
    while current_date <= end_date:
        print(current_date)
        print(current_date.month)
        
        current_month = current_date.strftime('%m')
        current_year = current_date.strftime('%Y')
        filename_format = f'../pagasa_data/outputs/lightning_data_{current_month}_{current_year}.csv'
        filename_list.append(filename_format)
        current_date = current_date + relativedelta(months=+1)
    # for month in range(int_start_month,int_end_month+1):
    en_inputs['files'] = filename_list
    #     print(month)
    return en_inputs


@app.route('/earthnetworks', methods=['GET'])
def fetch_earthnetworks_data():

    user_input = check_getEN_inputs(request.args)
    print(user_input)

    files_to_read = user_input['files']
    df_from_each_file = (pd.read_csv(f) for f in files_to_read)
    concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)

    cg_df = concatenated_df[(concatenated_df.flash_type==0)]
    cg_df['lightning_time'] = pd.to_datetime(cg_df['lightning_time'], format='%Y-%m-%d %H:%M:%S')
    
    datefilter = cg_df[((cg_df.lightning_time>user_input.get('start_date')) & (cg_df.lightning_time<user_input.get('end_date')))]

    datefilter = datefilter.reset_index(drop=True)
    datefilter = datefilter.drop(columns=['Unnamed: 0', 'id'])


    print(datefilter)
    
    data_response= datefilter.to_json(orient="records")
    
    
    data_response = build_response(data_response)
    
    return data_response

if __name__ == '__main__':
    app.run(debug=True,port=8080)