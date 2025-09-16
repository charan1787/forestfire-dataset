import pickle
from flask import Flask,request,jsonify,render_template
import numpy as np
import pandas as pf
from sklearn.preprocessing import StandardScaler
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "..", "..", "templates")

application = Flask(__name__, template_folder=TEMPLATE_DIR)
app=application

## import ridge regressor and standard scaler pickle
ridge_model=pickle.load(open('models/ridge.pkl','rb'))
standard_scaler=pickle.load(open('models/scaler.pkl','rb'))

@app.route("/")
def index():
    return render_template('index.html')

@app.route('/predictdata',methods=['GET','POST'])
def predict_datapoint():
    if request.method=="POST":
        Temperature=float(request.form.get('Temperature'))
        RH = float(request.form.get('RH'))
        Ws = float(request.form.get('Ws'))
        Rain = float(request.form.get('Rain'))
        FFMC = float(request.form.get('FFMC'))
        DMC = float(request.form.get('DMC'))
        ISI = float(request.form.get('ISI'))
        Classes = float(request.form.get('Classes'))
        Region = float(request.form.get('Region'))

        new_data_scaled=standard_scaler.transform([[Temperature,RH,Ws,Rain,FFMC,DMC,ISI,Classes,Region]])
        result=ridge_model.predict(new_data_scaled)

        return render_template('home.html',results=result[0])

        
    else:
        return render_template('home.html')

@app.route("/predict", methods=["POST"])
def predict_api():
    try:
        data = request.get_json()
        features = [
            data.get("Temperature", 0),
            data.get("RH", 0),
            data.get("Ws", 0),
            data.get("Rain", 0),
            data.get("FFMC", 0),
            data.get("DMC", 0),
           
            data.get("ISI", 0),
      
            data.get("Classes", 0),
            data.get("Region", 0),
        ]
        scaled = standard_scaler.transform([features])
        result = ridge_model.predict(scaled)[0]

        return jsonify({"prediction": float(result)})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__=="__main__":
    app.run(debug = True,host="0.0.0.0",port = 5000)
