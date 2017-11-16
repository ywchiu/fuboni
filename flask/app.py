from flask import Flask,jsonify
from sklearn.externals import joblib
app=Flask(__name__)

@app.route("/<area>")
def getPrice(area):
	clf = joblib.load('model.pkl')
	predicted = clf.predict(int(area))
	return jsonify({'data' : predicted[0]})

if __name__=="__main__":
	app.run()