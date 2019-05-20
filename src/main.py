from flask import Flask
app = Flask(__name__)

@app.route("/gmd/api/disease/")
def hello():
    return "Hello World!"