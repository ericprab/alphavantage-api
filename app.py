# save this as app.py
from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello, World!"

@app.route("/api/dailyeod")
def hello():
    return "Hello, dailyeod!"

@app.route("/api/monthlycpi")
def hello():
    return "Hello, monthlycpi!"

if __name__ == "__main__":
    app.run(debug=True)