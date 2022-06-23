#$env:FLASK_APP = "app"
from flask import Flask, request, render_template, redirect
from flask_wtf import FlaskForm
from flask_bootstrap import Bootstrap
from wtforms import StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired
import pandas as pd 
import json
import io
import contextlib
app = Flask(__name__)
Bootstrap(app)
app.config['SECRET_KEY'] = 's5TDW9C2ao'
PATH_TO_DATA = "./data.csv"

from custom_map_assignment import CustomDFAssigner

df = pd.read_csv(PATH_TO_DATA)

from wtforms.widgets.core import TextArea
class MyTextArea(TextArea):
    def __init__(self,**kwargs):
        self.kwargs = kwargs

    def __call__(self, field, **kwargs):
        for arg in self.kwargs:
            if arg not in kwargs:
                kwargs[arg] = self.kwargs[arg]
        return super(MyTextArea,self).__call__(field,**kwargs)

class MapForm(FlaskForm):
    mapping = TextAreaField('Mapping:', validators=[DataRequired()], widget = MyTextArea(rows = 10, cols = 12))
    submit = SubmitField('Submit')

@app.route("/", methods=['GET', 'POST'])
def hello_world():
    form = MapForm()
    message = ""
    if form.validate_on_submit():
        mapping = json.loads(form.mapping.data)
        try:
            with contextlib.redirect_stderr(io.StringIO()) as f:
                if isinstance(mapping, list):
                    if len(mapping):
                        message = CustomDFAssigner("API").map_validator(df.copy(), mapping)
                    else:
                        raise Exception("No mapping given")
                elif isinstance(mapping, dict):
                    target = []
                    target.append(mapping)
                    if len(target) > 0:
                        message = CustomDFAssigner("API").map_validator(df.copy(), target)
                    else:
                        raise Exception("No mapping given")
                else:
                    message = "Error: mapping couldn't be used"
            if message is None: message = f.getvalue().split('\n')[-3:-1]
        except AssertionError as err:
            return f"Unexpected Error\n{str(err)}"
    return render_template('index.html', form=form, message=message)

@app.route("/run_mapping", methods=["POST"])
def run_mapping():
    global df
    try:
        mapping = request.json
        if isinstance(mapping, list):
            if len(mapping):
                return (CustomDFAssigner("API").custom_assignment_processor(df.copy(), mapping)).to_csv(index=False)
            else:
                raise Exception("No mapping given")
        elif isinstance(mapping, dict):
            target = []
            target.append(mapping)
            if len(target):
                return (CustomDFAssigner("API").custom_assignment_processor(df.copy(), target)).to_csv(index=False)
            else:
                raise Exception("No mapping given")
        else:
            return {"error": "Error: mapping couldn't be used"}, 415
    except Exception as err:
        return f"Unexpected Error\n{str(err)}"

@app.route("/validate_mapping", methods=["POST"])
def validate():
    global df
    try:
        mapping = request.json
        if isinstance(mapping, list):
            if len(mapping):
                return CustomDFAssigner("API").map_validator(df.copy(), mapping)
            else:
                raise Exception("No mapping given")
        elif isinstance(mapping, dict):
            target = []
            target.append(mapping)
            if len(target) > 0:
                return CustomDFAssigner("API").map_validator(df.copy(), target)
            else:
                raise Exception("No mapping given")
        else:
            return {"error": "Error: mapping couldn't be used"}, 415
    except AssertionError as err:
        return f"Unexpected Error\n{str(err)}"


@app.route("/set_data", methods=["POST"])
#Just an idea, need to test
def set_df():
    global df
    new_data = (request.data).decode("utf-8")
    print(new_data)
    readable_data = io.StringIO(new_data)
    df = pd.read_csv(readable_data)
    return "Data has been set"

@app.route("/reset_data", methods=["GET"])
def reset_df():
    global df
    df = pd.read_csv(PATH_TO_DATA)
    return "Data has been reset"

@app.route("/view_data", methods=["GET"])
def view_df():
    global df
    return df.to_csv(index=False)