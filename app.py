from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

#import main
import os
import gc
from resume_parser import resumeparse

# app
app = Flask(__name__)


# routes
@app.route('/', methods=['GET'])
def first_page():
    return render_template('parser.html')


@app.route('/parser', methods=['GET', 'POST'])
def upload_file():
    gc.collect()
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))
        data = resumeparse.read_file(f.filename)
        os.remove(f.filename)
        return data


if __name__ == '__main__':
    app.run(port=5000, debug=True)
