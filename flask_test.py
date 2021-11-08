from flask import Flask
from flask import request
from flask import render_template

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST']) #using decorator to link to webpage which matching the feature
def home():
    #return '<h1>Home</h1>'
    return render_template('home.html')

@app.route('/login', methods=['GET'])
def login_from():
    #return '''<form action="/login" method="POST">
    #<p><input name="username"></p>
    #<p><input name="password"></p>
    #<p><button type="submit">Sign In</button></p>
    #</form>'''
    return render_template('form.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']
    if username =='admin' and password =='pwd':
        return render_template('login-ok.html',username=username)
        #return '<h3>Hello, admin</h3>'
    else:
        return render_template('form.html', message='Bad Request', username=username)
        #return '<h3>ERROR!</h3>'


if __name__ == '__main__':
    app.run()

