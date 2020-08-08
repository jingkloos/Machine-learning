### set up virtual env (windows):
1. set up myenv folder
    python -m venv c:\path\to\myenv
    replace c:\path\to\myenv to your path to your virtual env
2. activate myenv
    .\env\Scripts\activate
3. install packages in requirements.txt
    pip install -r requirements.txt

### run the app
you need to pass at least 2 arguments
steam data or not -- Y/N
how long to stream in seconds -- int number
what topics you want to stream (optional) -- key words like covid trump etc.

python main.py arg1 arg2 *args


