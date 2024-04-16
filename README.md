# Database

Tested on Python 3.11

### Create VirtualENV

~~~ bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
~~~

### Develop
~~~ bash
source venv/bin/activate
uvicorn app.main:app --reload
deactivate
~~~

### Run
~~~ bash
source venv/bin/activate
uvicorn app.main:app --host 127.0.0.1 --port 8000
deactivate
~~~