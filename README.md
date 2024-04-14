# Database

Tested on Python 3.11

TODO: Generate Docker image

### Create VirtualENV

~~~ bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
~~~

### Develop
~~~ bash
uvicorn app.main:app
~~~

### Run
~~~ bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
~~~