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
uvicorn main:app --reload
~~~

### Run
~~~ bash
uvicorn main:app --host 0.0.0.0 --port 80
~~~