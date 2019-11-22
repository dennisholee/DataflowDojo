# Postgresql Environment

Get Postgresql Docker image
```bash
docker pull postgres
```

Start a Postgresql instance

```bash
docker run -p 5432:5432 --name dataflow-postgres -e POSTGRES_PASSWORD=secret -d postgres
```

# Virtual environment
```bash
virtualenv -p /path/to/python_3.7 venv
source venv/bin/activate
```

# Install dependencies
```bash
pip install -r requirements.txt
```

# Execute
```bash
python dataflow.py
```

# Libraries

* [Beam nuggets](https://github.com/mohaseeb/beam-nuggets)

