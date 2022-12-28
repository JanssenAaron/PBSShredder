# PBSShredder

### Running the shredder
##### Clone repo
```
git clone https://github.com/JanssenAaron/PBSShredder.git
cd PBSShredder
```
##### Install required python modules
```
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

##### Edit shred.py
```
# Installation parameters
accounting_file_location = "{accounting log directory}"
connection_string="{connection engine (postgresql)}://{user}:{password}@{hostname}/{database name}"
```

##### Run shredder
```
python shred.py
```
