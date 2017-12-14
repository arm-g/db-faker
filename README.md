# db-faker
You have a private customer data in your database but cannot run tests on it because it is not save to export data and give it to developers and QAs. Here comes the sollution which is to fake your customer's data and use the database to run test on the same amount of data.
The faker project fakes your relational DB data based on your DB's schema(s) definitions.
db-faker uses pre-defined data types which uses 
[python faker](https://pypi.python.org/pypi/Faker) module.
Meanwhile, it allows you to define your own methods in ```data_types.py```.
Currently, db-faker supports
```fake.profile(fields=None, sex=None)
# {   'address': '03147 Wu Drive\nMatthewmouth, WV 06927',
#     'birthdate': '1978-07-13',
#     'blood_group': '0-',
#     'company': 'Stanley PLC',
#     'longitude': (Decimal('89.8079965'), Decimal('-154.729802')),
#     'latitude': (Decimal('89.8079965'), Decimal('-154.729802')),
#     'job': 'Chief Financial Officer',
#     'mail': 'jacquelinedickerson@gmail.com',
#     'name': 'David Palmer',
#     'gender': 'male/female',
#     'ssn': '736-81-1031',
#     'username': 'sharonpatrick',
#     'website': ['http://bruce.com/']}
#     'hash': some_128bit_hash,
#     'null_value': None}
```

This version only supports Postgress but the best is still yet to come so stay tuned.

## Getting Started

### Prerequisites

* ```python 2.7.*```
* ```pip```

### Installing

* ```git clone https://github.com/arm-g/db-faker.git```
* ```pip install -r requirements.txt```
* ```cp connection.sample.json connection.json``` and fill the conneciton params
* ```cp schema_definition.sample.json connection.json``` and fill the schema structure

## Usage example

* ```./db-faker.py run``` will check wheter the defintions are correct and will start the faking process.

* ```./db-faker.py cleanup``` Cleanup db structure changes after faking process.

## Release History
* 0.1.0
    * The first proper release
