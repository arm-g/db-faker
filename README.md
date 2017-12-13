# db-faker

The faker project fakes your relational DB data based on your DB's schema(s) definitions.

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
