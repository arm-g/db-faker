from faker import Faker


class Data(object):
    def __init__(self):
        self.faker = Faker()

    def get_types(self):
        fake_profile = self.faker.profile()
        types = {
            'name': fake_profile['name'],
            'first_name': fake_profile['name'].split(' ')[0],
            'last_name': fake_profile['name'].split(' ')[1],
            'full_name': self.faker.first_name() + self.faker.last_name(),
            'email': fake_profile['mail'],
            'hash': self.hash(),
            'phone': self.faker.phone_number(),
            'gender': 'male' if fake_profile['sex'] == 'M' else 'female',
            'company': fake_profile['company'],
            'job': fake_profile['job'],
            'latitude': fake_profile['current_location'][0],
            'longitude': fake_profile['current_location'][1],
            'address': fake_profile['address'],
            'birth_date': fake_profile['birthdate'],
            'null_value': None,
            'half_bool': self.faker.boolean(chance_of_getting_true=50)
        }
        return types

    def hash(self):
        import random
        return random.getrandbits(128)
