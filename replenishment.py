import requests
from urllib.parse import urljoin
import csv
import psycopg2
import numpy as np
import configparser
import boto3
import json

# Set authentication and server parameters
# Refer here for auth servers and api endpoints: https://manual.firstresonance.io/api/access-tokens
API_URL = 'https://api.ion-gov.com'
AUTHENTICATION_SERVER = 'auth.ion-gov.com'

CREATE_KIT_MUTATION = '''
    mutation createPartKit($input: CreatePartKitInput!) {
        createPartKit(input: $input) {
            partKit {
                id
            }
        }
    }
'''

CREATE_KIT_ITEM_MUTATION = '''
    mutation createPartKitItem($input: CreatePartKitItemInput!) {
        createPartKitItem(input: $input) {
            partKitItem {
                id
            }
        }
    }
'''


class KitRequest:

    def __init__(self, assigned_to_id, delivery_location):
        input('Begin Replenishment?')
        self.api_creds = self.grab_creds('ion/api/creds')
        self.db_creds = self.grab_creds('ion/db/psql')
        self.access_token = self.get_access_token()
        self.part_kit_id = None
        self.inventory_config = self.get_csv_data()
        self.current_inventory_dict = self.get_current_inventory()
        self.assigned_to_id = assigned_to_id
        self.delivery_location_id = delivery_location

    def grab_creds(self, sec_id: any):
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=sec_id)
        database_secrets = json.loads(response['SecretString'])
        return database_secrets

    def get_access_token(self):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.api_creds['clientId'],
            'client_secret': self.api_creds['clientSecret'],
            'audience': API_URL
        }

        headers = {'content-type': 'application/x-www-form-urlencoded'}

        auth_url = urljoin(f'https://{AUTHENTICATION_SERVER}', '/auth/realms/api-keys/protocol/openid-connect/token',
                           'oauth/token')
        res = requests.post(auth_url, data=payload, headers=headers)
        if res.status_code != 200:
            raise RuntimeError('An error occurred in the API request')
        return res.json()['access_token']

    def get_csv_data(self):
        """Import csv data from file."""
        csv_data = []
        with open('part_qty_config.csv', newline='') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            columns = []
            for row_number, row in enumerate(reader):
                row_object = {}
                if row_number == 0:
                    columns = row
                    continue
                for index, value in enumerate(row):
                    row_object[columns[index]] = value
                csv_data.append(row_object)
        return csv_data

    def call_api(self, query, variables):
        """Calls ions GraphQL api."""
        headers = {
            'Authorization': f'{self.access_token}',
            'Content-Type': 'application/json'
        }
        print(variables)
        res = requests.post(urljoin(API_URL, 'graphql'),
                            headers=headers,
                            json={'query': query, 'variables': variables})
        if res.status_code != 200:
            print(res.text)
            raise RuntimeError('An error occurred in the API request')
        return res.json()['data']

    def connect(self):
        return psycopg2.connect(
            database="postgres", user=self.db_creds['username'],
            password=self.db_creds['password'],
            host=self.db_creds['host'],
            port=self.db_creds['port']
        )

    def get_current_inv(self):

        conn = self.connect()

        conn.autocommit = True

        cursor = conn.cursor()

        cursor.execute(
            "select parts.id, locations.id, sum(parts_inventory.quantity) as inventory_qty from epirussystems_com.parts join epirussystems_com.parts_inventory on parts.id = parts_inventory.part_id join epirussystems_com.locations on parts_inventory.location_id = locations.id where locations.id = 711 group by parts.id, locations.id")

        resul = cursor.fetchall();

        conn.close()
        keys = ['part_id', 'location_id', 'quantity']
        #current_inventory = pd.DataFrame(resul, columns=keys)
        results = np.array(resul)
        current_inventory = [dict(zip(keys, l)) for l in results]
        return current_inventory

    def create_part_kit(self):
        """Create part kits by calling API."""
        print('creating kit')
        kit_payload = {
            'assignedToId': self.assigned_to_id,
            'deliveryLocationId': self.delivery_location_id
        }
        part_kit = self.call_api(CREATE_KIT_MUTATION, {'input':kit_payload})
        part_kit_id = part_kit['createPartKit']['partKit']['id']
        print(f"Created part kit: {part_kit_id}")
        return part_kit_id

    def create_part_kit_item(self, part_kit_id, part_id, request_quantity):
        #print(part_kit_id)
        kit_item_payload = {
            'partKitId': part_kit_id,
            'partId': part_id,
            'quantity': request_quantity
        }
        self.call_api(CREATE_KIT_ITEM_MUTATION, {'input': kit_item_payload})
        print(f"Added part_id {part_id} to {part_kit_id}")

    def get_current_inventory(self):
        """Gets the current inventory level based on json above."""
        current_inventory_dict = {}
        current_inventory = self.get_current_inv()
        for item in current_inventory:
            #print(item['part_id'],item['location_id'],item['quantity'])
            current_inventory_dict[(item['part_id'], item['location_id'])] = item['quantity']
        return current_inventory_dict

    def check_inventory_levels(self):
        """Checks inventory level vs desired min/max for part/location."""
        part_kit_id = None
        for part in self.inventory_config:
            part_id = part['part_id']
            location_id = part['lineside_location_id']
            inventory_qty = self.current_inventory_dict.get((int(part_id), int(location_id)),0)
            #print(part_id,location_id,inventory_qty)
            if inventory_qty < float(part['min_qty']):
                request_quantity = float(part['max_qty']) - inventory_qty
                if part_kit_id is None:
                    part_kit_id = self.create_part_kit()
                self.create_part_kit_item(part_kit_id, part_id, request_quantity)
            else:
                print(f'{part_id} quantity is {inventory_qty} at location: {location_id}')


def main():
    try:
        config = configparser.ConfigParser()
        config.read('prodReplenishment.ini')
        assigned_to_id = config['DEFAULT']['assigned_to_id']
        delivery_location = config['DEFAULT']['delivery_location_id']
        kitrequest = KitRequest(assigned_to_id, delivery_location)
        kitrequest.check_inventory_levels()
        print('Inventory levels checked')
    except Exception as e:
        print(f'An error occurred while running script: {e}')


if __name__ == "__main__":
    main()
