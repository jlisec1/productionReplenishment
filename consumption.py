import pandas as pd
import numpy as np
import requests
from urllib.parse import urljoin
import psycopg2
import boto3
import json

# Set authentication and server parameters
# Refer here for auth servers and api endpoints: https://manual.firstresonance.io/api/access-tokens
API_URL = 'https://api.ion-gov.com'
AUTHENTICATION_SERVER = 'auth.ion-gov.com'


UPDATE_ABOM_MUTATION = '''
    mutation updateAbom($input: UpdateABomItemInput!){
      updateAbomItem(input:$input){
        abomItem{
          id
        }
      }
    }
'''

UPDATE_RUN_STEP_STATUS = '''
    mutation updateRunStepStatus($input: UpdateRunStepInput!){
      updateRunStep(input:$input){
        runStep{
          status
        }
      }
    }
'''

AUTH_URL = 'auth.ion-gov.com'
API_URL = 'api.ion-gov.com'
REQUEST_URL = f'https://{API_URL}/graphql'


class AbomsRequest:

    #init function to set all variables initially
    def __init__(self):
        self.api_creds = self.grab_creds('ion/api/creds')
        self.db_creds = self.grab_creds('ion/db/psql')
        self.part = input('Enter the part_number for the assembly you just completed: ')
        self.rev = input('Enter the revision for the part number you just completed: ').upper()
        self.serial = input('Enter the serial number of the unit you just completed: ')
        self.flag = self.part_check()
        if self.flag:
            print('One or more of the inputs are incorrect. Try again.')
            self.__init__()
        else:
            pass
        self.access_token = self.get_access_token()
        self.df = self.gimme_aboms()

    def grab_creds(self, sec_id: any):
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=sec_id)
        database_secrets = json.loads(response['SecretString'])
        return database_secrets

    def set_part_info(self):
        self.serial = input('Enter the serial number of the unit you just completed: ')

    # gets access token for aws
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

    # sends query using API to graphQL
    def call_api(self, query, variables):
        """Calls ions GraphQL api."""
        headers = {
            'Authorization': f'{self.access_token}',
            'Content-Type': 'application/json'
        }
        #print(variables)
        res = requests.post(REQUEST_URL,
                            headers=headers,
                            json={'query': query, 'variables': variables})
        if res.status_code != 200:
            raise RuntimeError('An error occurred in the API request')
        return res.json()['data']

    # establishes connection to db
    def connect(self):
        return psycopg2.connect(
            database="postgres", user=self.db_creds['username'],
            password=self.db_creds['password'],
            host=self.db_creds['host'],
            port=self.db_creds['port']
        )

    def close_run(self):
        pass

    def part_check(self):

        conn = self.connect()

        conn.autocommit = True

        cursor = conn.cursor()

        query1 = "select parts_inventory.id from epirussystems_com.parts_inventory join epirussystems_com.parts on parts_inventory.part_id = parts.id where part_number = %s and revision = %s and serial_number = %s"
        cursor.execute(query1, [self.part, self.rev, self.serial])

        result = np.array(cursor.fetchall())

        conn.close()

        flag = not np.any(result)

        return flag

    #returns a data frame with all abom_id corresponding to the passed serial number (serial)
    def gimme_aboms(self):

        conn = self.connect()

        conn.autocommit = True

        cursor = conn.cursor()

        query1 = "with parents as(select ai.id parent_abom_id, pi.serial_number parent_serial, part_number,ai.part_id parent_part_id, pi.id as parent_part_inventory_id from epirussystems_com.abom_items ai join epirussystems_com.parts_inventory pi on ai.part_inventory_id = pi.id join epirussystems_com.parts p on pi.part_id = p.id where p.part_number = %s and revision = %s) select parent_serial,child_abom_item_id, cast(child_items.part_id as text), expected_quantity_per,child_items.quantity, cast(child_items._etag as text) from epirussystems_com.abom_edges join parents on parent_abom_item_id=parents.parent_abom_id join epirussystems_com.abom_items child_items on abom_edges.child_abom_item_id = child_items.id left join epirussystems_com.parts_inventory on child_items.part_inventory_id = parts_inventory.id left join epirussystems_com.parts on child_items.part_id = parts.id where tracking_type is null or tracking_type = 'lot'"
        cursor.execute(query1, [self.part, self.rev])

        resul = cursor.fetchall();

        conn.close()
        df = pd.DataFrame(resul, columns=['parent_sn', 'child_abom_item_id', 'child_part_id', 'expected_quantity_per',
                                        'quantity_installed', 'etag'])
        return df
        #print(df)

    #returns the part_inventory ID that we are going to use
    #for part in df returned above return the inventory ID we want
    def build_payload(self, part_id, loc):
        bm1 = (self.df['parent_sn'] == self.serial)
        bm = (self.df['child_part_id'] == part_id)
        dff = self.df[(bm1 & bm)]
        #print(dff['child_part_id'],dff['quantity_installed'])
        if int(dff['quantity_installed']) == 0:
            conn = self.connect()
            cursor = conn.cursor()
            query2 = "select id, quantity from epirussystems_com.parts_inventory where part_id = %s and origin_part_inventory_id is null and location_id = %s and quantity > 0"
            # in the future I want to make location a variable too
            for part_id in dff['child_part_id']:
                #print(part_id)
                cursor.execute(query2, [part_id, loc])
                resull = cursor.fetchall()
                if int(resull[0][1]) >= int(dff['expected_quantity_per']): #if the quantity at the location is greater than the quantity to be installed
                    ids = dff['child_abom_item_id'].tolist()
                    qty = dff['expected_quantity_per'].tolist()
                    etag = dff['etag'].tolist()
                    conn.close()
                    kit_item_payload = { #builds a payload that includes the specific part inventory that the abom will install from
                        'id': ids[0],
                        'quantity': qty[0] ,
                        'etag': etag[0],
                        'partInventoryId':resull[0][0]}
                    #print('payload built')
                    return kit_item_payload
                elif int(resull[0][1]) < int(dff['expected_quantity_per']):
                    print("INVENTORY TOO LOW FOR PART: " + part_id)
                    input('Did you inform the supervisor of the error y/n? : ')
                    return {'id':'-1'}
                elif int(resull[0][1]) == 0:
                    print("INVENTORY TOO LOW FOR PART: " + part_id)
                    input('Did you inform the supervisor of the error y/n? : ')
                    return {'id':'-1'}
        print('already fulfilled')
        return {'id':'-1'}


    # updates item in payload from user input
    def update_abom_item(self,loc):
        print(f'-----------------------Consuming From {loc}-----------------------')
        bm = (self.df['parent_sn']==self.serial)
        dff = self.df[bm]
        for part_id in dff['child_part_id']:
            #print(dff['quantity_installed'], dff['child_part_id'])
            kit = self.build_payload(part_id,loc)
            if int(kit['id']) >= 0:
                self.call_api(UPDATE_ABOM_MUTATION, {'input': kit})
                #print(f"Added part_id {part_id} to ABOM")
        print(f"***ALL PARTS ADDED TO ABOM*** {self.serial}")


def main(loc):
    try:
        abomsreq = AbomsRequest()
        #abomsreq.gimme_aboms()
        abomsreq.part_check()
        abomsreq.update_abom_item(loc)
    except Exception as e:
        print(f'An error occurred while running script: {e}')


if __name__ == "__main__":
    main()
