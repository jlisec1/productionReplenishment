import pandas as pd
import numpy as np
import requests
from urllib.parse import urljoin
import psycopg2
import boto3
import json
import queries

# Set authentication and server parameters
# Refer here for auth servers and api endpoints: https://manual.firstresonance.io/api/access-tokens
API_URL = 'https://api.ion-gov.com'
AUTHENTICATION_SERVER = 'auth.ion-gov.com'

AUTH_URL = 'auth.ion-gov.com'
API_URL = 'api.ion-gov.com'
REQUEST_URL = f'https://{API_URL}/graphql'
ACCESS_TOKEN_URL = f'https://{AUTH_URL}' \
    '/auth/realms/api-keys/protocol/openid-connect/token'


class AbomsRequest:

    #init function to set all variables initially
    def __init__(self):
        self.api_creds = self.grab_creds('ion/api/creds')
        self.db_creds = self.grab_creds('ion/db/psql')
        self.part = 'test_part' # input('Enter the part_number for the assembly you just completed: ').upper()
        self.rev =  'A' # input('Enter the revision for the part number you just completed: ').upper()
        self.serial = input('Enter the serial number of the unit you just completed: ')
        self.flag = self.part_check()
        self.access_token = self._generate_access_token()
        self.inventory = self.get_inv(loc=711) #need to figure out
        if self.flag:
            print('One or more of the inputs are incorrect. Try again.')
            self.__init__()
        else:
            pass
        self.df = self.gimme_aboms()

    def grab_creds(self, sec_id: any):
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=sec_id)
        database_secrets = json.loads(response['SecretString'])
        return database_secrets

    def set_part_info(self):
        self.serial = input('Enter the serial number of the unit you just completed: ')

    def _generate_access_token(self):
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.api_creds['clientId'],
            'client_secret': self.api_creds['clientSecret']
        }
        url = ACCESS_TOKEN_URL
        with requests.post(url, data=data) as response:
            response.raise_for_status()
            self.access_token = response.json()['access_token']
        return response.json()['access_token']

    def _request(self, query: dict):  # what does this function do?
        retry = True
        while True:
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            url = REQUEST_URL
            with requests.post(url, headers=headers, json=query) as response:
                if (
                    retry and
                    response.status_code == requests.codes.unauthorized
                ):
                    self._generate_access_token()
                    retry = False
                    continue
                response.raise_for_status()
                return response.json()

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

    def part_check(self): #checks to see if the requested part is in fact in ion

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

        query1 = "with parents as(select ai.id parent_abom_id, pi.serial_number parent_serial, part_number,ai.part_id parent_part_id, pi.id as parent_part_inventory_id from epirussystems_com.abom_items ai join epirussystems_com.parts_inventory pi on ai.part_inventory_id = pi.id join epirussystems_com.parts p on pi.part_id = p.id where p.part_number = %s and revision = %s) select parent_serial,child_abom_item_id, cast(child_items.part_id as text), cast(mbom_substitute.part_id as text) alternate_part_id, expected_quantity_per,child_items.quantity, cast(child_items._etag as text) from epirussystems_com.abom_edges join parents on parent_abom_item_id=parents.parent_abom_id join epirussystems_com.abom_items child_items on abom_edges.child_abom_item_id = child_items.id left join epirussystems_com.parts_inventory on child_items.part_inventory_id = parts_inventory.id left join epirussystems_com.parts on child_items.part_id = parts.id left join epirussystems_com.mbom_substitute on origin_mbom_item_id=mbom_substitute.mbom_item_id where child_items.part_id <> 5888 and tracking_type is null or tracking_type = 'lot'"
        cursor.execute(query1, [self.part, self.rev])

        resul = cursor.fetchall();
        #print(resul)
        conn.close()
        df = pd.DataFrame(resul, columns=['parent_sn', 'child_abom_item_id', 'child_part_id', 'alternate_part_id','expected_quantity_per',
                                        'quantity_installed', 'etag'])
        return df
        #print(df)

    def get_inv(self, loc):
        """"""
        query = {
            'query': queries.GET_INV,
            'variables': {
                'filters': {
                    "locationId": {"eq": loc},
                    "status": {"eq": "AVAILABLE"}
                }
            }
        }
        r = self._request(query)
        num = len(r['data']['partInventories']['edges'])
        d_list = []
        for i in np.arange(1, num):
            d_list.append(r['data']['partInventories']['edges'][i]['node'])
        df = pd.DataFrame.from_records(d_list)
        return df


    #returns the part_inventory ID that we are going to use
    #for part in df returned above return the inventory ID we want
    def build_payload(self, part_id, loc):
        bm1 = (self.df['parent_sn'] == self.serial)
        bm = (self.df['child_part_id'] == part_id)
        dff = self.df[(bm1 & bm)]
        my_col = dff['alternate_part_id']
        bma = (self.inventory['partId'] == int(part_id))
        inv = self.inventory[bma]
        row = (inv.query('quantityAvailable == quantityAvailable.max()').tail(1))
        if dff['quantity_installed'].values[:1] < dff['expected_quantity_per'].values[:1]: # if the installed quantity is less than expected
            if len(inv.index) == 0: # if the dataframe is empty (therefore there is no inv at the location
                print('CANNOT FIND INVENTORY FOR PART_ID ' + str(part_id) + ' AT LOCATION_ID:' + str(loc))
                print('looking for alternate')
                if not my_col.isna().any(): #if the alternate part cell is not null (there exists an alternate part)
                    alt = dff.iloc[0,3]
                    altbma = (self.inventory['partId'] == int(alt))
                    altinv = self.inventory[altbma]
                    altrow = (altinv.query('quantityAvailable == quantityAvailable.max()').tail(1))
                    if int(altrow['quantityAvailable']) >= int(dff['expected_quantity_per']):  # if the quantity at the location is greater than the quantity to be installed
                        altinvid = altrow['id'].tolist()
                        ids = dff['child_abom_item_id'].tolist()
                        qty = dff['expected_quantity_per'].tolist()
                        etag = dff['etag'].tolist()
                        kit_item_payload = { # builds a payload that includes the specific part inventory that the abom will install from
                            'id': ids[0],
                            'quantity': qty[0],
                            'etag': etag[0],
                            'partInventoryId': altinvid[0]}
                        print('Building aBOM with alternate')
                        return kit_item_payload
                    else: # if there is none of the alternate at the location
                        print(f'No quantity of alternative {alt} at location {loc}')
                        input('Did you inform the supervisor of the low inventory?')
                        return {'id': '-1'}
                else: # if no alternate part was found
                    print('System was unable to find an alternate for: ' + str(part_id))
                    input('Did you inform the supervisor of the low inventory?')
                    return {'id': '-1'}
            else:
                if int(row['quantityAvailable']) >= int(dff['expected_quantity_per']): # if the quantity at the location is greater than the quantity to be installed
                    ids = dff['child_abom_item_id'].tolist()
                    invid = row['id'].tolist()
                    qty = dff['expected_quantity_per'].tolist()
                    etag = dff['etag'].tolist()
                    kit_item_payload = { # builds a payload that includes the specific part inventory that the abom will install from
                        'id': ids[0],
                        'quantity': qty[0] ,
                        'etag': etag[0],
                        'partInventoryId':invid[0]}
                    return kit_item_payload
        elif dff['quantity_installed'].values[:1] >= dff['expected_quantity_per'].values[:1]:
            print('already fulfilled')
            return {'id':'-1'}


    def consume_to_abom(self,loc): # updates item in payload from user input
        print(f'-----------------------Consuming From {loc}-----------------------')
        bm = (self.df['parent_sn']==self.serial)
        dff = self.df[bm]
        for part_id in dff['child_part_id']:
            kit = self.build_payload(part_id,loc)
            if int(kit['id']) >= 0:
                self.call_api(queries.UPDATE_ABOM_MUTATION, {'input': kit})
        print(f"***ALL PARTS ADDED TO ABOM*** {self.serial}")


def main():
    try:
        abomsreq = AbomsRequest()
        abomsreq.part_check()
        abomsreq.consume_to_abom(loc=711)
    except Exception as e:
        print(f'An error occurred while running script: {e}')


if __name__ == "__main__":
    main()
