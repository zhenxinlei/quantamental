import pickle
import json
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import  HttpError


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class GoogleSheetController:

    def __getService(self):
        """Shows basic usage of the Sheets API.
          Prints values from a sample spreadsheet.
          """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    '/Users/ZhenxinLei/Desktop/tmp/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                print("after getting creds ")
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
            print(" create pickle ")

        service = build('sheets', 'v4', credentials=creds)

        return service


    def read(self, sheet_id, range):
        try:
            service =self.__getService()
            request = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range)
            response = request.execute()
            #print("read response {}".format(response))
            return response['values']
        except:
            return None

    def clear(self, sheet_id, range):
        service =self.__getService()
        response_body ={
        }
        request = service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=range, body=response_body)
        response = request.execute()
        print(response)
        pass

    def write(self,sheet_id, range, values):
        service = self.__getService()
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,range=range, valueInputOption="USER_ENTERED",
            body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))


    def get_or_create_tab(self, sheet_id,tab_name, header):
        service = self.__getService()
        try:
            request = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=tab_name+"!A1:A2")
            response=request.execute()

            #print(response)
        except HttpError as e:
            e_str = e.content.decode()
            if 'Unable to parse range: ' in e_str and  header != None:

                body = {

                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': tab_name,

                            }
                        }
                    }]
                }

                result = service.spreadsheets().batchUpdate(
                    spreadsheetId=sheet_id,body=body).execute()
                self.write(sheet_id, tab_name + "!A1:Z1", [header])
            else:
                 raise e

    def append(self,sheet_id,range, values):
        service = self.__getService()

        body = {
            'values': values
        }
        result = service.spreadsheets().values().append(
            spreadsheetId=sheet_id, range=range,
            valueInputOption="USER_ENTERED", body=body).execute()
        print('{0} cells appended.'.format(result \
                                           .get('updates') \
                                           .get('updatedCells')))





if __name__ == '__main__':
    controller = GoogleSheetController()
    SAMPLE_SPREADSHEET_ID = '1_96TAcYvT07e9Lriur6YTQhTL30_G2bF8jM3_4tgNkc'
    SAMPLE_RANGE_NAME = 'playground!A1:E'

    #controller.get_or_create_tab(SAMPLE_SPREADSHEET_ID, "playground", None)
    #controller.get_or_create_tab(SAMPLE_SPREADSHEET_ID, "playground2",['Date','Stay','New','Removed'])

    controller.append(SAMPLE_SPREADSHEET_ID, "playground", ['Sample'])