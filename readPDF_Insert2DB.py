import tabula
from tabula import read_pdf
from tabulate import tabulate
import requests
from datetime import datetime,date
import pyodbc
import pandas as pd
def fileName():
    return datetime.today().strftime ('%d-%b-%Y')

def downloadPDF(url):
    fn = ""
    url = url
    try:
        r = requests.get(url) #
        fn =  str(fileName() + ".pdf")
        print(fn)
        with open(fn,'wb') as f:
            f.write(r.content)
    except Exception as ex:
        print(ex)

def readingPDF():
    try:
        # path = "G:\\Web Scrapping\\16-Apr-21.pdf"
        path =  str(fileName() + ".pdf")
        print(path )
        df = read_pdf(path,pages=1,java_options="-Dfile.encoding=UTF8",guess=False)
        tabula.convert_into(path, str("FB25-"+fileName() + ".tsv"), output_format="tsv", pages='all',guess=False)

        # data = pd.read_csv('FB25.tsv',sep='\t')
        tsv = open(str("FB25-"+fileName() + ".tsv")) 
        j = 0
        lis = []

        for i in tsv:
            if j > 8:
                lis.append(i)
            j +=1
        def Split(value):
            v = value.split()[-1]
            return v
        
        mid = list(map(Split,lis))
        print(mid)
        # print(data[8:])
#       
        pairID = [1,51,52,53]
        currencyPair = ['USDPKR','EURPKR','GBPPKR','JPYPKR']

        for i in range(4):
            conn = pyodbc.connect('Driver={SQL Server};'     # first go to ODBC check (system ) Data Source check SQL Server is aviliabe 
                        'Server=FARAZAHMED;'  # server Name  ,sept='             
                        'Database=master;'    # Schema Name
                        'Trusted_Connection=yes;')         
    #             set cursor 
            cursor = conn.cursor()
            # Prepare the stored procedure execution script and parameter values
            storedProc = "Exec [dbo].[SP_SBPFB25] @PairID=?,@CurrencyPair=?,@MID=?"

            # Values that inserted into table
            params = (pairID[i],currencyPair[i],mid[i])
            
            # Execute Stored Procedure With Parameters
            cursor.execute( storedProc, params )

            #             cursor.execute("exec [dbo].[SPConnectionWithPython] @Fname=?, @Lname =? ")
            conn.commit()
            
            print("Inserted SuccessFully")
            
            # Close the cursor and delete it
            cursor.close()
            del cursor

    except Exception as ex:
        print(ex)


downloadPDF("https://www.sbp.org.pk/ecodata/crates/2021/Apr/16-Apr-21.pdf")
readingPDF()