# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 10:15:10 2017

@author: DM390
"""


from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from cStringIO import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from dateutil import parser
from datetime import datetime
#This extracts table from pdf
from tabula import read_pdf

import re,os,pandas,sqlalchemy

#fileName = 'MethylMethacrylate_Asia-Pacific_7Oct2016.pdf'
#fileName2 = 'MethylMethacrylate_Asia-Pacific_14Oct2016.pdf'

def extractTable(fileName,dateString):
    
    df = read_pdf(fileName)
    df.rename(columns={'Unnamed: 1':'Limit'},inplace=True)
    df.rename(columns={'Unnamed: 2':'PriceRange'},inplace=True)
    df.rename(columns={'Unnamed: 3':'WeekLimit'},inplace=True)
    df.rename(columns={'Unnamed: 4':'FourWeeksAgo'},inplace=True)
    df.rename(columns={'Unnamed: 5':'US_CTS/lb'},inplace=True)
    temp = df.columns[0]
    df.rename(columns={temp:'Market'},inplace=True)
    
    for i in df['Market']:
        if re.match(r'^SPOT PRICES.*',str(i)):
            temp2 = i
            ind = df[df['Market'] == temp2].index.tolist()[0]
    
    for i in range(0,ind):
        df = df.set_value(i,'SPOT_PRICES_CARGOES',temp)
        
    for i in range(ind,len(df)):
        df = df.set_value(i,'SPOT_PRICES_CARGOES',temp2)
    
    df.dropna(inplace=True)    

    df=df.drop(['Limit','WeekLimit'],axis=1)
    
    df['Market'] = df['Market'].map(lambda x:x.strip('USD/tonne'))

    l = fileName.strip('.pdf').split('_')
    
    df['FileName'] = fileName.split('/')[2]    
    df['Commodity'] = l[0].split('/')[2]
    df['Continent']=l[1]
    df['Date'] = parser.parse(dateString)
    df['DateFetched'] = datetime.now()
    return df


def toDataFrame(points,fileName,dateString):
    l = []
    for key,value in points.items():
        l.append((key,value))    

    col = ['Heading','Body']

    df = pandas.DataFrame(data=l,columns=col)

    l = fileName.strip('.pdf').split('_')

    df['FileName'] = fileName.split('/')[2]    
    df['Commodity'] = l[0].split('/')[2]
    df['Continent'] = l[1]
    df['Date']= parser.parse(dateString) 
    df['DateFetched'] = datetime.now()
    
    return df

def convert(fname, pages=None):
    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)

    output = StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    infile = file(fname, 'rb')
    for page in PDFPage.get_pages(infile, pagenums):
        interpreter.process_page(page)
    infile.close()
    converter.close()
    text = output.getvalue()
    output.close
    return text 
    

#def extractPoints(tempText,pointsFound):
#    tempDict = {}
#    for heading in pointsFound:
#        if heading == 'Overview':
#            pattern = r'(?<=Overview).*[\s\S]*(?=Price chart)'
#        elif heading == pointsFound[-1]:
#            pattern = r'(?<='+heading+').*[\s\S]*(?=Full Report List)'
#        else:
#            pattern = r'(?<='+heading+').*[\s\S]*(?='+pointsFound[pointsFound.index(heading)+1]+')'
#            
#        tempDict.setdefault(heading,re.findall(pattern,tempText,re.IGNORECASE)[0].strip())
#    
#    return tempDict    

def extractPoints(tempText,pointsFound):
    tempDict = {}
    for heading in pointsFound:
        if heading == 'Overview':
            pattern = r'(?<=Overview)[^;;]+?(?=Price chart)'
        elif heading == pointsFound[-1]:
            pattern = r'(?<='+heading+')[^;;]+?(?=Full Report List)'
        else:
            pattern = r'(?<='+heading+')[^;;]+?(?='+pointsFound[pointsFound.index(heading)+1]+')'
            
        tempDict.setdefault(heading,re.findall(pattern,tempText,re.IGNORECASE)[0].strip())
    
    return tempDict    



def cleanText(points):
    for key in points.keys():
        points[key] = points[key].decode(encoding='ascii',errors='ignore').strip()
    

def parsePDF(files):

    global contentDF,tableDF,listOfpoints,pointsFound,engine
    for File in files:
        text = convert(File)
        dateString = re.search(r'\d{1,2} [a-zA-Z]+ \d{4}',text).group(0)
        
        tableDF = tableDF.append(extractTable(File,dateString),ignore_index=True)
        #tableDF['PriceRange'].str.split('-',1,expand=True)
        
        #regexObj = re.compile(r'ICIS accepts no liability for commercial decisions.*[\s\S]{10}.*[\s\S]{10}.*www.icis.com[\s\S]{30}.*Asia Pacific[\s\S]{2}')
        
        regexObj = re.compile(r'ICIS accepts no liability for commercial decisions[^;;]+?Methyl Methacrylate   Asia Pacific[\s\S]{2}')
        
        tempText = regexObj.sub('',text)
        
        pointsFound = [i for i in listOfpoints if re.search(i,text,re.IGNORECASE) ]
        
        points = extractPoints(tempText,pointsFound)    
        points['Overview'] =re.sub(r'.*[\s\S]*(?<=\d)','',points['Overview'])
        
        cleanText(points)    
        df = toDataFrame(points,File,dateString)
        contentDF = contentDF.append(df,ignore_index=True)                
                
        fileName = File.split('.pdf')[0]+'.txt'

        with open(fileName,'wb') as fd:
            fd.write(text)

        print 'Parsed '+File.split('/')[2]+' '+str(datetime.now())
        
    tableDF.to_sql(name='ICIS_Commodity_prices',con=engine,if_exists = 'append',index = False)
    contentDF.to_sql(name='ICIS_doc',con=engine,if_exists = 'append',index = False)
    #contentDF = pandas.DataFrame();tableDF=pandas.DataFrame();pointsFound = []
    
def getFiles():
    temp = []
    for doc in os.listdir('./documents'):
        if doc.endswith('.pdf'):
            if os.path.exists('./documents/'+doc.split('.pdf')[0]+'.txt'):
                continue
            else:
                temp.append('./documents/'+doc)
    
    if len(temp) == 0:
        print "New files not found"            
    return temp


if __name__=="__main__":

    listOfpoints = ['Overview','Asia spot markets','Chinese domestic market',
                    'Other Regional Markets','Upstream','Downstream related','IPEX']
    
    pointsFound = []

    engine = sqlalchemy.create_engine('mssql+pyodbc://sa:miswak365@VrushabhPC')
    
    contentDF = pandas.DataFrame();tableDF=pandas.DataFrame()
    
    files = getFiles()
    
    parsePDF(files)


