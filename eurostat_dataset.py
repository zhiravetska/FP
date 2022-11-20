import gzip
import urllib.request as request
import pandas as pd
import io
import sqlite3

class eurostat_dataset:
    """
    get code from:
    https://ec.europa.eu/eurostat/databrowser/explore/all/all_themes?lang=en&display=list&sort=category
    """
    def __init__(self,code):
       self.code=code
       
# Occasionally (but not often), 
# you really don't care about the object that your method is bound to, 
# and in that circumstance, you can decorate the method 
# with the builtin staticmethod() function
    @staticmethod 
    def _getCodes(dictionary):
      url_dict = "https://ec.europa.eu/eurostat/" + \
          "estat-navtree-portlet-prod/BulkDownloadListing" + \
          "?sort=1&downfile=dic%2Fen%2F" +\
           dictionary + ".dic"
      with request.urlopen(url_dict) as resp:
                file_content = resp.readlines()
      d={}
      for i in file_content:
            if len(i) > 1:
                  row=i.decode().split('\t')
                  d[row[0]] = row[1].strip()
      return d
    
    def GetDf(self):
        """ 
        dataset: choose the dataset code from:
    https://ec.europa.eu/eurostat/databrowser/explore/all/all_themes?lang=en&display=list&sort=category
        something like EXT_LT_INTROEU27_2020
    List of datasets for downloding: 
    https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start=a
    Returtn the cleaned dataset with decoded abbreviation

        """

        self.code = self.code.lower()
        url = "https://ec.europa.eu/eurostat/" + \
            "estat-navtree-portlet-prod/" + \
            "BulkDownloadListing?file=data%2F/" + \
            self.code + ".tsv.gz"

        with request.urlopen(url) as r:
                with gzip.GzipFile(fileobj=r) as data:
                    file_content = data.read() #data read as binary from gzip
        # the approach to make the dataframe from the object with read data:
        # https://stackoverflow.com/questions/39213597/convert-text-data-from-requests-object-to-dataframe-with-pandas
        # the eurostat data contains not strict columns:first line with separator ',',the date columns - separator - '\t'
        df = pd.read_csv(io.StringIO(file_content.decode('utf-8')),sep=",|\t| [^ ]?\t", na_values=":",
                        engine="python",encoding='utf-8')
        #devide one column 'geo\\date'
        df.columns = [x.split('\\')[0].strip(' ') for x in df.columns]
        # use only file_content,not df,it is binary, so use decode()
        #file_content has the first row with the code we need to decode into names using function get_code
        codes=file_content.decode().split('\t')[0].split('\\')[0].split(',')
        df = df.melt(id_vars=codes, 
        var_name="date", 
        value_name="value")
        for c in codes:
            df[c].replace(self._getCodes(c),inplace=True)
        return df

    def WriteToDatabase(self):
        """
        eurostat dataset will be written 
        to the sqlite database 'ngr.bd' 
        as a separate table

        """
        conn = sqlite3.connect("ngr.db")
        self.getDf().to_sql(str(self.code), conn, if_exists="replace")
        conn.close()

    def DropTable(self):
        """
        drop table in the database

        """
        self.code=self.code.lower()
        conn = sqlite3.connect("ngr.db")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE "+str(self.code))
        print("Table dropped... ")
        conn.commit()
        conn.close()
    
    def DatasetInfo(self):
        """

        Output: Title, Data Start, Data End
        """
        self.code=self.code.lower()
        url = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents_en.txt'
        df = pd.read_csv(url, skiprows=6, header=None, sep='\s+')
        columns = pd.read_csv(url, header=None, sep='\s+').iloc[0].values
        df.columns=columns[:-1]
        df=df[~df.type.isin(['folder','table'])]
        df.title = df.title.str.lstrip()
        title = df[df.code==self.code].title.values[0]
        data_start = df[df.code==self.code]['data start'].values[0]
        data_end = df[df.code==self.code]['data end'].values[0]
        return title,data_start,data_end