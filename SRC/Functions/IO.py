#Created 2021-09-03
#Copyright Spencer W. Leifeld


class CSV_File:
    
    def __init__(self, aSparkSession,  aPath, aSchema=None):
        self.spark = aSparkSession
        print('Now Reading: ' + aPath)
        self.csvFile = self.spark.read.csv(aPath, schema=aSchema, header=True, inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, nullValue='NULL', nanValue=0, maxCharsPerColumn=128)

    def GetSparkDF(self):
        return self.csvFile

    def Schema(self):
        self.csvFile.printSchema()

    @staticmethod
    def ExportCSV(aPath, aDataframe, aColumns=None, aAppend=False, aIndexColumns=None):
        if aAppend:
            mode = 'append'
        else:
            mode = 'overwrite'
        if aColumns is None:
            aDataframe.to_koalas().to_csv(aPath, na_rep='N/A', date_format='yyyy-mm-dd', mode=mode, index_col=aIndexColumns)