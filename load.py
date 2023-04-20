import pandas as pd
import random
import string

from openpyxl.workbook import Workbook
class Load:

    def exportToExcel(self, report):
        print('report length: {}'.format(len(report)))
        if len(report) != 0:
            letters = string.ascii_lowercase
            random_name = ''.join(random.choice(letters) for i in range(5))
            df = pd.DataFrame(data=report)
            df.to_excel('{}.xlsx'.format(random_name), index=False)