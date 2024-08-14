'''
   these are functions used by the update_data and display_data
   scripts and by the read_data_func functions
        
   access these values in other modules by
        import sp500_pe.helper_func as hp
'''

from openpyxl import load_workbook
import polars as pl
from datetime import datetime
import openpyxl.utils.cell
import sys


def my_df_print(df):                                
    with pl.Config(
        tbl_cell_numeric_alignment="RIGHT",
        thousands_separator=",",
        float_precision=1
    ):
        print(df)
        

def string_to_date(series):
    '''
        receives pl.Series (col from df), str file names
        extracts the date string, "yyyy mm dd",
        returns the string to date object, as pl.Series
    '''
    # splits f_name into two two words,
    # the first and the rest
    # then splits the .xlsx from the rest
    return pl.Series([datetime.strptime((f_name.split(' ', 1)[1])
                                               .split('.', 1)[0], 
                                        '%Y %m %d').date()
                      for f_name in series])
    
    
def date_to_year_qtr(series):
    '''
        Receives pl.Series (col from df)
        Returns year_qtr string, yyyy-Qq, as pl.Series
    '''
    return pl.Series([f"{date.year}-Q{date_to_qtr(date)}"
            for date in series])
    
    
def date_to_qtr(date):
    '''
        Returns qtr number as string
    '''
    return f"{((date.month) - 1) // 3 + 1 }"


def is_quarter_4(series):
    return pl.Series([yq[-1] == '4'
                      for yq in series])
    
    
def yrqtr_to_yr(series):
    '''series of strings y-q in,
       series of strings y out
    '''
    return pl.Series([yq[:4]
                      for yq in series])


def read_wksht(file_addr, wksht_name, first_col, date_key=None):
    """_summary_
    This helper function returns the worksheet and date
    for the worksheet name in the workbook 
    specified by the file address
    
    Args:
        file_addr (_type_): _description_
        wksht_name (_type_): _description_
        date_key (_type_): _description_

    Returns:
        list
        _type_: _description_
    """

    wkbk = load_workbook(filename=file_addr,
                         read_only=True,
                         data_only=True)
    wksht = wkbk[wksht_name]
    max_to_read = wksht.max_row
    
    # find cell with the date of the wkbk, fetch its value
    # crawl down col A; find first datetime entry
    if date_key is not None:
        d_row = 1
        date = wksht['A1'].value
        while (not isinstance(date, datetime) and
              (d_row < max_to_read)):
            d_row += 1
            date = wksht[f'{first_col}{d_row}'].value
        if d_row == max_to_read:
            return [wksht, None]
        name_date = \
            f'{date.month:02}-{date.day:02}-{date.year:04}'
    else:
        name_date = None
                                                                          
    return [wksht, name_date]


def find_key_loc(start, stop, wksht, key_wrds, col=None, row=None):
    """
    Set key and col to search a col down its rows for a string 
    that matches key.
    Set row, not col, to search a row along its cols for None

    Args:
        start (_type_): _description_
        stop (_type_): _description_
        key (_type_): _description_
    """
    # helper function for returning step
    def return_step(item, key_wrds):
        if any(k is None for k in key_wrds):
            if ((item is None) or 
                (item in key_wrds)):
                return True
        elif ((item is not None) and 
              (item in key_wrds)):
            return True
        else:
            return False
        
    if (((col is not None) and (row is not None)) or
       ((col is None) and (row is None))):
        print('In find_key_loc():')
        print('One and only one of row, col must be None')
        print(start, stop, wksht, key_wrds, col, row)
        sys.exit()
    
    # scan rows
    if col is not None:
        for step in range(start, stop):
            item = wksht[f'{col}{step}'].value
            if isinstance(item, str):
                item = "".join(list(item.split()))
            if return_step(item, key_wrds):
                return step
    # scan cols
    elif row is not None:
        for step in range(start, stop):
            col = openpyxl.utils.cell.get_column_letter(step)
            item = wksht[f'{col}{row}'].value
            if return_step(item, key_wrds):
                return step
            
    print('In find_key_loc():')                
    print(f'key {key_wrds} not found')
    print(start, stop, wksht, key_wrds, col, row)
    sys.exit()


def data_from_sheet(wksht, start_col, stop_col, skip_cols,
                    start, stop):
    """_summary_
    
    This helper function fetches the block of data specified
    by first_col, start (row) and last_col, stop (row)
    in the specified worksheet

    Args:
        first_col (_type_): _description_
        last_col (_type_): _description_
        start (_type_): _description_
        stop (_type_): _description_
    """
    # rng references the block of data
    # the comprehense fetches the data over cols for each row
    rng = wksht[f'{start_col}{start}:{stop_col}{stop}']
    data = [[col_cell.value 
            for ind, col_cell in enumerate(row)
            if ind not in skip_cols]
            for row in rng]
    
    return data
