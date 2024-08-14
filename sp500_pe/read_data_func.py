'''
   these are functions used by the update_data and display_data
   scripts
        
   access these values in other modules by
        import sp500_pe.read_data_func as rd
'''

from openpyxl import load_workbook
import polars as pl
from datetime import datetime
import polars.selectors as cs
import openpyxl.utils.cell
import sp500_pe.helper_func as hp

def data_reader(key_wrds, stop_wrds, file_addr, 
                wksht_name, first_col, last_col, 
                empty_cols=[], date_key=None, 
                column_names=None):
    """_summary_
    
    This function uses read_wksht() and data_from_sheet()
    to return the block of data in a worksheet as a dataframe

    Args:
        key_wrd (_type_): _description_
        file_addr (_type_): _description_
        wksht_name (_type_): _description_
        empty_cols (_type_): _description_
        first_col (_type_): _description_
        last_col (_type_): _description_
        date_cell (_type_): _description_
        column_names (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    wksht, name_date = hp.read_wksht(file_addr, wksht_name,
                                     first_col, date_key)
    max_to_read = wksht.max_row
    
    # first data row to read. Follows key_wrd row.
    start = 1 + hp.find_key_loc(1, max_to_read, wksht, key_wrds, first_col)
    # last data row to read. For rows, no key or row values.
    stop = -1 + hp.find_key_loc(start, max_to_read, wksht, 
                             stop_wrds, first_col)
    
    # read a list of lists (rows)
    data = hp.data_from_sheet(wksht, first_col, last_col, empty_cols,
                           start, stop)
    # iterate over rows to convert all dates to datetime
    # row[0]: datetime or str, '%m/%d/%Y' is first 'word' in str
    for row in data:
        if isinstance(row[0], str):
            row[0] = row[0].split(" ")[0]
            row[0] = datetime.strptime(row[0], '%m/%d/%Y')
    
    data_df = pl.DataFrame(data, schema=column_names, orient="row")\
                .cast({cs.float(): pl.Float32,
                       pl.Datetime: pl.Date})\
                .with_columns(pl.col('date')
                                .map_batches(hp.date_to_year_qtr)
                                .alias('year_qtr'))
    return [data_df, name_date]


def margin_reader(key_wrds, file_addr, wksht_name,
                  first_col):
    """read the operating margin data for S&P 500 by year.
       one df for each date of projections

    Returns:
        data_df   : pl.DateFrame containing margin data
        name_date : date of projection
    """
    
    wksht, _ = hp.read_wksht(file_addr, wksht_name, first_col)
    max_to_read = wksht.max_row
    
    # find the rows with margin data
    start = 1 + hp.find_key_loc(3, max_to_read, wksht, 
                             key_wrds, col=first_col)
    stop = start + 3
    
    # The label for the first col, which contains the qtr labels
    # is 'qtr'.  The other cols are named by year
    col_label = ["qtr"]
    max_to_read = 100
    l_col = -1 + hp.find_key_loc(2, max_to_read, wksht,
                              [None], row=start-1)
    stop_col = col = openpyxl.utils.cell.get_column_letter(l_col)
    
    for step in range(2,l_col+1):
        col = openpyxl.utils.cell.get_column_letter(step)
        yr = str(wksht[f'{col}{start - 1}'].value)
        yr = "".join([ch for ch in yr if ch.isdigit()])
        col_label.append(yr)
    
    data = hp.data_from_sheet(wksht, first_col, stop_col, [],
                           start, stop)
    
     # clean the entries in the 'qtr' col of the df
    for idx, row in enumerate(data):
        row[0] = f'Q{4-idx}'
    
    # build "tall" 2-col DF with 'year_qtr' and 'margin'
    data_df = pl.DataFrame(data, schema=col_label, orient="row")\
                .cast({cs.float(): pl.Float32})\
                .unpivot(index= 'qtr', variable_name='year')
    data_df = data_df.with_columns(
                        pl.struct(['qtr', 'year'])\
                        .map_elements(lambda x: 
                                        f"{x['year']}-{x['qtr']}",
                                        return_dtype= pl.String)\
                        .alias("year_qtr"))\
                     .drop(['year', 'qtr'])\
                     .sort(['year_qtr'])
    return data_df


def  fred_reader(file_addr, name):
    wkbk = load_workbook(filename=file_addr,
                         read_only=True,
                         data_only=True)
    wksht = wkbk.active
    min_row = 12
    max_row = wksht.max_row
    
    data = hp.data_from_sheet(wksht, "A", "B", [],
                    min_row, max_row)
    
    df = pl.DataFrame(data, schema=['date', name],
                      orient='row')\
           .cast({pl.Datetime: pl.Date,
                  cs.float(): pl.Float32})\
           .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias('year_qtr'))\
           .drop('date')
    return df