'''This program reads selected data from S&P, sp-500-eps-est.xlsx
   and from the 10-year TIPS rate from FRED: 
        https://fred.stlouisfed.org/series/DFII10
   It then writes these data as polars dataframes to .parquet files
        and writes a record of the files that it has read and writen
        in the form of a dictionary to to a .json file
   The polars dataframes contain the latest projections of earnings for the
   S&P500 within each quarter since late 2017. A polars dataframe contains
   the actual earnings and the value of the index for each quarter beginning
   in 1988. This dataframe also contains actual values for operating
   margins, revenues, book values, dividends, and other actual data
   reported by S&P, plus actual values for the 10-year TIPS.
   
   The addresses of documents for this project appear in this program's 
   project directory: S&P500_PE/sp500_pe/__init__.py
'''


import sp500_pe as sp
import sp500_pe.helper_func as hp
import sp500_pe.read_data_func as rd

import json
import sys
import polars as pl

#######################  Parameters  ##################################

# data from "ESTIMATES&PEs" wksht
RR_COL_NAME = 'real_int_rate'
YR_QTR_NAME = 'yr_qtr'
PREFIX_OUTPUT_FILE_NAME = 'sp-500-eps-est'
EXT_OUTPUT_FILE_NAME = '.parquet'

SHT_0_NAME = "ESTIMATES&PEs"
COLUMN_NAMES = ['date', 'price', 'op_eps', 'rep_eps',
                'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']
PROJ_COLUMN_NAMES = ['date', 'op_eps', 'rep_eps',
                     'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']

SHT_1_NAME = "QUARTERLY DATA"
COLUMN_NAMES_QTR = ['date', 'div_ps', 'sales_ps',
                    'bk_val_ps', 'capex_ps', 'divisor']

SHT_0_DATE_PARAMS = {
    'date_keys' : ['Date', 'Data as of the close of:'],
    'value_col_1' : 'D',
    'date_key_2' : ['ACTUALS'],
    'value_col_2' : 'B',
    'column_names' : COLUMN_NAMES,
    'include_prices' : True
}

SHT_0_PARAMS = {
    'act_key' : ['ACTUALS', 'Actuals'],
    'first_blk_col' : 'A',
    'last_blk_col' : 'J',
    'skip_col' : [4, 7],
    'column_names' : COLUMN_NAMES
}

SHT_M_PARAMS = {
    'marg_key': ['QTR'],
    'first_blk_col': 'A',
    'stop_col_key': None,
    'yr_qtr_name': YR_QTR_NAME
}

SHT_1_PARAMS = {
    'act_key' : ['END'],
    'first_blk_col' : 'A',
    'last_blk_col' : 'I',
    'skip_col' : [2, 3, 7],
    'column_names' : COLUMN_NAMES_QTR
}

SHT_0_PROJ_DATE_PARAMS = {
    'date_keys' : ['Date', 'Data as of the close of:'],
    'value_col_1' : 'D', 
    'date_key_2' : None, 
    'value_col_2' : None,
    'column_names' : None,
    'include_prices' : False
}

SHT_0_PROJ_PARAMS = {
    'act_key' : ['ESTIMATES'],
    'first_blk_col' : 'A',
    'last_blk_col' : 'J',
    'skip_col' : [1, 4, 7],
    'column_names' : PROJ_COLUMN_NAMES
}


#######################  MAIN Function  ###############################

def update_data_files():
    '''create or update earnings, p/e, and margin data
       from 'sp-500-eps-est ...' files
    '''
    

# ++++++  PRELIMINARIES +++++++++++++++++++++++++++++++++++++++++++++++
# load file containing record_dict: record of files seen previously
#   if record_dict does not exist, create an empty dict to initialize
    if sp.RECORD_DICT_ADDR.exists():
        with sp.RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)
        print('\n============================================')
        print(f'Read record_dict from: \n{sp.RECORD_DICT_ADDR}')
        print('============================================\n')
        
        # backup record_dict
        with sp.BACKUP_RECORD_DICT_ADDR.open('w') as f:
            json.dump(record_dict, f)
        print('============================================')
        print(f'Wrote record_dict to: \n{sp.BACKUP_RECORD_DICT_ADDR}')
        print('============================================\n')
        
    else:
        print('\n============================================')
        print(f'No record dict file found at: \n{sp.RECORD_DICT_ADDR}')
        print(f'Initialized record_dict with no entries')
        print('============================================\n')
        record_dict = {'prev_files': [],
                       'prev_used_files': [],
                       'latest_used_file': "",
                       'output_proj_files': [],
                       'proj_yr_qtrs' : []}
        
# create list of earnings input files not previously seen
# and add them to 'prev_files'
    prev_files_set = set(record_dict['prev_files'])
    
    dir_path = sp.INPUT_DIR
    new_files_set = \
        set(str(f.name) for f in dir_path.glob('sp-500-eps*.xlsx'))
    # [f for f in path.glob("*.[jpeg jpg png]*")]
    
    new_files_set = new_files_set - prev_files_set
    
    # if no new data, print alert and exit
    if len(new_files_set) == 0:
        print('\n============================================')
        print(f'No new files in {sp.INPUT_DIR}')
        print('All files have been read previously')
        print('============================================\n')
        sys.exit()
        
# there is new data, add new files to historical record
    record_dict['prev_files'].extend(
        list(new_files_set)
    )
    # most recent appear first
    record_dict['prev_files'].sort(reverse= True)

# find the latest new file for each quarter
    data_df = pl.DataFrame(list(new_files_set), 
                          schema= ["new_files"],
                          orient= 'row')\
                .with_columns(pl.col('new_files')
                            .map_batches(hp.string_to_date)
                            .alias('date'))\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias('year_qtr'))\
                .group_by('year_qtr')\
                .agg([pl.all().sort_by('date').last()])\
                .sort(by= 'year_qtr') 
    
    files_to_archive = list(new_files_set)
    del new_files_set

# combine with prev_files where new_files has larger date for year_qtr
# (new files can update and replace prev files for same year_qtr)
# new_files has only one file per quarter -- no need for group_by
    prev_used = record_dict['prev_used_files']
    if len(prev_used) > 0:
        used_df = pl.DataFrame(prev_used, 
                               schema= ['used_files'],
                               orient= 'row')\
                .with_columns(pl.col('used_files')
                            .map_batches(hp.string_to_date)
                            .alias('date'))\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias('year_qtr'))
                
    # update used_files, a join with new files
        # 1st filter removes yr_qtr rows that have no dates in data_df
        # pl.when marks rows that have data only from data_df or have
        #       more recent data from data_df
        # 2nd filter keeps only the rows with new data
        used_df = used_df.join(data_df,
                               on= 'year_qtr',
                               how= 'full',
                               coalesce= True)\
                         .filter(pl.col('date_right').is_not_null())\
                         .with_columns(
                             pl.when((pl.col('date').is_null()) | 
                                     (pl.col('date') <
                                      pl.col('date_right')))
                               .then(1)
                               .otherwise(0)
                               .alias('new_data'))\
                         .filter(pl.col('new_data') == 1)\
                         .rename({'used_files' : 'proj_to_delete'})\
                         .drop(['date'])\
                         .rename({'date_right': 'date'})\
                         .sort(by= 'year_qtr')
                         
        # remove old files from record_dict lists
        #   prev_used_files (.xlsx) & output_proj_files (.parquet)
        # remove the output .parquet file from output_proj_dir
        files_to_remove_list = \
            pl.Series(used_df.filter(pl.col('proj_to_delete')
                                    .is_not_null())\
                            .select(pl.col('proj_to_delete')))\
                            .to_list()
                            
        for file in files_to_remove_list:
            record_dict['prev_used_files'].remove(file)
            file_list = file.split(" ", 1)
            proj_file = \
                f'{file_list[0]} {file_list[1]
                                    .replace(' ', '-')
                                    .replace('.xlsx', '.parquet')}'
            record_dict['output_proj_files'].remove(proj_file)
            # using Path() object
            address_proj_file = sp.OUTPUT_PROJ_DIR / proj_file
            if address_proj_file.exists():
                address_proj_file.unlink()
                print('\n============================================')
                print(f'Removed {proj_file} from: \n{sp.OUTPUT_PROJ_DIR}')
                print(f'Found file with more recent date for the quarter')
                print('============================================\n')
            else:
                print('\n============================================')
                print(f"WARNING")
                print(f"Tried to remove: \n{address_proj_file}")
                print(f'Address does not exist')
                print('============================================\n') 
                
    # when len(prev_used) == 0
    else:
        used_df = data_df

    del data_df           
    
    # add dates of projections and year_qtr to record_dict
    # https://www.rhosignal.com/posts/polars-nested-dtypes/   pl.list explanation
    # https://www.codemag.com/Article/2212051/Using-the-Polars-DataFrame-Library
    # pl.show_versions()

# files with new data: files_to_read_list
    files_to_read_list = \
        pl.Series(used_df.select('new_files'))\
            .to_list()
    # add dates of projections and year_qtr to record_dict
    record_dict['prev_used_files'].extend(
        files_to_read_list)
    record_dict['prev_used_files'].sort(reverse= True)
    record_dict['proj_yr_qtrs']= \
        hp.date_to_year_qtr(
                hp.string_to_date(record_dict['prev_used_files'])
            ).to_list()
    # most recent is first
    record_dict["latest_used_file"] = record_dict['prev_used_files'][0]
                     
# +++++  fetch the latest data  +++++++++++++++++++++++++++++++++++++++
    print('\n================================================')
    print(f'Updating historical data from: {record_dict["latest_used_file"]}')
    print(f'in directory: \n{sp.INPUT_DIR}')
    print('================================================\n')
    
    # address of the most recent file
    latest_file_addr = sp.INPUT_DIR / record_dict["latest_used_file"]
    
    # most recent date and prices
    name_date, actual_df = \
        rd.read_sp_date(latest_file_addr,
                        SHT_0_NAME, **SHT_0_DATE_PARAMS)
    
    # load historical data, if updates are available
    df = rd.read_sp_sheet(latest_file_addr,
                          SHT_0_NAME,
                          **SHT_0_PARAMS)

    # if any date is None, halt
    if (name_date is None or
        any([item is None
            for item in actual_df['date']])):
        
        print('\n============================================')
        print(f'Abort using {latest_file_addr} \nmissing history date')
        print(f'Name_date: {name_date}')
        print(actual_df['date'])
        print('============================================\n')
        sys.exit()
        
    actual_df = pl.concat([actual_df, df], how= "diagonal")\
                  .with_columns(pl.col('date')
                        .map_batches(hp.date_to_year_qtr)
                        .alias(YR_QTR_NAME))
    del df
        
# margins
    margins_df = rd.margin_reader(latest_file_addr,
                                  SHT_0_NAME, **SHT_M_PARAMS)
    margins_df = margins_df.rename({'value': 'op_margin'})
    
    # merge margins with previous data
    actual_df = actual_df.join( 
            margins_df, 
            how="left", 
            on= YR_QTR_NAME,
            coalesce= True)
    del margins_df
        
# real interest rates, eoq, from FRED DFII10
    real_rt_df = rd.fred_reader(sp.INPUT_RR_ADDR,
                                RR_COL_NAME, YR_QTR_NAME)
    
    # merge real_rates with sp500_pe_dict['history']['actuals']
    actual_df = actual_df.join( 
            real_rt_df, 
            how="left", 
            on=[YR_QTR_NAME],
            coalesce= True)
    del real_rt_df
    
# qtrly_data
    qtrly_df = rd.read_sp_sheet(latest_file_addr,
                                SHT_1_NAME, **SHT_1_PARAMS)\
                 .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(YR_QTR_NAME))
    
    # merge qtrly with sp500_pe_dict['history']['actuals']
    actual_df = actual_df.join(qtrly_df,  
                               how= "left", 
                               on= [YR_QTR_NAME],
                               coalesce= True)
    del qtrly_df
    
# +++++ update history file +++++++++++++++++++++++++++++++++++++++++++
    # move any existing hist file in output_dir to backup
    if sp.OUTPUT_HIST_ADDR.exists():
        sp.OUTPUT_HIST_ADDR.rename(sp.BACKUP_HIST_ADDR)
        print('\n============================================')
        print(f'Moved history file from: \n{sp.OUTPUT_HIST_ADDR}')
        print(f'to: \n{sp.BACKUP_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'Found no history file at: \n{sp.OUTPUT_HIST_ADDR}')
        print(f'Wrote no history file to: \n{sp.BACKUP_HIST_ADDR}')
        print('============================================\n')
        
    # write actual_df, the historical data, into the output file
    # save sp500_pe_dict to file
    with sp.OUTPUT_HIST_ADDR.open('w') as f:
        actual_df.write_parquet(f)
    print('\n============================================')
    print(f'Wrote history file to: \n{sp.OUTPUT_HIST_ADDR}')
    print('============================================\n')
    
    del actual_df
    

 
# +++++ update projection files +++++++++++++++++++++++++++++++++++++++
# ordinarily a very short list
# loop through files_to_read, fetch projections of earnings for each date
    failure_to_read_lst = []
    for file in files_to_read_list:
        # echo file name and address to console
        input_address = sp.INPUT_DIR / file
        print(f'\n input file: {file}')    
        
# projections of earnings
        # read date of projection, no prices or other data
        name_date, _ = \
            rd.read_sp_date(input_address,
                            SHT_0_NAME, **SHT_0_PROJ_DATE_PARAMS)
        name_date = name_date.date()
    
        # load projections for the date
        proj_df = rd.read_sp_sheet(input_address,
                                   SHT_0_NAME, **SHT_0_PROJ_PARAMS)\
                    .with_columns(pl.col('date')
                        .map_batches(hp.date_to_year_qtr)
                        .alias(YR_QTR_NAME))

        # if any date is None, abort
        if (name_date is None or
            any([item is None
                for item in proj_df['date']])):
            print('\n============================================')
            print('In main(), projections:')
            print(f'Skipped sp-500 {name_date} missing projection date')
            print('============================================\n')
            failure_to_read_lst.append(file)
            continue
        
        # if name_date is not None, write the df to file
        output_file_name = \
            f'{PREFIX_OUTPUT_FILE_NAME} {name_date}{EXT_OUTPUT_FILE_NAME}'
        record_dict['output_proj_files'].append(output_file_name)
        output_file_address = sp.OUTPUT_PROJ_DIR / output_file_name
        print(f'output file: {output_file_name}')
        with output_file_address.open('w') as f:
            proj_df.write_parquet(f)
            
    # +++++ update archive ++++++++++++++++++++++++++++++++++++++++
    # archive all input files -- uses Path() variables
    # https://sysadminsage.com/python-move-file-to-another-directory/
    print('\n============================================')
    for file in files_to_archive:
        input_address = sp.INPUT_DIR / file
        if input_address.exists():
            input_address.rename(sp.ARCHIVE_DIR / file)
            print(f"Archived: {input_address}")
            
        else:
            print(f"\nWARNING")
            print(f"Tried: {input_address}")
            print(f'Address does not exist\n')
    print('============================================\n')
        
    sp.INPUT_RR_ADDR.rename(sp.ARCHIVE_DIR / sp.INPUT_RR_FILE)
    print('\n============================================')
    print(f"Archived: \n{sp.INPUT_RR_FILE}")
    print('============================================\n')
            
    # list should begin with most recent items
    # more efficient search for items to edit above
    record_dict['prev_files'].sort(reverse= True)
    record_dict['prev_used_files'].sort(reverse= True)
    record_dict['output_proj_files'].sort(reverse= True)
            
# store record_dict
    with sp.RECORD_DICT_ADDR.open('w') as f:
        json.dump(record_dict, f)
    print('\n====================================================')
    print('Saved record_dict to file')
    print(f'{sp.RECORD_DICT_ADDR}')
    print(f'\nlatest_used_file: {record_dict['latest_used_file']}\n')
    print(f'output_proj_files: \n{record_dict['output_proj_files'][:6]}\n')
    print(f'prev_used_files: \n{record_dict['prev_used_files'][:6]}\n')
    print(f'prev_files: \n{record_dict['prev_files'][:6]}\n')
    print(f'proj_yr_qtrs: \n{record_dict['proj_yr_qtrs'][:6]}\n')
    print('====================================================\n')
 
    print('\n====================================================')
    print('Retrieval is complete\n')
    
    n = len(files_to_read_list)
    m = len(failure_to_read_lst)
    print(f'{n} new input files read and saved')
    print(f'from {sp.INPUT_DIR}')
    print(f'  to {sp.OUTPUT_DIR}\n')
    print(f'{m} files not read and saved:\n')
    print(failure_to_read_lst)
    print('====================================================')

if __name__ == '__main__':
    update_data_files()