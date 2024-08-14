import sp500_pe as sp
import sp500_pe.display_helper_func as dh
import sp500_pe.plot_func as pf
import sp500_pe.helper_func as hp

import sys
from pathlib import Path

import polars as pl
import json
import matplotlib.pyplot as plt


#=================  Global Parameters  ================================

# main titles for displays
page0_suptitle = " \nPrice-Earnings Ratios for the S&P 500"
proj_eps_suptitle = " \nCalendar-Year Earnings per Share for the S&P 500"
page2_suptitle = " \nEarnings Margin and Equity Premium for the S&P 500"
page3_suptitle = \
    " \nS&P 500 Earnings Yield, 10-Year TIPS Rate, and Equity Premium"

# str: source footnotes for displays
e_data_source = \
    'https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all'
rr_data_source = '10-year TIPS: latest rate for each quarter,' + \
    ' Board of Governors of the Federal Reserve System, ' + \
    '\nMarket Yield on U.S. Treasury Securities at 10-Year' + \
    ' Constant Maturity, Investment Basis, Inflation-Indexed,' +\
    '\nfrom Federal Reserve Bank of St. Louis, FRED [DFII10].'
page0_source = e_data_source
page1_source = e_data_source
page2_source = e_data_source + '\n\n' + rr_data_source
page3_source = e_data_source + '\n\n' + rr_data_source

# hyopothetical rate of appreciation for future stock prices
ROG = 0.05

hist_col_names = ['date', 'price', 'op_eps', 'rep_eps',
                'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps',
                'op_margin', 'real_int_rate']

data_cols_rename  = {'op_margin': 'margin',
                    'real_int_rate': 'real_rate'}


# ================  MAIN =============================================+

# https://mateuspestana.github.io/tutorials/pandas_to_polars/
# https://www.rhosignal.com/posts/polars-pandas-cheatsheet/
# https://www.rhosignal.com/tags/polars/
# https://jrycw.github.io/ezp2p/
# https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.filter.html
# https://fralfaro.github.io/DS-Cheat-Sheets/examples/polars/polars/

def display_data():
    
# read record_df
    if sp.RECORD_DICT_ADDR.exists():
        with sp.RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)
        print('\n============================================')
        print(f'Read record_dict from: \n{sp.RECORD_DICT_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No record_dict in \n{sp.RECORD_DICT_ADDR.name}')
        print(f'at: \n{sp.RECORD_DICT_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
    # provide the date of projection
    date_lst = record_dict['latest_used_file']\
        .split('.')[0][-10:].split(' ')
    date_this_projn = f'{date_lst[1]}-{date_lst[2]}-{date_lst[0]}'
    yr_qtr_current_projn = f'{date_lst[0]}-Q{
                            (int(date_lst[1]) - 1) // 3 + 1}'
    
 # read hist_df
    if sp.OUTPUT_HIST_ADDR.exists():
        with sp.OUTPUT_HIST_ADDR.open('r') as f:
            data_df = pl.read_parquet(source= f,
                                      columns= hist_col_names)\
                        .with_columns(pl.col('date')
                                .map_batches(hp.date_to_year_qtr)
                                .alias('year_qtr'))
        print('\n============================================')
        print(f'Read data history from: \n{sp.OUTPUT_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No data history in: \n{sp.OUTPUT_HIST_ADDR.name}')
        print(f'at: \n{sp.OUTPUT_HIST_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
# create dict for projections dict, 'year_qtr' of projections are keys
    data_df = pl.DataFrame(record_dict['proj_yr_qtrs'],
                           schema= ['year_qtr'])\
               .join(data_df,
                     on= 'year_qtr',
                     how= 'left',
                     coalesce= True)
               
# +++++ values in 'year_qtr' are the values for the "x-axis", all graphs

# +++++ read proj dfs +++++++++++++++++++++++++++++++++++++++++++++++++
# put dfs in proj_dict, key = dfs' 'year_qtr' value (from file.name)
    proj_dict = dict()
    for file_name, yr_qtr in zip(record_dict['output_proj_files'],
                                 record_dict['proj_yr_qtrs']):
        file_addr = sp.OUTPUT_PROJ_DIR / file_name
        if file_addr.exists():
            with file_addr.open('r') as f:
                gf = pl.read_parquet(f)
            proj_dict[yr_qtr] = gf
        else:
            print('\n============================================')
            print(f'No output file at \n{file_addr.name}')
            print(f'in: \n{file_addr}')
            print('Processing ended')
            print('============================================\n')

# DISPLAY THE DATA ====================================================
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.subplot_mosaic.html
    # https://matplotlib.org/stable/api/axes_api.html
    # https://matplotlib.org/stable/api/axes_api.html#axes-position

# +++++ create the y-values (columns) for the series to be plotted

# page zero  ======================
# shows:  projected eps for current cy and future cy
# the projections shown for each quarter are the latest
# made in the quarter

# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{proj_eps_suptitle}\n{date_this_projn}',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(page0_source, fontsize= 8)

# subsets of columns for op eps (top panel)
    df = data_df.select(['year_qtr', '12m_op_eps'])
    p_dict_columns = ['12m_op_eps', 'year_qtr']
    df = dh.page0_df(df, proj_dict, p_dict_columns, '12m_op_eps')\
                .rename({'12m_op_eps': 'actual'})\
                .sort(by= 'year_qtr')
    
    xlabl = '\ndate of projection\n'
    ylabl = '\nearnings per share\n'
    
    pf.plots_page0(ax['operating'], df,
                title= ' \nProjections of Operating EPS',
                ylim= (100, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
# subsets of columns for rep eps (bottom panel)
    df = data_df.select(['year_qtr', '12m_rep_eps'])
    p_dict_columns = ['12m_rep_eps', 'year_qtr']
    df = dh.page0_df(df, proj_dict, p_dict_columns, '12m_rep_eps')\
                .rename({'12m_rep_eps': 'actual'})\
                .sort(by= 'year_qtr')
    
    pf.plots_page0(ax['reported'], df,
                title= ' \nProjections of Reported EPS',
                ylim= (75, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
# show the figure
    print('\n============================')
    print(sp.DISPLAY_0_ADDR)
    print('============================\n')
    fig.savefig(str(sp.DISPLAY_0_ADDR))
    
    del df
    
# page one  ======================
# shows:  historical 12m trailing pe plus
#    forward 12m trailing pe, using current p

# create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{page0_suptitle}\n{date_this_projn}\n ',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(page1_source, fontsize= 8)
    
# create the top and bottom graphs for op and rep pe
# new DF with cols for p/e and alt p/e, both using 12m trailing E
    #   also year_qtr and actual cy
    #       0) year_qtr (from df) 
    #       1) historical 12m trailing p/e (from df)
    #       2) alt1 using constant p for proj quarters
    #       3) alt2 using p growing at ROG for proj quarters
    #       4) rolling 12m E (hist+proj) for proj quarters
    
    # top panel
    df = data_df.select(['year_qtr', '12m_op_eps', 'price'])
    
    p_df = proj_dict[yr_qtr_current_projn]\
               .select(['year_qtr', '12m_op_eps', 'price'])
    
    df = dh.page1_df(df, p_df, '12m_op_eps', ROG )
    
    denom = 'divided by projected earnings'
    legend1 = f'price constant from {date_this_projn}\n{denom}'
    legend2 = f'price increases 5% ar from {date_this_projn}\n{denom}'
    
    df = df.rename({'pe': 'historical',
               'fix_proj_p/e': legend1,
               'incr_proj_p/e': legend2})
    
    title = 'Ratio: Price to 12-month Trailing Operating Earnings'
   
    pf.plots_page1(ax['operating'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')

    # bottom panel
    df = data_df.select(['year_qtr', '12m_rep_eps', 'price'])
    
    p_df = proj_dict[yr_qtr_current_projn]\
               .select(['year_qtr', '12m_rep_eps', 'price'])
    
    df = dh.page1_df(df, p_df, '12m_rep_eps', ROG )
    
    df = df.rename({'pe': 'historical',
                    'fix_proj_p/e': legend1,
                    'incr_proj_p/e': legend2})
    
    title = 'Ratio: Price to 12-month Trailing Reported Earnings'
    
    pf.plots_page1(ax['reported'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')
    
    print('\n============================')
    print(sp.DISPLAY_1_ADDR)
    print('============================\n')
    fig.savefig(str(sp.DISPLAY_1_ADDR))
    
    del df
    
# page two  ======================
# shows:  historical data for margins and 
# historical and current estimates for equity premium
    
    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['margin'],
                             ['premium']])
    fig.suptitle(
        f'{page2_suptitle}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(page2_source, fontsize= 8)
    
    # create the top and bottom graphs for margins and premiums
    # create working df for op margins (top panel)

    df = data_df.rename({'op_margin' : 'margin'})\
                .select('year_qtr', 'margin')\
                .with_columns((pl.col('margin') * 100)
                            .alias('margin100'))\
                .drop('margin')\
                .rename({'margin100': 'margin'})\
                .sort(by= 'year_qtr')
    
    title = 'Margin: quarterly operating earnings relative to revenue'
    
    pf.plots_page2(ax['margin'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ')

# create working df for premia (bottom panel)
    df = data_df.rename({'real_int_rate' : 'real_rate'})\
                .select('year_qtr', '12m_rep_eps', 
                        'real_rate', 'price')\
                .with_columns(((pl.col('12m_rep_eps') /
                                pl.col('price')) * 100 -
                               pl.col('real_rate'))
                                    .alias('premium'))\
                .drop('12m_rep_eps', 'real_rate', 'price')\
                .sort(by= 'year_qtr')

    title = 'Equity Premium: \nratio of 12-month trailing reported earnings to price, '
    title += 'less 10-year TIPS rate'

    pf.plots_page2(ax['premium'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ')
    
    print('\n============================')
    print(sp.DISPLAY_2_ADDR)
    print('============================\n')
    fig.savefig(str(sp.DISPLAY_2_ADDR))
    #plt.savefig(f'{output_dir}/eps_page2.pdf', bbox_inches='tight')
    
    del(df)
    
# page three  ======================
# shows:  components of the equity premium,
# using 12m forward projected earnings
    
    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # upper and lower plots
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{page3_suptitle}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(page3_source, fontsize= 8)
    
    # create the top and bottom graphs for premiums
    data_df = data_df.rename({'real_int_rate' : 'real_rate'})
    
    # create working df for op premium (top panel)
    # add a col: proj eps over the next 4 qtrs
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 'op_eps', 'fwd_12mproj_op_eps')
    df = dh.page3_df(df, 'fwd_12mproj_op_eps')
    
    title = 'Operating Earnings: projections for 12-months forward'

    pf.plots_page3(ax['operating'], df,
                ylim= (None, 9),
                title= title,
                ylabl= ' \npercent\n ',
                xlabl= ' \n ')
    
    # bottom panel
    # add a col : proj eps over the next 4 qtrs
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 'rep_eps', 'fwd_12mproj_rep_eps')
    df = dh.page3_df(df, 'fwd_12mproj_rep_eps')
    
    title = 'Reported Earnings: projections for 12-months forward'

    pf.plots_page3(ax['reported'], df,
                ylim= (None, 9),
                title= title,
                ylabl= ' \npercent\n ',
                xlabl= ' \n ')
    
    print('\n============================')
    print(sp.DISPLAY_3_ADDR)
    print('============================\n')
    fig.savefig(str(sp.DISPLAY_3_ADDR))
    #plt.savefig(f'{output_dir}/eps_page3.pdf', bbox_inches='tight')
    
    sys.exit()
# the following assumes that data_df is sorted
# the following isolates the year_qtrs that have no actual E
# only the proj_dict has price and real_rate for these qtrs
    # the null values occur only at the "tail" of data_df
    # add new data for data_df in the tail rows where E is null
    for idx, dt in enumerate(key_lst):
        kdx = len(data_df) - len(key_lst) + idx
        data_df[kdx, 'price'] = proj_dict[dt]['price']
        data_df[kdx, 'real_rate'] = proj_dict[dt]['real_rate']
    del key_lst
    
# add "contemporaneous" 12m fwd earn proj for each qtr to data_df
    # fetch list of fwd op earn, add list as col to data_df
    data_df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                   'op_eps', 'fwd_12mproj_op_eps')
    data_df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                   'rep_eps', 'fwd_12mproj_rep_eps')

# assumes that data_df is sorted ascending by 'year_qtr'
# print the latest quarter's latest_proj_key
    date_this_projn = data_df[len(data_df) - 1, 
                              'latest_proj_key']
    print('\n=================================')
    print(f"latest projection: \n{date_this_projn}")
    print('=================================')


if __name__ == '__main__':
    display_data()
    