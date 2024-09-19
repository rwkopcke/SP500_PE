from copy import deepcopy
import gc
import sys

import polars as pl

import sp500_pe.helper_func as hp


'''
def create_yq_str(dt_itrble):
    str_lst = [(date.strftime("%Y"), date.strftime("%m"))
               for date in dt_itrble]
    return [f'{item[0]}-Q{(int(item[1]) - 1) // 3 + 1}'
            for item in str_lst]
'''


def contemp_12m_fwd_proj(df, p_dict, eps, name_proj):
    '''add col to df that contains
       projected E over the next 4 quarters
    '''
    # put 12m fwd projection in new col name_proj
    # for all qtrs in df
    df = df.with_columns(pl.Series(
                    [fwd_12m_ern(eps, p_dict[yrqtr])
                     for yrqtr in df['yr_qtr']])
                         .alias(name_proj))\
           .cast({name_proj: pl.Float32})
    return df


def fwd_12m_ern(name, p_df):
    '''
        calculate "contemporaneous" projection of 12m fwd earn
        for date key of p_df, the DF of projections for a specific date
        p_df is a pandas DF that contains the projections
    '''
    # ensure the yr_qtrs are ascending to sum down the rows
    # from the current 'yr_qtr'
    p_df = p_df.sort(by= 'yr_qtr')
    fwd_e = sum((p_df.item(id, name)
                 for id in range(4)))
    del p_df
    gc.collect()
    return fwd_e


def page0_df(df, p_dict, p_dict_columns, name_act):
    '''
    # returns new DF with cols named for each cy
    #   also yr_qtr and actual cy
    #       0) yr_qtr (from df)
    #       1) projections for current year's cy E (from p_dict)
    #       2) projections for next year's cy E (ditto)
    #       3) actual cy, null except for Q4 (from df)
    # for each yr_qtr's proj key; fetch projections,
    #       group by year
    #       enter the value of the projection for the date 
    #       of the projection in data in the column named for the 
    #       future year
    '''
    
    # create 2cols 
    #   actual_op and actual_rep 12m eps for each yr,
    #   which appears only in the 4th qtr, otherwise null
    hf = df.select(pl.col(name_act),
                   pl.col('yr_qtr'))\
                .filter(pl.col('yr_qtr')
                        .map_batches(hp.is_quarter_4))\
                .join(df,
                      how= 'right',
                      on= 'yr_qtr',
                      coalesce= True)\
                .select(pl.col(name_act),
                        pl.col('yr_qtr'))

    # for each yr_qtr in df, fetch its proj_df from p_dict
    # filter to select 12m proj in Q4s
    # join with df on yr_qtr
    
    # name of the col of e from proj from list: op or rep?
    for idx, yrqtr in enumerate(df['yr_qtr']):
        # target yr_qtr, place in col for filtered pro_df
        pro_df = p_dict[yrqtr]\
                    .select(p_dict_columns)\
                    .filter(pl.col('yr_qtr')
                            .map_batches(hp.is_quarter_4))\
                    .with_columns(pl.col('yr_qtr')
                                      .map_batches(hp.yrqtr_to_yr)
                                      .alias('year'),
                                  pl.lit(yrqtr).alias('yr_qtr'))
                    
        # remove any projections for previous year from Q1
        if yrqtr[-2:] == 'Q1':
            pro_df = pro_df.filter(pl.col('year')>= yrqtr[0:4])
        
        # accumulate rows for the projection DF for each yr_qtr  
        if idx == 0:
            p_df = deepcopy(pro_df)
        else:
            p_df = pl.concat([p_df, pro_df],
                             how= 'vertical')
    
    # pivot years into column names for each yr_qtr
    p_df = p_df.pivot(index= 'yr_qtr',
                      columns= 'year')
    
    # build DF to return for plotting
    p_df = hf.select(['yr_qtr', 
                      name_act])\
             .join(p_df,
                   on= 'yr_qtr',
                   how= 'left',
                   coalesce= True)
    del hf
    del pro_df
    gc.collect()
    return p_df


def  page1_df(df, p_df, eps, ROGQ):
    
    # find most recent price from projection df
    df = df.with_columns((pl.col('price') / pl.col(eps))
                            .alias('pe'))\
           .sort(by= 'yr_qtr')
    base_price = \
        df.filter(pl.col(eps).is_not_null())[-1, 'price']

    # build projected df for graph from df and p_df
    p_df = p_df.with_columns(pl.lit(base_price)
                               .alias('fixed_price'))\
               .sort(by= 'yr_qtr')\
               .with_columns(pl.Series(
                                    [base_price * ROGQ**idx
                                     for idx in range(len(p_df))])
                                .alias('incr_price'))\
               .with_columns((pl.col('fixed_price') / pl.col(eps))
                                .alias('fix_proj_p/e'),
                             (pl.col('incr_price') / pl.col(eps))
                                .alias('incr_proj_p/e'))                      
    df = df.join(p_df,
                 on= 'yr_qtr',
                 how= 'full',
                 coalesce= True)\
           .sort(by= 'yr_qtr')\
           .select(['yr_qtr', 'pe',
                    'fix_proj_p/e', 'incr_proj_p/e'])
    return df

def page3_df(df, name_12m_fwd_eps):
    
    hf = df.with_columns((pl.col(name_12m_fwd_eps) * 100 /
                          pl.col('price'))
                            .alias('earnings / price'))\
           .with_columns((pl.col('earnings / price') -
                          pl.col('real_int_rate'))
                            .alias('equity premium'))\
           .rename({'real_int_rate': '10-year TIPS rate'})\
           .select('yr_qtr', 'earnings / price', 'equity premium',
                   '10-year TIPS rate')\
           .sort(by= 'yr_qtr')
    return hf 
