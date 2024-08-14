import polars as pl
import sp500_pe.helper_func as hp
from copy import deepcopy
import sys

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
    df = df.with_columns(pl.Series(
                            [fwd_12m_ern(eps, p_dict[yrqtr])
                            for yrqtr in df['year_qtr']])
                         .alias(name_proj))\
           .cast({name_proj: pl.Float32})\
           .select('year_qtr', name_proj, 'price', 'real_rate')
    return df


def fwd_12m_ern(name, p_df):
    '''
        calculate "contemporaneous" projection of 12m fwd earn
        for date key of p_df, the DF of projections for a specific date
        p_df is a pandas DF that contains the projections
    '''
    # ensure the year_qtrs are ascending as iter down the rows
    # from the current 'year_qtr'
    p_df = p_df.sort(by= 'year_qtr')
    fwd_e = sum((p_df.item(id, name)
                 for id in range(4)))
    return fwd_e


def page0_df(df, p_dict, p_dict_columns, name_act):
    '''
    # returns new DF with cols named for each cy
    #   also year_qtr and actual cy
    #       0) year_qtr (from df)
    #       1) projections for current year's cy E (from p_dict)
    #       2) projections for next year's cy E (ditto)
    #       3) actual cy, null except for Q4 (from df)
    # for each year_qtr's proj key; fetch projections,
    #       group by year
    #       enter the value of the projection for the date 
    #       of the projection in data in the column named for the 
    #       future year
    '''
    
    # create 2cols 
    #   actual_op and actual_rep 12m eps for each yr,
    #   which appears only in the 4th qtr, otherwise null
    hf = df.select(pl.col(name_act),
                   pl.col('year_qtr'))\
                .filter(pl.col('year_qtr')
                        .map_batches(hp.is_quarter_4))\
                .join(df,
                      how= 'right',
                      on= 'year_qtr',
                      coalesce= True)\
                .select(pl.col(name_act),
                        pl.col('year_qtr'))

    # for each year_qtr in df, fetch its proj_df from p_dict
    # filter to select 12m proj in Q4s
    # join with df on year_qtr
    
    # name of the col of e from proj from list: op or rep?
    name_proj = p_dict_columns[0]
    
    for idx, yrqtr in enumerate(df['year_qtr']):
        # target year_qtr, place in col for filtered pro_df
        pro_df = p_dict[yrqtr]\
                    .select(p_dict_columns)\
                    .filter(pl.col('year_qtr')
                            .map_batches(hp.is_quarter_4))\
                    .with_columns(pl.col('year_qtr')
                                      .map_batches(hp.yrqtr_to_yr)
                                      .alias('year'),
                                  pl.lit(yrqtr).alias('year_qtr'))
                    
        # remove any projections for previous year from Q1
        if yrqtr[-2:] == 'Q1':
            pro_df = pro_df.filter(pl.col('year')>= yrqtr[0:4])
        
        # accumulate rows for the projection DF for each year_qtr  
        if idx == 0:
            p_df = deepcopy(pro_df)
        else:
            p_df = pl.concat([p_df, pro_df],
                             how= 'vertical')
    
    # pivot years into column names for each year_qtr
    p_df = p_df.pivot(index= 'year_qtr',
                      columns= 'year',
                      values= name_proj)
    
    # build DF to return for plotting
    p_df = hf.select(['year_qtr', 
                      name_act])\
             .join(p_df,
                   on= 'year_qtr',
                   how= 'left',
                   coalesce= True)
    return p_df


def  page1_df(df, p_df, eps, ROG):
    # find most recent price from projection df
   
    base_price = pl.Series(
        p_df.select(pl.col('price'))\
            .filter(pl.col('price').is_not_null())
        ).to_list()[0]
    
    q_grate = (1+ROG) ** 0.25
    
    p_df = p_df.with_columns(pl.lit(base_price)
                             .alias('fixed_price'))\
               .sort(by= 'year_qtr')\
               .with_columns(pl.Series(
                                       [base_price * q_grate**idx
                                        for idx in range(len(p_df))])
                                .alias('incr_price'))\
               .with_columns((pl.col('fixed_price')/
                              pl.col(eps))\
                                .alias('fix_proj_p/e'),
                             (pl.col('incr_price')/
                              pl.col(eps))\
                                .alias('incr_proj_p/e'))                      
    
    hf = df.with_columns((pl.col('price') /
                          pl.col(eps))
                            .alias('pe'))\
           .join(p_df,
                 on= 'year_qtr',
                 how= 'full',
                 coalesce= True)\
           .sort(by= 'year_qtr')\
           .select(['year_qtr', 'pe',
                    'fix_proj_p/e', 'incr_proj_p/e'])
    return hf

def page3_df(df, name_12m_fwd_eps):
    
    hf = df.with_columns((pl.col(name_12m_fwd_eps) * 100 /
                          pl.col('price'))
                            .alias('earnings / price'))\
           .with_columns((pl.col('earnings / price') -
                          pl.col('real_rate'))
                            .alias('equity premium'))\
           .rename({'real_rate': '10-year TIPS rate'})\
           .select('year_qtr', 'earnings / price', 'equity premium',
                   '10-year TIPS rate')\
           .sort(by= 'year_qtr')
    return hf 
