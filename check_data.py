# Packages
import pandas as pd
import datetime as dtt

path_data = '/Users/areguly6/Dropbox (GaTech)/VIP_ML_for_Finance/Chen_Ha_Cho_Dou_Lev_replication/data/'

# Import CRSP: 
#   PERMNO is security identifier for CRSP, SHRCD is security type, 
#   SICCD is SIC industry categorization. NCUSIP is historical security identifier.
#   COMNAM is company name, PRIMEXCH is primary exchange, CUSIP is current security identifier
#   PRC is price, VOL is volume, SHROUT is shares outstanding, vwretd and ewretd are value and equally weighted market return averages.
#
# you will need date and NCUSIP to identify mainly these companies. May use Companyname and or Ticker with XBRL if there is no/partial CUSIP identifier
df_crsp = pd.read_csv(path_data+'crsp_monthly.csv')
df_crsp['date'] = pd.to_datetime(df_crsp['date'])

# Import IBES
df_ibes = pd.read_csv(path_data+'ibes_annual.csv')


# YOU SHOULD MERGE IT ON XBRL data, based on the process described in Table 1
# but here is an example how to merge them together:

# Select needed columns
df_crsp_m = df_crsp[['date','NCUSIP','COMNAM','PRC','SHROUT','vwretd','ewretd']]
df_crsp_m.shape
df_ibes_m = df_ibes[['CUSIP','CNAME','date','VALUE']]
df_ibes_m.shape

# Merge: inner (both has to be in data, note you have to modify it to be in XBRL, and it is not the same stuff necessarily)
df = pd.merge(df_ibes_m, df_crsp_m, how='inner', left_on=['date', 'CUSIP'], right_on=['date','NCUSIP'])

# All obs from CRPS and IBES
df.shape

# Drop duplicates
df = df.drop_duplicates(subset=['date','NCUSIP'])
df.shape

# Available stock price from CRSP (non NA)
df2 = df.dropna(subset=['PRC'])
df2.shape

##
# NOTES: a) you need to carefully investigate and remove the duplicates form CRSP and IBES and XBRL individually! Check the sample sizes! E.g. here you will end up with way less observations