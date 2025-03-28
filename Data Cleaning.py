# -*- coding: utf-8 -*-
"""

## All the links for reprice

Looker:
https://lookerstudio.google.com/u/0/reporting/078c8c24-8210-462a-a814-e31ee1066a34/page/p_e4t9i225ad

Settlement: https://seller.walmart.com/wfsLite/reports


Order Report: https://seller.walmart.com/order-management/details?orderGroups=All

Inventory:
https://seller.walmart.com/wfsLite/manage-inventory

Inbound receipts:
https://seller.walmart.com/wfsLite/reports
"""

from google.colab import drive
drive.mount('/content/drive')

# import packages
!pip install pytz
import pytz
import math
import numpy as np
import pandas as pd
from datetime import datetime
from google.colab import drive
drive.mount('/content/drive')
import matplotlib.pyplot as plt
import gc

"""## 1. Looker & Admin

"""

# read looker
df_looker= pd.read_csv('/content/drive/MyDrive/Reprice/Walmart_Reprice NEW_Table.csv')

# drop vars
df_looker.drop('avg_cost', axis=1, inplace=True)

# --- Vendor ---
# export vender nan to excel sheet2
df_nan = df_looker[df_looker['Vendor'].isna()]

# remove those obs with vendor nan
df_looker = df_looker.dropna(subset=['Vendor'])

# Avg cost (admin)
# select vars
df_admin= pd.read_csv('/content/drive/MyDrive/Reprice/all product costs.csv')[['ASIN','Avg Cost']]

# merge
df_looker = df_looker.merge(df_admin, on='ASIN', how='left')

# if admin nan, take vendor price
df_looker['Avg Cost'].fillna(df_looker['vendor_price'], inplace=True)

# 清掉df_admin變數，釋放RAM
del df_admin
gc.collect()

# remove the duplicates SKU
df_looker.sort_values(by=['SKU', 'Avg Cost'], ascending=[True, False], inplace=True)
df_looker.drop_duplicates(subset='SKU', keep='first', inplace=True)

# Lowest_amazon_price
# replace nan with 0
df_looker['lowest_amazon_price'].fillna(0, inplace=True)

# MAP, Is_map_enforced
df_looker['map'].isna()
df_looker.loc[(df_looker['map'].isna()), 'map'] = df_looker['resolved_map']


# CA+ DB
# CA
indices= df_looker[df_looker['Vendor']== 'Cathedral Art'].index

for index in indices:
    df_looker.at[index, 'map'] = df_looker.at[index, 'vendor_price'] * 2 + 1.99
    df_looker.at[index, 'is_map_enforced'] = True

# DB
df_db= pd.read_excel('/content/drive/MyDrive/Reprice/MAP.xlsx')[['SKU','MAP']]
# merge df_looker
df_looker = df_looker.merge(df_db, on='SKU', how='left')
# replace
df_list = df_db['SKU'].tolist()
df_looker['map'] = df_looker.apply(lambda row: row['MAP'] if row['SKU'] in df_list else row['map'], axis=1)
# drop map
df_looker.drop('MAP', axis=1, inplace=True)

# 清掉df_db變數，釋放RAM
del df_db
gc.collect()

"""## 2. Shipping Fee"""

# new settlement15
df15= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202503.csv', skiprows=3)
df15['Partner GTIN']= df15['Partner GTIN'].replace(['"', '='], '', regex=True)
df15['Partner GTIN'] = pd.to_numeric(df15['Partner GTIN'], errors='coerce', downcast='integer')
df15['Walmart.com PO #']= df15['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df15= df15.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement14
df14= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202502.csv', skiprows=3)
df14['Partner GTIN']= df14['Partner GTIN'].replace(['"', '='], '', regex=True)
df14['Partner GTIN'] = pd.to_numeric(df14['Partner GTIN'], errors='coerce', downcast='integer')
df14['Walmart.com PO #']= df14['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df14= df14.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement13
df13= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202501.csv', skiprows=3)
df13['Partner GTIN']= df13['Partner GTIN'].replace(['"', '='], '', regex=True)
df13['Partner GTIN'] = pd.to_numeric(df13['Partner GTIN'], errors='coerce', downcast='integer')
df13['Walmart.com PO #']= df13['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df13= df13.rename(columns={'Walmart.com PO #': 'PO Number'})


# new settlement12
df12= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202412.csv', skiprows=3)
df12['Partner GTIN']= df12['Partner GTIN'].replace(['"', '='], '', regex=True)
df12['Partner GTIN'] = pd.to_numeric(df12['Partner GTIN'], errors='coerce', downcast='integer')
df12['Walmart.com PO #']= df12['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df12= df12.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement11
df11= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202411.csv', skiprows=3)
df11['Partner GTIN']= df11['Partner GTIN'].replace(['"', '='], '', regex=True)
df11['Partner GTIN'] = pd.to_numeric(df11['Partner GTIN'], errors='coerce', downcast='integer')
df11['Walmart.com PO #']= df11['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df11= df11.rename(columns={'Walmart.com PO #': 'PO Number'})


# new settlement10
df10= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202410.csv', skiprows=3)
df10['Partner GTIN']= df10['Partner GTIN'].replace(['"', '='], '', regex=True)
df10['Partner GTIN'] = pd.to_numeric(df10['Partner GTIN'], errors='coerce', downcast='integer')
df10['Walmart.com PO #']= df10['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df10= df10.rename(columns={'Walmart.com PO #': 'PO Number'})


# new settlement9
df9= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202409.csv', skiprows=3)
df9['Partner GTIN']= df9['Partner GTIN'].replace(['"', '='], '', regex=True)
df9['Partner GTIN'] = pd.to_numeric(df9['Partner GTIN'], errors='coerce', downcast='integer')
df9['Walmart.com PO #']= df9['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df9= df9.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement8
df8= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202408.csv', skiprows=3)
df8['Partner GTIN']= df8['Partner GTIN'].replace(['"', '='], '', regex=True)
df8['Partner GTIN'] = pd.to_numeric(df8['Partner GTIN'], errors='coerce', downcast='integer')
df8['Walmart.com PO #']= df8['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df8= df8.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement7
df7= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202407.csv', skiprows=3)
df7['Partner GTIN']= df7['Partner GTIN'].replace(['"', '='], '', regex=True)
df7['Partner GTIN'] = pd.to_numeric(df7['Partner GTIN'], errors='coerce', downcast='integer')
df7['Walmart.com PO #']= df7['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df7= df7.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement6
df6= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202406.csv', skiprows=3)
df6['Partner GTIN']= df6['Partner GTIN'].replace(['"', '='], '', regex=True)
df6['Partner GTIN'] = pd.to_numeric(df6['Partner GTIN'], errors='coerce', downcast='integer')
df6['Walmart.com PO #']= df6['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df6= df6.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement5
df5= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202405.csv', skiprows=3)
df5['Partner GTIN']= df5['Partner GTIN'].replace(['"', '='], '', regex=True)
df5['Partner GTIN'] = pd.to_numeric(df5['Partner GTIN'], errors='coerce', downcast='integer')
df5['Walmart.com PO #']= df5['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df5= df5.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement4
df4= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202404.csv', skiprows=3)
df4['Partner GTIN']= df4['Partner GTIN'].replace(['"', '='], '', regex=True)
df4['Partner GTIN'] = pd.to_numeric(df4['Partner GTIN'], errors='coerce', downcast='integer')
df4['Walmart.com PO #']= df4['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df4= df4.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement3
df3= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0312.csv', skiprows=3)
df3['Partner GTIN']= df3['Partner GTIN'].replace(['"', '='], '', regex=True)
df3['Partner GTIN'] = pd.to_numeric(df3['Partner GTIN'], errors='coerce', downcast='integer')
df3['Walmart.com PO #']= df3['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df3= df3.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement2
df2= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0312.csv', skiprows=3)
df2['Partner GTIN']= df2['Partner GTIN'].replace(['"', '='], '', regex=True)
df2['Partner GTIN'] = pd.to_numeric(df2['Partner GTIN'], errors='coerce', downcast='integer')
df2['Walmart.com PO #']= df2['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df2= df2.rename(columns={'Walmart.com PO #': 'PO Number'})

# new settlement
df0= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0227.csv', skiprows=3)
df0['Partner GTIN']= df0['Partner GTIN'].replace(['"', '='], '', regex=True)
df0['Partner GTIN'] = pd.to_numeric(df0['Partner GTIN'], errors='coerce', downcast='integer')
df0['Walmart.com PO #']= df0['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df0= df0.rename(columns={'Walmart.com PO #': 'PO Number'})

# shipping fee
df1= pd.read_csv('/content/drive/MyDrive/Reprice/settlement.csv', skiprows=3)
df1['Partner GTIN']= df1['Partner GTIN'].replace(['"', '='], '', regex=True)
df1['Partner GTIN'] = pd.to_numeric(df1['Partner GTIN'], errors='coerce', downcast='integer')
df1['Walmart.com PO #']= df1['Walmart.com PO #'].replace(['"', '='], '', regex=True)
df1= df1.rename(columns={'Walmart.com PO #': 'PO Number'})

# concat
df1= pd.concat([df0, df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15], ignore_index=True)


# shipping fee%
df_pl_inbound= df1[df1['Transaction Type']== 'InboundTransportationFee']
df_pl_inbound= df_pl_inbound.groupby('PO Number')['Net Payable'].sum().reset_index()

# inbound receipts
df_inbound= pd.read_csv('/content/drive/MyDrive/Reprice/inboundReceipts.csv')
df_inbound['GTIN']= df_inbound['GTIN'].replace(['"', '='], '', regex=True)
df_inbound['GTIN'] = pd.to_numeric(df_inbound['GTIN'], errors='coerce', downcast='integer')

# merge
df_inbound= df_inbound.merge(df_looker[['GTIN','Avg Cost','Vendor']], on='GTIN', how='left')
df_inbound= df_inbound[~df_inbound['Vendor'].isna()]
df_inbound['Inventory Value']= df_inbound['Received Units']*df_inbound['Avg Cost']
df_inbound.head(2)

# return max vendor
df_maxvendor= df_inbound.groupby('PO Number')['Vendor'].apply(lambda x: x.value_counts().idxmax()).reset_index()

# inven value
df_valuevendor= df_inbound.groupby('PO Number')['Inventory Value'].sum().reset_index()

# merge vendor, inven value
df_inbound= df_maxvendor.merge(df_valuevendor, how='left',on='PO Number')
df_inbound.head(3)

del df_valuevendor
gc.collect()

# merge settlement, inbound
df_pl_inbound= df_pl_inbound.merge(df_inbound, how='left',on='PO Number')

# group by shipping no and return sum
df_pl_inbound= df_pl_inbound.groupby('Vendor').agg({'Net Payable': 'sum','Inventory Value': 'sum'}).reset_index()

# shipping fee&%
df_pl_inbound['Shipping%']= df_pl_inbound['Net Payable']/df_pl_inbound['Inventory Value']

# add comfy brands
 df_pl_inbound = pd.concat([df_pl_inbound, df_comfy], ignore_index=True)

 # fillin with comfy hour %
 df_pl_inbound.loc[df_pl_inbound['Vendor'].isin(comfy_list), 'Shipping%'] = df_pl_inbound.loc[df_pl_inbound['Vendor'] == 'Comfy Hour', 'Shipping%'].values[0]
 df_ship= df_pl_inbound[['Vendor','Shipping%']]

# 10/29 fix_檢查 'Comfy Hour' 是否存在於 df_pl_inbound['Vendor']
if not df_pl_inbound.loc[df_pl_inbound['Vendor'] == 'Comfy Hour', 'Shipping%'].empty:
    comfy_hour_shipping = df_pl_inbound.loc[df_pl_inbound['Vendor'] == 'Comfy Hour', 'Shipping%'].values[0]
    df_pl_inbound.loc[df_pl_inbound['Vendor'].isin(comfy_list), 'Shipping%'] = comfy_hour_shipping
else:
    # 如果找不到 'Comfy Hour'，可以選擇填入預設值，例如 0 或 np.nan
    df_pl_inbound.loc[df_pl_inbound['Vendor'].isin(comfy_list), 'Shipping%'] = 0
    df_ship= df_pl_inbound[['Vendor','Shipping%']]

# make vendors lowercase to match pa tracking
df_ship['Vendor'] = df_ship['Vendor'].str.lower()
df_looker['Vendor'] = df_looker['Vendor'].str.lower()

# merge
df = df_looker.merge(df_ship, on='Vendor', how='left')

# if charge nan fill 0
df['Shipping%'] = df['Shipping%'].fillna(0.05)

"""## 3. Settlement & Inventory"""

# new settlement15
df15= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202503.csv', skiprows=3)
df15['Partner GTIN']= df15['Partner GTIN'].replace(['"', '='], '', regex=True)
df15['Partner GTIN'] = pd.to_numeric(df15['Partner GTIN'], errors='coerce', downcast='integer')


# new settlement14
df14= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202502.csv', skiprows=3)
df14['Partner GTIN']= df14['Partner GTIN'].replace(['"', '='], '', regex=True)
df14['Partner GTIN'] = pd.to_numeric(df14['Partner GTIN'], errors='coerce', downcast='integer')


# new settlement13
df13= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202501.csv', skiprows=3)
df13['Partner GTIN']= df13['Partner GTIN'].replace(['"', '='], '', regex=True)
df13['Partner GTIN'] = pd.to_numeric(df13['Partner GTIN'], errors='coerce', downcast='integer')


# new settlement12
df12= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202412.csv', skiprows=3)
df12['Partner GTIN']= df12['Partner GTIN'].replace(['"', '='], '', regex=True)
df12['Partner GTIN'] = pd.to_numeric(df12['Partner GTIN'], errors='coerce', downcast='integer')


# new settlement11
df11= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202411.csv', skiprows=3)
df11['Partner GTIN']= df11['Partner GTIN'].replace(['"', '='], '', regex=True)
df11['Partner GTIN'] = pd.to_numeric(df11['Partner GTIN'], errors='coerce', downcast='integer')


# new settlement10
df10= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202410.csv', skiprows=3)
df10['Partner GTIN']= df10['Partner GTIN'].replace(['"', '='], '', regex=True)
df10['Partner GTIN'] = pd.to_numeric(df10['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement9
df9= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202409.csv', skiprows=3)
df9['Partner GTIN']= df9['Partner GTIN'].replace(['"', '='], '', regex=True)
df9['Partner GTIN'] = pd.to_numeric(df9['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement8
df8= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202408.csv', skiprows=3)
df8['Partner GTIN']= df8['Partner GTIN'].replace(['"', '='], '', regex=True)
df8['Partner GTIN'] = pd.to_numeric(df8['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement7
df7= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202407.csv', skiprows=3)
df7['Partner GTIN']= df7['Partner GTIN'].replace(['"', '='], '', regex=True)
df7['Partner GTIN'] = pd.to_numeric(df7['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement6
df6= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202406.csv', skiprows=3)
df6['Partner GTIN']= df6['Partner GTIN'].replace(['"', '='], '', regex=True)
df6['Partner GTIN'] = pd.to_numeric(df6['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement5
df5= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202405.csv', skiprows=3)
df5['Partner GTIN']= df5['Partner GTIN'].replace(['"', '='], '', regex=True)
df5['Partner GTIN'] = pd.to_numeric(df5['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement4
df4= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_202404.csv', skiprows=3)
df4['Partner GTIN']= df4['Partner GTIN'].replace(['"', '='], '', regex=True)
df4['Partner GTIN'] = pd.to_numeric(df4['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement3
df3= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0326.csv', skiprows=3)
df3['Partner GTIN']= df3['Partner GTIN'].replace(['"', '='], '', regex=True)
df3['Partner GTIN'] = pd.to_numeric(df3['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement2
df2= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0312.csv', skiprows=3)
df2['Partner GTIN']= df2['Partner GTIN'].replace(['"', '='], '', regex=True)
df2['Partner GTIN'] = pd.to_numeric(df2['Partner GTIN'], errors='coerce', downcast='integer')

# new settlement
df0= pd.read_csv('/content/drive/MyDrive/Reprice/settlement_0227.csv', skiprows=3)
df0['Partner GTIN']= df0['Partner GTIN'].replace(['"', '='], '', regex=True)
df0['Partner GTIN'] = pd.to_numeric(df0['Partner GTIN'], errors='coerce', downcast='integer')

# old settlement
df1= pd.read_csv('/content/drive/MyDrive/Reprice/settlement.csv', skiprows=3)
df1['Partner GTIN']= df1['Partner GTIN'].replace(['"', '='], '', regex=True)
df1['Partner GTIN'] = pd.to_numeric(df1['Partner GTIN'], errors='coerce', downcast='integer')

# concat to old settlement
df1= pd.concat([df0, df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15], ignore_index=True)

# del df0,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13
# gc.collect()

# fulfillment + prep fee
df_settle = df1[(df1['Transaction Type'] == 'FulfillmentFee') | (df1['Transaction Type'] == 'PrepServiceFee')]
# select vars
df_settle2= df_settle[['Partner GTIN','Transaction Type','Reason Code','Detail','Qty','Net Payable']]
# select vars
df_settle= df_settle[['Partner GTIN','Transaction Type','Reason Code','Qty','Net Payable']]
# rename
df_settle2= df_settle2.rename(columns={'Partner GTIN': 'GTIN'})
# rename
df_settle= df_settle.rename(columns={'Partner GTIN': 'GTIN'})

# wfs
df_wfs= df_settle2[df_settle2['Transaction Type']== 'FulfillmentFee']
# sum obs with same gtin
df_wfs['Net Sum'] = df_wfs.groupby(['GTIN'])['Net Payable'].transform('sum')
# see num of duplicates
df_wfs['Duplicates Count'] = df_wfs.groupby(['GTIN'])['Net Payable'].transform('count')
# take avg
# df_wfs['Avg. Net Payment']= round(df_wfs['Net Sum']/ df_wfs['Duplicates Count'], 2)
df_wfs['Avg. Net Payment']=  df_wfs['Net Payable']/ df_wfs['Qty']
# When 'Detail'= 'Under$10', WFS_Fee -$1
df_wfs.loc[df_wfs['Detail'] == 'Under$10', 'Avg. Net Payment'] = df_wfs.loc[df_wfs['Detail'] == 'Under$10', 'Avg. Net Payment'] - 1
# rename
df_wfs= df_wfs.rename(columns={'Avg. Net Payment': 'WFS Fee Settlement'})
df_wfs.drop(['Detail'], axis=1, inplace=True)
df_wfs.head(10)

# drop wfs duplicates
df_wfs.drop_duplicates(subset=['GTIN'], keep='first', inplace=True)

# prep
df_prep= df_settle[df_settle['Transaction Type']== 'PrepServiceFee']
# check if duplicates max is 2
df_prep['Duplicates Count'] = df_prep.groupby(['GTIN'])['Net Payable'].transform('count')
# prep by 1 unit
df_prep['Avg. Net Payment']= df_prep['Net Payable']/ df_prep['Qty']

# drop unwanted vars
df_prep.drop(['Qty', 'Net Payable', 'Duplicates Count'], axis=1, inplace=True)

# sum 2 types of prep fee
df_prep['Prep Fee'] = df_prep.groupby('GTIN')['Avg. Net Payment'].transform('sum')

# drop prep duplicates
df_prep.drop_duplicates(subset=['GTIN'], keep='first', inplace=True)

# select vars
df_wfs= df_wfs[['GTIN','WFS Fee Settlement']]
df_prep= df_prep[['GTIN','Prep Fee']]

# merge wfs df
df = df.merge(df_wfs, on='GTIN', how='left')

# replace wfs fee with net payable if have one, remain wfs if dont
for index, row in df.iterrows():
    if not pd.isna(row['WFS Fee Settlement']):
        df.at[index, 'WFS Fee'] = row['WFS Fee Settlement']

# merge prep
df = df.merge(df_prep, on='GTIN', how='left')

# fill nan with 0
df['Prep Fee'] = df['Prep Fee'].fillna(0)

# real shipping fee
df['Shipping Fee']= df['Avg Cost']*df['Shipping%']

# on hand or inbound
df['on hand or inbound'] = df.apply(lambda row: True if row['Available to sell'] > 0 or row['Inbound units'] > 0 else False, axis=1)

"""##Item Report"""

df1= pd.read_csv('/content/drive/MyDrive/Restock/ItemReport.csv')

# select vars
df_item= df1[['Reviews Count', 'Average Rating', 'Publish Status','Buy Box Item Price','SKU']]

df = df.merge(df_item, on='SKU', how='left')

# fill nan with 0
df['Reviews Count'] = df['Reviews Count'].fillna(0)
df['Average Rating'] = df['Average Rating'].fillna(0)

# del df_item
# gc.collect()

df

"""##Looker_Current"""

df_current= pd.read_csv('/content/drive/MyDrive/Reprice/Walmart_Reprice NEW_Current.csv')
# select vars

df_current= df_current[['identity_sku','is_discontinue', 'GTIN', 'current_price','min_order_qty','last_30_days_instock_days',	'last_30_days_sales',	'last_30_days_units_sales',	'last_7_days_instock_days',	'last_7_days_sales',	'last_7_days_units_sales']]

# remove the duplicates SKU
# df_current.sort_values(by=['identity_sku', 'Avg Cost'], ascending=[True, False], inplace=True)
df_current.drop_duplicates(subset='identity_sku', keep='first', inplace=True)

df = df.merge(df_current, left_on='SKU', right_on='identity_sku', how='left')

# fill nan with 0
df['last_30_days_instock_days'] = df['last_30_days_instock_days'].fillna(0)
df['last_30_days_sales'] = df['last_30_days_sales'].fillna(0)
df['last_30_days_units_sales'] = df['last_30_days_units_sales'].fillna(0)
df['last_7_days_instock_days'] = df['last_7_days_instock_days'].fillna(0)
df['last_7_days_sales'] = df['last_7_days_sales'].fillna(0)
df['last_7_days_units_sales'] = df['last_7_days_units_sales'].fillna(0)
df['is_discontinue'] = df['is_discontinue'].fillna('True')


df

"""## Order Report"""

df1= pd.read_excel('/content/drive/MyDrive/Reprice/PO_Data.xlsx')

# select vars
df_order= df1[['SKU','Order Date','Item Cost']]
# convert type
df_order['Order Date'] = pd.to_datetime(df_order['Order Date'])

# only retain most recent order, remove duplicates
df_order.sort_values(by=['SKU', 'Order Date'], ascending=[True, False], inplace=True)
df_order.drop_duplicates(subset='SKU', keep='first', inplace=True)

today = datetime.now()

# us time zone
us_time_zone = pytz.timezone('America/New_York')

# convert current date
today = today.replace(tzinfo=pytz.utc).astimezone(us_time_zone)

# no sale in 14
today = datetime.today()
df_order['Sold in last'] = (today - df_order['Order Date']).dt.days

# merge
df = df.merge(df_order, on='SKU', how='left')
df['Order Date']= pd.to_datetime(df['Order Date']).dt.to_period('D')
gc.collect()

# add cost
df['Cost']= round(df['Avg Cost']+ df['WFS Fee'], 2)
df['Cost'] = np.where(df['current_price'] < 10, df['Cost'] + 1, df['Cost'])

#add current price margin
df['Current Price Margin']= round(0.85 - df['Cost']/df['current_price'],2)

#add AMZ_lowest margin
df['AMZ lowest Margin']= round(0.85 - df['Cost']/df['lowest_amazon_price'],2)

#add BB Price Margin
df['Buy Box Item Price Margin']= round(0.85 - df['Cost']/df['Buy Box Item Price'],2)

#add Breakeven Price
df['Breakeven']= round(df['Cost']/0.85,2)

#add Final Map
df['is_map_enforced'] = df['is_map_enforced'].replace({True: 'TRUE', False: 'FALSE'})
df['Final Map'] = np.where(df['is_map_enforced'] == 'TRUE', df['map'], '0')

df.head(3)

# add Age - 250316
import pandas as pd
from dateutil.relativedelta import relativedelta

today = pd.to_datetime('today')
lllast_month = (today - relativedelta(months=3)).strftime('%b')
llast_month = (today - relativedelta(months=2)).strftime('%b')
last_month = (today - relativedelta(months=1)).strftime('%b')
this_month = (today - relativedelta(months=0)).strftime('%b')

Inbound = pd.read_csv('/content/drive/MyDrive/Reprice/inboundReceipts.csv')
Inbound = Inbound[['SKU','PO Delivered Date','Received Units']]
# Inbound['PO Delivered Date'] = pd.to_datetime(Inbound['PO Delivered Date'])
Inbound.loc[:, 'PO Delivered Date'] = pd.to_datetime(Inbound['PO Delivered Date'])
# Inbound = Inbound[(Inbound['PO Delivered Date'].notnull()) & (Inbound['Received Units'] > 0)] #exclude null date
Inbound = Inbound.loc[(Inbound['PO Delivered Date'].notnull()) & (Inbound['Received Units'] > 0)]
latest_po_inbound = Inbound.loc[Inbound.groupby('SKU')['PO Delivered Date'].idxmax()]
latest_po_inbound['PO Delivered Date'] = pd.to_datetime(latest_po_inbound['PO Delivered Date'], errors='coerce')
latest_po_inbound['Age'] = (pd.to_datetime('today') - latest_po_inbound['PO Delivered Date']).dt.days
df = pd.merge(df,latest_po_inbound[['SKU','Age']], on='SKU',how='left').replace([np.inf, -np.inf, np.nan], 0).round(2)
df.head(3)


df2= df[['Vendor','Item ID','SKU','ASIN','vendor_price', 'Avg Cost','WFS Fee','Cost','Reviews Count','Average Rating','Publish Status','is_discontinue','min_order_qty',
        'map', 'is_map_enforced','Final Map', 'Available to sell','Inbound units','Days of supply','on hand or inbound',
        'Order Date','Item Cost','Sold in last','Buy Box Item Price','lowest_amazon_price','AMZ lowest Margin','current_price','Current Price Margin',
        'last_30_days_instock_days',	'last_30_days_sales',	'last_30_days_units_sales',	'last_7_days_instock_days',	'last_7_days_sales',	'last_7_days_units_sales','Age']]

df= df[['Vendor','Item ID','SKU','ASIN','vendor_price', 'Avg Cost','WFS Fee','Cost','Reviews Count','Average Rating','Publish Status','is_discontinue','min_order_qty',
        'map', 'is_map_enforced','Final Map', 'Available to sell','Inbound units','Days of supply','on hand or inbound',
        'Order Date','Item Cost','Sold in last','Buy Box Item Price','lowest_amazon_price','AMZ lowest Margin','current_price','Current Price Margin','Age']]


# rename
columns_to_rename = {
    'vendor_price': 'Vendor Price',
    'Avg Cost': 'Avg. Cost',
    'Shipping%': 'Shipping Fee %',
    'lowest_amazon_price': 'Lowest Amazon Price',
    'map': 'MAP',
    'is_map_enforced': 'MAP Enforced',
    'Available to sell': 'Available to Sell',
    'min_order_qty': 'MOQ',
    'Item Cost': 'Last Sold Price',
}

for old_col, new_col in columns_to_rename.items():
    df = df.rename(columns={old_col: new_col})

for old_col, new_col in columns_to_rename.items():
    df2 = df2.rename(columns={old_col: new_col})

df.head(3)

# export to my drive
df2.to_excel('/content/drive/MyDrive/Results/Costs.xlsx', index=False)

# export sheet1.
# drop obs with no inven
df.to_excel('Re-price Table.xlsx', index=False, sheet_name='Main')
# df.to_excel('/content/drive/MyDrive/Reprice/Re-price Table.xlsx', engine='openpyxl', index=False)
df = df.loc[df['Available to Sell'] != 0]