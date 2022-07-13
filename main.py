import pandas as pd
import glob
import os
import pathlib
import warnings
warnings.simplefilter("ignore")
from fnmatch import fnmatch
import mimetypes
import re
import sys
import json

from datetime import datetime
from Google import Create_Service #from file Google.py
from googleapiclient.http import MediaFileUpload #for Google API Upload
from babel.numbers import format_currency #from format currency
from template_cleaning_blibli import to_drop_blibli
from template_cleaning_lazada import to_drop_lazada
from template_cleaning_shopee import to_drop_shopee
from template_cleaning_tokopodia import to_drop_tokopedia

#initialze Drive API
#you can get the API from https://console.cloud.google.com/
#SCOPES always like that for upload file
CLIENT_SECRET_FILE = 'client_secret_xyzacc.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

#folder ID from Google Drive
# parentFolder_ID =  '13lWjGJrjI5rMpmIH_mBDOyjrQpTcNGWo'
tokopediaFolder_ID = '1e2rnG02ODL6EM8up4qSr_Am3g6_sO9nX'
bukalapakFolder_ID = '1XtiWEDwnau8h68cKAiSFY61b924KINq5'
lazadaFolder_ID = '1Y7sfsC6O99RTGfBuwt7V91IygSr0ya70'
shopeeFolder_ID = '1tWxyFeEp958Ax7gx4P0eLBZshJyPEpB0'
blibliFolder_ID = '10a29pnirWuxm1Kl2ARS2_P-SwCi5RlWs'

folder = '/media/secret-document/DATA ONE/KREASI/Python Project Data Cleaning V01'

#initialize path equal to parent path above
root = folder
parent_path = '/media/secret-document/DATA ONE/KREASI/Python Project Data Cleaning V01/Hasil Data Celaning'

if not os.path.exists(parent_path):
    os.makedirs(parent_path)

for path, subdirs, files in os.walk(root):
    for name in files:
        file_extension = pathlib.Path(name).suffix
        if fnmatch(file_extension, '.xlsx'):
            filename = os.path.join(path, name)
            lastName = os.path.basename(name)
            split_lastName = re.split('[\b\W\b]+', lastName)
            final = split_lastName[1]
            
            lastPath = os.path.basename(os.path.normpath(path))
            if lastPath == 'Lazada' or lastPath == 'lazada':
                df_lazada  = pd.read_excel(filename)
                #display(df_lazada)
                df_lazada.drop(to_drop_lazada, inplace=True, axis=1)
                df_order_lazada =  df_lazada[['createTime', 'orderNumber',  'sellerSku',  'paidPrice', 'status']]
                df_order_lazada['orderNumber'] = df_order_lazada['orderNumber'].astype(str)
                df_order_lazada["paidPrice"] = df_order_lazada["paidPrice"].apply(lambda x: format_currency(x, currency="Rp. ", locale="id_ID", group_separator=True))
                
                df_order_lazada['createTime'] = pd.to_datetime(df_order_lazada['createTime'], format='%d %b %Y %H:%M')
                df_order_lazada['createTime'] =  df_order_lazada['createTime'].dt.strftime('%m/%d/%Y')
                df_order_lazada[["Date", "Month", "Year"]] = df_order_lazada["createTime"].str.split("/", expand = True)
                
                pathlib.Path(f'{parent_path}/{lastPath}_bc2').mkdir(parents=True, exist_ok=True) 
                df_order_lazada.to_excel(f'{parent_path}/{lastPath}_bc2/{lastPath}_b2c_'+str(final)+'.xlsx', index = None, header=True)
            #Tokopedia
            elif lastPath == 'Tokopedia' or lastPath == 'tokopedia':
                df_tokopedia  = pd.read_excel(filename, header=4)
                #display(df_tokopedia)
                df_tokopedia.drop(to_drop_tokopedia, inplace=True, axis=1)
                df_tokopedia["Harga Jual (IDR)"] = df_tokopedia["Harga Jual (IDR)"].apply(lambda x: format_currency(x, currency="Rp. ", locale="id_ID", group_separator=True))
                df_tokopedia["Gudang Pengiriman"] = ''
                df_tokopedia['Tanggal Pembayaran'] = pd.to_datetime(df_tokopedia['Tanggal Pembayaran'], format='%d-%m-%Y %H:%M:%S')
                df_tokopedia['Tanggal Pembayaran'] =  df_tokopedia['Tanggal Pembayaran'].dt.strftime('%m/%d/%Y %H:%M')

                pathlib.Path(f'{parent_path}/{lastPath}_bc2').mkdir(parents=True, exist_ok=True) 
                df_tokopedia.to_excel(f'{parent_path}/{lastPath}_bc2/{lastPath}_b2c_'+str(final)+'.xlsx', sheet_name='Laporan Penjualan', index = None, header=True)
            #Shopee
            elif lastPath == 'Shopee' or lastPath == 'shopee':
                f_filename  = split_lastName [2]
                f_shopee = f_filename.split('_')
                ff = f_shopee[1]
                
                df_shopee  = pd.read_excel(filename)
                #display(df_shopee)
                df_shopee.drop(to_drop_shopee, inplace=True, axis=1)
                df_order_shopee =  df_shopee[['Waktu Pesanan Dibuat', 'No. Pesanan',  'SKU Induk', 'Total Harga Produk', 'Jumlah Produk di Pesan', 'Status Pesanan']]
                
                df_order_shopee['Waktu Pesanan Dibuat'] = pd.to_datetime(df_order_shopee['Waktu Pesanan Dibuat'], format='%Y-%m-%d %H:%M')
                df_order_shopee['Waktu Pesanan Dibuat'] =  df_order_shopee['Waktu Pesanan Dibuat'].dt.strftime('%m/%d/%Y')
                df_order_shopee[["Date", "Month", "Year"]] = df_order_shopee["Waktu Pesanan Dibuat"].str.split("/", expand = True)
                
                pathlib.Path(f'{parent_path}/{lastPath}_bc2').mkdir(parents=True, exist_ok=True) 
                df_order_shopee.to_excel(f'{parent_path}/{lastPath}_bc2/{lastPath}_b2c_'+str(ff)+'.xlsx', sheet_name='orders', index = None, header=True)

        if fnmatch(file_extension, '.csv'):
            filename = os.path.join(path, name)
            lastName = os.path.basename(name)
            split_lastName = re.split('[\b\W\b]+', lastName)
            final = split_lastName[1]
            
            lastPath = os.path.basename(os.path.normpath(path))
            if lastPath == 'blibli' or lastPath == 'Blibli':
                df_blibli  = pd.read_csv(filename)
                df_blibli.drop(to_drop_blibli, inplace=True, axis=1)
                df_order_blibli =  df_blibli[['No. Order', 'Tanggal Order',  'Merchant SKU',  'Total Barang', 'Order Status', 'Harga Produk']]
                df_order_blibli["Harga Produk"] = df_order_blibli["Harga Produk"].apply(lambda x: format_currency(x, currency="Rp. ", locale="id_ID", group_separator=True))
                
                df_order_blibli['Tanggal Order'] = pd.to_datetime(df_order_blibli['Tanggal Order'], format='%m/%d/%Y %H:%M')
                df_order_blibli['Tanggal Order'] =  df_order_blibli['Tanggal Order'].dt.strftime('%m/%d/%Y')
                df_order_blibli[["Date", "Month", "Year"]] = df_order_blibli["Tanggal Order"].str.split("/", expand = True)
                
                pathlib.Path(f'{parent_path}/{lastPath}_bc2').mkdir(parents=True, exist_ok=True) 
                df_order_blibli.to_excel(f'{parent_path}/{lastPath}_bc2/{lastPath}_b2c_'+str(final)+'.xlsx', index = None, header=True)

def getList(folder_id, service):

    query = f"parents = '{folder_id}'"

    data = service.files().list(q=query).execute()
    
    response = data.get('files')

    lists = []
    # print(data.get('files'))
    for res in response:
        lists.append(res['name'])
        
    return lists

def Upload(folder_id, nameFile, lists, service, filelist, mime_type):
    # print('name', name)
    # print('list', lists)
    if nameFile not in lists:
        file_metadata = {
            'name': nameFile,
            'parents': [folder_id]
        }
        media = MediaFileUpload(str(filelist)+'/{0}'.format(nameFile), mimetype=mime_type)

        service.files().create(
            body = file_metadata,
            media_body = media,
            fields = 'id'
        ).execute()

try:
    if fnmatch(sys.argv[1], 'withUpload'):
        service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

        List_tokopedia = getList(tokopediaFolder_ID, service)
        List_shopee = getList(shopeeFolder_ID, service)
        List_lazada = getList(lazadaFolder_ID, service)
        List_blibli = getList(blibliFolder_ID, service)
        #List_tokopedia = getList(tokopediaFolder_ID, service)

        for path, subdirs, files in os.walk(parent_path):
            #loop through the subfolder and find all file in all subfolder of parent_path
            for nameFile in files:
                mime_type, encoding = mimetypes.guess_type(name)
                filelist = os.path.join(path)
                split_filename = nameFile.split('_')
                final_fn = split_filename[0]
        
                #condition to make the file upload based on name of file
                #and this will upload file to folder google drive that equal to first of the filename
                if final_fn == 'Tokopedia' or final_fn == 'tokopedia':
                    Upload(tokopediaFolder_ID, nameFile, List_tokopedia, service, filelist, mime_type)
                # break
                elif final_fn == 'Blibli' or final_fn == 'blibli':
                    Upload(blibliFolder_ID, nameFile, List_blibli, service, filelist, mime_type)
                #break
                elif final_fn == 'Lazada' or final_fn == 'lazada':
                    Upload(lazadaFolder_ID, nameFile, List_lazada, service, filelist, mime_type)
                elif final_fn == 'Shopee' or final_fn == 'shopee':
                    Upload(shopeeFolder_ID, nameFile, List_shopee, service, filelist, mime_type)
except IndexError:
   print('Cleaning Done File Save in Local!')

#def Upload(folder_id, name):
    

    