#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import re
import pymssql
from datetime import date,timedelta
import numpy as np
import prestodb
import datetime


# In[5]:


# DOA=pd.read_excel("DOA Processed from 2019.xlsx",sheet_name="Data")#sql 


# In[145]:


conn=pymssql.connect(host='10.122.3.175:1433',
user='bcdmuser',
password='(gN?f6(uS36W',
database='LEN-IN-DOA'
)


# In[146]:


def sql_data_f(a):
    cur=conn.cursor()
    cur.execute(a)
    data=cur.fetchall()
    headers=cur.description
    colm_names=[colm[0] for colm in headers]
    data=pd.DataFrame(data,columns=colm_names)
    return data


# In[147]:


#automation
date_end=date.today()-timedelta(days=date.today().day+600)
print(date_end)

r="select * from dbo.DOA_AUTOMATION where CreatedOn  <'"+str(date_end)+"' "
r


# In[148]:


DOA_AUTO=sql_data_f("select * from dbo.DOA_AUTOMATION where CreatedOn  <'%s'"%(str(date_end)))
# DOA_AUTO=sql_data_f("select * from dbo.DOA_AUTOMATION where CreatedOn between '2022-01-01' and '2022-03-01'" )

DOA_AUTO.shape


# In[13]:


len(pd.unique(DOA_AUTO["DOACreateId"]))


# In[14]:


DOA_AUTO.columns.values


# In[15]:


DOA_AUTO_1=DOA_AUTO[["DOACreateId","CreatedOn","SerialNo","MTMNO","UATRefNo","Organisation","State","CITY","StatusName"]]
DOA_AUTO_1.shape


# In[16]:


DOA_AUTO_1["StatusName"]=DOA_AUTO_1["StatusName"].str.upper()
pd.unique(DOA_AUTO_1["StatusName"])


# In[17]:


DOA_AUTO_1=DOA_AUTO_1[DOA_AUTO_1["StatusName"].isin(["CLOSED - REPLACEMENT","NEGOTIATION","CLOSED - REFUND","ASSIGNED TO LOGISTICS VENDOR","BILLING","CLOSED - REPLACEMENT THROUGH BP","CUSTOMER CONFIRMATION AWAITED","DOA CERTIFICATE ISSUED","FSO CREATION","PENDING FROM ORDERDESK TEAM","RSO CREATION"])]
DOA_AUTO_1.shape


# In[18]:


# DOA_AUTO_1.to_excel("DOA_AUTO_1.xlsx")


# In[19]:


# cre_id=pd.unique(DOA_AUTO_1["DOACreateId"])
# CRE_LIS=cre_id.tolist()
# string=','.join([str(item) for item in CRE_LIS])
# data_4="("+string+")"
# c="select * from dbo.DOACreate where DOACreateId in"+data_4


# In[20]:


DOA_AUTO_2=DOA_AUTO_1.drop_duplicates("DOACreateId")
DOA_AUTO_2.shape


# In[24]:


# DOA_create=sql_data_f(c)
# DOA_create


# In[ ]:


b= 0
c= 50000
d=(len(DOA_AUTO_2))
DOA_create=pd.DataFrame()
for i in range(int(d/50000)+1):
    data_2=DOA_AUTO_2["DOACreateId"][b:c].tolist()
    data_3="','".join([str(item) for item in data_2])
    data_4="('"+data_3+"')"
    def data_processing():
        c="select * from dbo.DOACreate where DOACreateId in"+data_4
        data_df_d=sql_data_f("select * from dbo.DOACreate where DOACreateId in%s"%data_4)
        return data_df_d
    data_df_d= data_processing()
    DOA_create=DOA_create.append(data_df_d)
    b=c
    c=c+50000


# In[23]:


DOA_AUTO_1.shape


# In[25]:


DOA_create.shape


# In[26]:


DOA_create_1=DOA_create[["DOACreateId","CreatedOn","TicketId"]]  #DOA Create data


# In[27]:


# d="select * from dbo.DOADetails where DOACreateId in"+data_4
# DOA_details=sql_data_f(d)


# In[28]:


b= 0
c= 50000
d=(len(DOA_AUTO_2))
DOA_details=pd.DataFrame()
for i in range(int(d/50000)+1):
    data_2=DOA_AUTO_2["DOACreateId"][b:c].tolist()
    data_3="','".join([str(item) for item in data_2])
    data_4="('"+data_3+"')"
    def data_processing():
        d="select * from dbo.DOADetails where DOACreateId in"+data_4
        data_df_d=sql_data_f("select * from dbo.DOACreate where DOACreateId in%s"%data_4)
        return data_df_d
    data_df_d= data_processing()
    DOA_details=DOA_details.append(data_df_d)
    b=c
    c=c+50000


# In[29]:


DOA_details_1=DOA_details[["DOACreateId","RefundAmount","DOACertificateCreatedOn"]]


# In[30]:


# prod="select * from dbo.DOAProductDetail where DOACreateId in"+data_4
# DOA_Prod_DETAIL=sql_data_f(prod)
# # DOA_Prod_DETAIL


# In[31]:


b= 0
c= 30000
d=(len(DOA_AUTO_2))
DOA_Prod_DETAIL=pd.DataFrame()
for i in range(int(d/30000)+1):
    data_2=DOA_AUTO_2["DOACreateId"][b:c].tolist()
    data_3="','".join([str(item) for item in data_2])
    data_4="('"+data_3+"')"
    def data_processing():
        prod="select * from dbo.DOAProductDetail where DOACreateId in"+data_4
        data_df_d=sql_data_f("select * from dbo.DOAProductDetail where DOACreateId in %s"%data_4)
        return data_df_d
    data_df_d= data_processing()
    DOA_Prod_DETAIL=DOA_Prod_DETAIL.append(data_df_d)
    b=c
    c=c+30000


# In[32]:


len(pd.unique(DOA_Prod_DETAIL["DOACreateId"]))
pd.unique(DOA_Prod_DETAIL["ProductCategoryId"])


# In[33]:


DOA_Prod_DETAIL_1=DOA_Prod_DETAIL[["DOACreateId","SerialNo","MTMNO","Model","ProductCategoryId"]]
DOA_Prod_DETAIL_2=DOA_Prod_DETAIL_1.drop_duplicates("ProductCategoryId")
DOA_Prod_DETAIL_2=DOA_Prod_DETAIL_2[DOA_Prod_DETAIL_2["ProductCategoryId"]>=0]


# In[34]:


pcid=pd.unique(DOA_Prod_DETAIL_2["ProductCategoryId"])
pcid_LIS=pcid.tolist()
string1=','.join([str(item) for item in pcid_LIS])
data_3="("+string1+")"
pdc="select * from dbo.DOAProductCategory where DOAProductCategoryId in"+data_3


# In[35]:


pdc


# In[36]:


# b= 0
# c= 50000
# d=(len(DOA_Prod_DETAIL_2))
# DOA_Prod_Category=pd.DataFrame()
# for i in range(int(d/50000)+1):
#     data_2=DOA_Prod_DETAIL_2["ProductCategoryId"][b:c].tolist()
#     data_3="','".join([str(item) for item in data_2])
#     data_4="('"+data_3+"')"
#     def data_processing():
#         pdc="select * from dbo.DOAProductCategory where DOAProductCategoryId in"+data_4
#         data_df_d=sql_data_f(pdc)
#         return data_df_d
#     data_df_d= data_processing()
#     DOA_Prod_Category=DOA_Prod_Category.append(data_df_d)
#     b=c
#     c=c+50000


# In[37]:


DOA_Prod_Category=sql_data_f("select * from dbo.DOAProductCategory where DOAProductCategoryId in %s"%data_3)
DOA_Prod_Category.shape


# In[38]:


DOA_Prod_Category_1=DOA_Prod_Category[["DOAProductCategoryId","DOAProductCategoryName"]]


# In[39]:


DOA_Prod_DETAIL_2=DOA_Prod_DETAIL_1.merge(DOA_Prod_Category_1,left_on="ProductCategoryId",right_on="DOAProductCategoryId",how="left")


# In[40]:


DOA_Prod_DETAIL_3=DOA_Prod_DETAIL_2[["DOACreateId","SerialNo","MTMNO","Model","DOAProductCategoryName"]]


# In[41]:


DOA_Prod_DETAIL_3


# In[42]:


# ctcss="select * from dbo.DOACreateToCustomerSubSegment where DOACreateId in"+data_4
# DOA_CreateToCustomerSubSegment=sql_data_f(ctcss)
# # DOA_CreateToCustomerSubSegment


# In[43]:


b= 0
c= 30000
d=(len(DOA_AUTO_2))
DOA_CreateToCustomerSubSegment=pd.DataFrame()
for i in range(int(d/30000)+1):
    data_2=DOA_AUTO_2["DOACreateId"][b:c].tolist()
    data_3="','".join([str(item) for item in data_2])
    data_4="('"+data_3+"')"
    def data_processing():
        ctcss="select * from dbo.DOACreateToCustomerSubSegment where DOACreateId in"+data_4
        data_df_d=sql_data_f("select * from dbo.DOACreateToCustomerSubSegment where DOACreateId in %s"%data_4)
        return data_df_d
    data_df_d= data_processing()
    DOA_CreateToCustomerSubSegment=DOA_CreateToCustomerSubSegment.append(data_df_d)
    b=c
    c=c+30000


# In[44]:


DOA_CreateToCustomerSubSegment["CustomerSubSegmentId"]
len(pd.unique(DOA_CreateToCustomerSubSegment["CustomerSubSegmentId"]))


# In[45]:


DOA_CreateToCustomerSubSegment_1=DOA_CreateToCustomerSubSegment.drop_duplicates("CustomerSubSegmentId")
DOA_CreateToCustomerSubSegment_1=DOA_CreateToCustomerSubSegment_1[DOA_CreateToCustomerSubSegment_1["CustomerSubSegmentId"]>=0]


# In[46]:


DOA_CreateToCustomerSubSegment_1


# In[47]:


cuss=pd.unique(DOA_CreateToCustomerSubSegment_1["CustomerSubSegmentId"])
CUSS=cuss.tolist()
string2=','.join([str(item) for item in CUSS])
data_2="("+string2+")"
cu="select * from dbo.DOACustomerSubSegment where CustomerSubSegmentId in"+data_2


# In[48]:


DOA_DOACustomerSubSegment=sql_data_f("select * from dbo.DOACustomerSubSegment where CustomerSubSegmentId in %s"%data_2)


# In[49]:


# b= 0
# c= 20000
# d=(len(DOA_CreateToCustomerSubSegment_1))
# DOA_DOACustomerSubSegment=pd.DataFrame()
# for i in range(int(d/20000)+1):
#     data_2=DOA_CreateToCustomerSubSegment_1["CustomerSubSegmentId"][b:c].tolist()
#     data_3="','".join([str(item) for item in data_2])
#     data_4="('"+data_3+"')"
#     def data_processing():
#         cu="select * from dbo.DOACustomerSubSegment where CustomerSubSegmentId in"+data_4
#         data_df_d=sql_data_f(cu)
#         return data_df_d
#     data_df_d= data_processing()
#     DOA_DOACustomerSubSegment=DOA_DOACustomerSubSegment.append(data_df_d)
#     b=c
#     c=c+20000


# In[50]:


DOA_DOACustomerSubSegment_1=DOA_DOACustomerSubSegment[["CustomerSubSegmentId","CustomerSubSegmentName"]]


# In[51]:


CustomerSubSegment=DOA_CreateToCustomerSubSegment.merge(DOA_DOACustomerSubSegment_1,on="CustomerSubSegmentId",how="left")


# In[52]:


CustomerSubSegment=CustomerSubSegment[["DOACreateId","CustomerSubSegmentName"]]


# In[53]:


DOA_AUTO_2=DOA_AUTO_1.merge(CustomerSubSegment,on="DOACreateId",how="left")


# In[54]:


DOA_AUTO_3=DOA_AUTO_2.merge(DOA_create_1,left_on=["DOACreateId","CreatedOn"],right_on=["DOACreateId","CreatedOn"],how="left")


# In[55]:


DOA_AUTO_4=DOA_AUTO_3.merge(DOA_details_1,on="DOACreateId",how="left")


# In[56]:


DOA_AUTO_5=DOA_AUTO_4.merge(DOA_Prod_DETAIL_3,left_on=["DOACreateId","SerialNo","MTMNO"],right_on=["DOACreateId","SerialNo","MTMNO"],how="left")


# In[57]:


DOA_Final=DOA_AUTO_5[["DOACreateId","TicketId","CreatedOn","State","CITY","MTMNO","SerialNo","Model","DOAProductCategoryName","DOACertificateCreatedOn","RefundAmount","Organisation","UATRefNo","CustomerSubSegmentName","StatusName"]]


# In[58]:


DOA_Final['year'] =pd.DatetimeIndex(DOA_Final['CreatedOn']).year
DOA_Final["mon"]=pd.DatetimeIndex(DOA_Final['CreatedOn']).month
DOA_Final['month'] =pd.DatetimeIndex(DOA_Final['CreatedOn']).month.astype(str)+"-"+pd.DatetimeIndex(DOA_Final['CreatedOn']).year.astype(str)


# In[59]:


for i in range(0,len(DOA_Final['month'])):
    if (DOA_Final["mon"][i])<4:
        DOA_Final['year'][i] ="FY'"+str((DOA_Final['year'][i]-1))+"-"+str((DOA_Final['year'][i]))
    else:
        DOA_Final['year'][i] ="FY'"+str((DOA_Final['year'][i]))+"-"+str((DOA_Final['year'][i] +1))


# In[60]:


DOA_Final=DOA_Final[["month","year","DOACreateId","TicketId","CreatedOn","State","CITY","MTMNO","SerialNo","Model","DOAProductCategoryName","DOACertificateCreatedOn","RefundAmount","Organisation","UATRefNo","CustomerSubSegmentName","StatusName"]]


# In[61]:


DOA_Final.rename(columns={"year":"Year","month":"Month","DOACreateId":"DOA CreatedID","TicketId":"Ticket Id","CreatedOn":"Created On",
                          "CITY":"City","MTMNO":"MTM NO","SerialNo":"Serial No","DOAProductCategoryName":"Product Category",
                          "DOACertificateCreatedOn":"Certificate Issued Date","RefundAmount":"Refund Amount","UATRefNo":"UAT Ref No",
                          "CustomerSubSegmentName":"Customer Sub Segment"},inplace=True)


# In[62]:


# DOA_Final.to_excel("DOA_report.xlsx")


# In[63]:


###Final Input After DOA report preparation####
DOA=DOA_Final


# In[64]:


DOA_Final.to_excel("DOA_report.xlsx")


# In[65]:


DOA["MTM NO"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA["MTM NO"]]
# DOA["Serial No"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA["Serial No"]]
DOA=DOA.apply(lambda x: x.astype(str).str.upper())


# In[79]:



def read_data(table,k,h,y):
    conn = prestodb.dbapi.connect(
    host='presto.dbc.ludp.lenovo.com',
    port= 30060,
    user= 'p413_g2861',
    catalog = 'hive',
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication("p413_g2861","gVer-5217"),)
    conn._http_session.verify= "presto.cer"
    cur = conn.cursor()
#     query=k+table+h+y#"select * from "+table+" where land1 = 'IN'"
#     print(("select * from %s%s%s"%(table,h,y)))
    cur.execute("select * from %s%s%s"%(table,h,y))
    rows = cur.fetchall()
    cols=cur.description
    return rows,cols


# In[80]:


#fetching data from VBRP table using serial number

data_df_st_1=DOA[DOA["Serial No"]!=''].drop_duplicates(['Serial No']).reset_index(drop=True)
k="select * from "
h=" where sernr IN "
b= 0
c= 50000
d=(len(data_df_st_1))
data_1=pd.DataFrame()
for i in range(int(d/50000)+1):
    data_df_st_2=data_df_st_1["Serial No"][b:c].tolist()
    data_df_st_4="','".join(str(x) for x in data_df_st_2)
    data_df_st_5="('"+data_df_st_4+"')"
    def data_processing_2():
        rows,cols=read_data("ecc_db_bc.vbrp_srl_full_lan_e",k,h,data_df_st_5)
        column = [col[0] for col in cols]
        data_2=pd.DataFrame(rows,columns=column)
        return data_2
    data_2= data_processing_2()
    data_1=data_1.append(data_2)
    #data_df_vbrp_sr.append(data_t_ver)
    b=c
    c=c+50000


# In[81]:





# In[82]:


data_2_vbrp=data_1.copy()


# In[83]:


data_1=data_2_vbrp.copy()


# In[84]:


data_1=data_1.drop_duplicates(['vbeln','posnr','sernr','matnr','fkdat','erdat','erzet']).reset_index(drop=True)


# In[85]:


data_1["fkdat"]=pd.to_datetime(data_1["fkdat"],format= '%Y%m%d')
#filtering YBRE (Return) data
vbrp_Return=data_1[data_1["fkart"]=="YBRE"]


# In[86]:


# new column "Exeption Reason" = "No SN Found in VBRP"  & "No Return Date After DOA"
DOA["Certificate Issued Date"]=pd.to_datetime(DOA["Certificate Issued Date"],format= '%Y-%m-%d')
DOA["Certificate Issued Date"]=DOA["Certificate Issued Date"].apply(lambda x : x.date())
vbrp_ser=list(set(data_1["sernr"].to_list()))
vbrp_ser_no_ybre=list(set(vbrp_Return["sernr"].to_list()))
DOA["Exception Reason"]=DOA["Serial No"].apply(lambda x :"No SN Found in VBRP" if x not in vbrp_ser else ("No Return Date After DOA" if x not in vbrp_ser_no_ybre else ""))


# In[87]:


DOA["Certificate Issued Date"]=pd.to_datetime(DOA["Certificate Issued Date"])


# In[88]:


#Taking the Min_date of "Certificate Issued Date"
Doa=DOA[DOA["Serial No"]!="NAN"]
duplicate_DOA=Doa.groupby(["Serial No"])["Year"].count().reset_index()
duplicate_DOA_same_day=Doa.groupby(["Serial No"])["Certificate Issued Date","MTM NO"].nunique().reset_index()
DOA_min_return=Doa.groupby(["Serial No"]).agg(Min_date=pd.NamedAgg(column="Certificate Issued Date",aggfunc=min)).reset_index()


# In[89]:


print(duplicate_DOA.shape,Doa.shape)


# In[90]:


# duplicate_DOA[duplicate_DOA["Year"]>1]
# duplicate_DOA_same_day[duplicate_DOA_same_day["MTM NO"]>1]
# DOA_min_return[DOA_min_return["Serial No"].isin(["MP1WBPF0","MP1WSX41","MP1WTWPL","PF3946N7","V303MCAC","V303MCAD"])]
# Doa[Doa["Serial No"].isin(["MP1WBPF0","MP1WSX41","MP1WTWPL","PF3946N7","V303MCAC","V303MCAD"])]
# Doa[Doa["Serial No"]=="HA1B5ATX"]


# In[91]:


duplicate_DOA=duplicate_DOA.merge(duplicate_DOA_same_day,on="Serial No",how="left")


# In[92]:


# duplicate_DOA


# In[93]:


duplicate_DOA=duplicate_DOA.merge(DOA_min_return,on="Serial No",how="left")


# In[94]:


pa=duplicate_DOA[duplicate_DOA["Year"]>1 ]#not Part of analysis & (duplicate_DOA["Created On"]==1) & (duplicate_DOA["MTM NO"]==1)
na=pa["Serial No"].to_list()
# pa


# In[95]:


duplicate_DOA=duplicate_DOA[duplicate_DOA["Year"]>1]
dupli_list_ser=duplicate_DOA["Serial No"].to_list()


# In[96]:


DOA=DOA.merge(duplicate_DOA[["Serial No","Min_date"]],left_on =["Serial No"],right_on=["Serial No"],how="left",indicator="Dupliate")


# In[97]:


duplicate_DOA.shape


# In[98]:


# DOA[DOA["Days diff(Min-max cre)"]>0]
# DOA[DOA["Serial No"].isin(["MP1QTZPG","PF3FLHBN","R90YLVHS","PF2Y54H0"])]


# In[99]:


#finding diff between "Certificate Issued Date" and "Min_date"
DOA["Days diff(Min-max cre)"]=(DOA["Certificate Issued Date"]-DOA["Min_date"]).dt.days
DOA["Days diff(Min-max cre)"]=DOA["Days diff(Min-max cre)"].fillna(value=-22222.0)


# In[100]:


# New "Exception Reason" = "Duplicate DOA Processed"
for i in range(len(DOA)):
    if(DOA["Days diff(Min-max cre)"][i]>0):
        kal=DOA["Exception Reason"][i]
        DOA["Exception Reason"][i]="Duplicate DOA Processed"
    
    elif(int(DOA["Days diff(Min-max cre)"][i])==0):
        p_a=str(DOA["Serial No"][i])
        n_a=DOA[DOA["Serial No"]==p_a].reset_index()
        n_a=n_a.sort_values(by=["Ticket Id"])
        n_a=n_a[["Serial No","Ticket Id"]]
        na=n_a.groupby(["Serial No"]).first()
        na=na.reset_index()
        ap=str(na["Ticket Id"][0])
        app_1=DOA.index[(DOA["Ticket Id"]!=ap) & (DOA["Serial No"]==p_a)].tolist()
        for j in app_1:
            DOA["Exception Reason"][j]="Duplicate DOA Processed"
        app=DOA.index[(DOA["Ticket Id"]==ap) & (DOA["Serial No"]==p_a)].tolist()
        appp=app[0]
        DOA["Exception Reason"][appp]=""


# In[101]:


DOA_single=pd.DataFrame()
DOA_Duplicate=pd.DataFrame()


# In[102]:


#separating single and duplicates
df_1=DOA[DOA["Dupliate"]=="both"]
df_2=DOA[DOA["Dupliate"]!="both"]
DOA_Duplicate=DOA_Duplicate.append(df_1)
DOA_single=DOA_single.append(df_2)


# In[103]:


#the OG
# df_1=DOA[DOA["Dupliate"]=="both"]
# df_2=DOA[DOA["Dupliate"]!="both"]# need to work on this
# DOA_Duplicate=DOA_Duplicate.append(df_1)
# DOA_single=DOA_single.append(df_2)


# In[104]:


abc=DOA_Duplicate[DOA_Duplicate["Exception Reason"]==""]


# In[105]:


abc["Exception Reason"]=abc["Exception Reason"].apply(lambda x :"Duplicate DOA Processed" if str(x) =="" else x)


# In[106]:


DOA_single=DOA_single.append(abc)


# In[107]:


vbrp_r=vbrp_Return.merge(DOA_single[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["sernr"])
vbrp_r["Certificate Issued Date"]=pd.to_datetime(vbrp_r["Certificate Issued Date"],format= '%Y-%m-%d')
vbrp_r["multi_returns"]=""

for i in range(len(vbrp_r)):
    a=vbrp_r["Certificate Issued Date"][i]
    b=vbrp_r["fkdat"][i]
    if(a<=b):
        vbrp_r["multi_returns"][i]="Check"


# In[108]:


vbrp_check=vbrp_r[vbrp_r["multi_returns"]=="Check"]


# In[109]:


# vbrp_r.to_excel("vbrpr.xlsx")


# In[110]:


# "Exception Reason" = "SN Not Present in DOA" & "More than 1 Return after DOA"
##Creation of Min_date return##
vbrp_Return_groupby=vbrp_check.groupby(["sernr"]).agg(Min_date_return=pd.NamedAgg(column="fkdat",aggfunc=min)).reset_index()
Vbrp_return_group= vbrp_check.groupby(["sernr"])["fkart"].count()
Vbrp_return_group=Vbrp_return_group.to_frame()
Vbrp_return_group=Vbrp_return_group.reset_index()

vbrp_Return_groupby_1=vbrp_check.groupby(["sernr","matnr"]).agg(Min_date_return=pd.NamedAgg(column="fkdat",aggfunc=min)).reset_index()
#kal=DOA[DOA["Ticket Id"]=="D190927058"]
#kal
DOA_single=DOA_single.merge(Vbrp_return_group,how="left",left_on="Serial No",right_on="sernr")
for k in range(len(DOA_single)):
    if(str(DOA_single["Serial No"][k])=="NAN"):
        DOA_single["Exception Reason"][k]="SN Not Present in DOA"
    if(DOA_single["fkart"][k]>1):
        DOA_single["Exception Reason"][k]="More than 1 Return after DOA"
        
        
# New column MTM Mismatch created with Value "MTM SAME AS DOA"  & "MTM DIFF from DOA"   
DOA_single=DOA_single.merge(vbrp_Return_groupby_1[["sernr","matnr"]],how="left",right_on=["sernr","matnr"],left_on=["Serial No","MTM NO"],indicator="MTM Mismatch")
DOA_single=DOA_single.merge(vbrp_Return_groupby[["sernr","Min_date_return"]],how="left",right_on=["sernr"],left_on=["Serial No"],indicator="First return")


DOA_single["MTM Mismatch"]=DOA_single["MTM Mismatch"].map({'both': 'MTM SAME AS DOA', 'left_only': 'MTM DIFF from DOA'})



DOA_single["Certificate Issued Date"]=pd.to_datetime(DOA_single["Certificate Issued Date"],format= '%Y-%m-%d')
#DOA_single["Created On"]=DOA_single["Created On"].apply(lambda x : x.date() )

# New Bucketing & Exception Reason: "NO RETURN DATE AFTER DOA"
DOA_single["No of Days"]=(DOA_single["Min_date_return"]-DOA_single["Certificate Issued Date"]).dt.days
DOA_single["No of Days"]=DOA_single["No of Days"].fillna(value=-22222.0)


DOA_single["Bucket for No of Day"]=""
for j in range(len(DOA_single)):
    if(int(DOA_single["No of Days"][j])< 0):
        DOA_single["Bucket for No of Day"][j]="NO Returns"
    elif(int(DOA_single["No of Days"][j])<= 7):
        DOA_single["Bucket for No of Day"][j]="0-7 Days"
    elif(int(DOA_single["No of Days"][j])<= 30):
        DOA_single["Bucket for No of Day"][j]="8-30 Days"
    elif(int(DOA_single["No of Days"][j])<= 60):
        DOA_single["Bucket for No of Day"][j]="31-60 Days"
    elif(int(DOA_single["No of Days"][j])<= 180):
        DOA_single["Bucket for No of Day"][j]="61-180 Days"
    elif(int(DOA_single["No of Days"][j])<= 365):
        DOA_single["Bucket for No of Day"][j]="181-365 Days"
    elif(int(DOA_single["No of Days"][j])<= 730):
        DOA_single["Bucket for No of Day"][j]="1-2 Years"
    elif(int(DOA_single["No of Days"][j])<= 1095):
        DOA_single["Bucket for No of Day"][j]="2-3 Years"
    else:
        DOA_single["Bucket for No of Day"][j]="Greater Than 3 Years"
        
        
DOA_single["Exception Reason"]=DOA_single.apply(lambda x: "No Return Date After DOA" if (x["Bucket for No of Day"]=="NO Returns")&(x["Exception Reason"]=="")else x["Exception Reason"],axis=1)


# In[111]:


## Exception Reason= "NO RETURN BUT SOLD AFTER DOA"

data_1=data_1.sort_values(by="fkdat")


# In[112]:


dat_1=data_1.merge(vbrp_Return_groupby,on=["sernr"],how="left",indicator="Returned & Not sold")
dat_1=dat_1[dat_1["Returned & Not sold"]=="both"]


# In[ ]:





# In[113]:


# Comparing Min_date & FKDAT
dat_1["No of Days"]=(dat_1["fkdat"]-dat_1["Min_date_return"]).dt.days
dat_1["No of Days"]=dat_1["No of Days"].fillna(value=-22222.0)
dat_1=dat_1.reset_index()


# In[114]:


dat_1["Returned & sold"]=""
for i in range(len(dat_1)):
    if(int(dat_1["No of Days"][i]>=0)):
        dat_1["Returned & sold"][i]="Check"


# In[115]:


dat_1_1=dat_1[dat_1["Returned & sold"]=="Check"]
dat_1_1=dat_1_1.sort_values(by=["sernr","fkdat","erdat","erzet"])


# In[116]:


ret_sale=dat_1_1.groupby(['sernr'])['fkart'].apply(','.join).reset_index()


# In[117]:


ret_sale["return&sold"]=""
for i in range(len(ret_sale)):
    s=str(ret_sale["fkart"][i])
    z=s.split(",")
    x=["YBF2"]
    if("YBF2" in z):
        ret_sale["return&sold"][i]="Yes"
    else:
        ret_sale["return&sold"][i]="No"


# In[118]:


re_sold=ret_sale[ret_sale["return&sold"]=="No"]
re_sold_no=re_sold["sernr"].to_list()


# In[119]:


for i in range(len(DOA_single)):
    if(str(DOA_single["Serial No"][i]) in re_sold_no):
        DOA_single["Exception Reason"][i]="Returned & Not Sold"


# In[120]:


No_re_but_sale=data_1[["sernr","fkdat","erdat","erzet","fkart"]]


# In[121]:


df_No_return_avi=DOA_single[DOA_single["Exception Reason"]=="No Return Date After DOA"]


# In[122]:


No_re_but_sale=No_re_but_sale.sort_values(by=["sernr","fkdat","erdat","erzet"])


# In[123]:


No_re_but_sale=No_re_but_sale.merge(df_No_return_avi[["Certificate Issued Date","Serial No"]],how="left",right_on=["Serial No"],left_on=["sernr"],indicator=True)


# In[124]:


No_re_but_sale_1=No_re_but_sale[No_re_but_sale["_merge"]=="both"].reset_index(drop=True)
No_re_but_sale_1=No_re_but_sale_1.sort_values(by=["sernr","fkdat","erdat","erzet"]).reset_index(drop=True)


# In[125]:


No_re_but_sale_1["No Return but sale"]=""
for i in range(len(No_re_but_sale_1)):
    a=No_re_but_sale_1["Certificate Issued Date"][i]
    b=No_re_but_sale_1["fkdat"][i]
    if(a<=b):
        No_re_but_sale_1["No Return but sale"][i]="Check"


# In[126]:


No_re_but_sale_2=No_re_but_sale_1[No_re_but_sale_1["No Return but sale"]=="Check"]


# In[127]:


No_re_but_sale_2=No_re_but_sale_2.groupby(['sernr'])['fkart'].apply(','.join).reset_index()


# In[128]:


No_re_but_sale_2.shape


# In[129]:


No_re_but_sale_2["no_return_but_sold"]=""
for i in range(len(No_re_but_sale_2)):
    s=str(No_re_but_sale_2["fkart"][i])
    z=s.split(",")
    if("YBF2" in z):
        No_re_but_sale_2["no_return_but_sold"][i]="Yes"
    else:
        No_re_but_sale_2["no_return_but_sold"][i]="No"


# In[130]:


No_re_but_sale_3=No_re_but_sale_2[No_re_but_sale_2["no_return_but_sold"]=="Yes"]


# In[131]:


noreturnbutsold=No_re_but_sale_3["sernr"].to_list()


# In[132]:


DOA_single["Exception Reason"]=DOA_single.apply(lambda x: "No Return but Sold After DOA" if str(x["Serial No"]) in noreturnbutsold else str(x["Exception Reason"]),axis=1)


# In[133]:


# Connection for SO data
conn=pymssql.connect(host='flexap.lenovo.com:1433',
user='a_app_bcdm',
password='Password@1',
database='DM_AP_BIZ'
)
cur=conn.cursor()


# In[134]:


def sql_data_f(a):
    cur=conn.cursor()
    cur.execute(a)
    data=cur.fetchall()
    headers=cur.description
    colm_names=[colm[0] for colm in headers]
    data=pd.DataFrame(data,columns=colm_names)
    return data


# In[135]:


# a=list(set(DOA_single["Serial No"].to_list()))
# aa="','".join(str(x) for x in a)
# aaa="('"+aa+"')"
# h="select * from INITSOL.V_CDMS_SO_DSR where SERIAL_NO IN "
# i=" and INVOICE_NO!=''"
# j=h+aaa+i


# In[136]:


# data_df_so= sql_data_f(j)


# In[137]:



b= 0
c= 30000
d=(len(DOA_single))
data_df_so=pd.DataFrame()
for i in range(int(d/30000)+1):
    data_2_=DOA_single["Serial No"][b:c].tolist()
    data_3="','".join([str(item) for item in data_2_])
    data_4="('"+data_3+"')"
    def data_processing():
#         h="select * from INITSOL.V_CDMS_SO_DSR where SERIAL_NO IN "
#         i=" and INVOICE_NO!=''"
#         j=h+data_4+i
        data_df_d=sql_data_f("select * from INITSOL.V_CDMS_SO_DSR where SERIAL_NO IN %s and INVOICE_NO!=''",data_4)
        return data_df_d
    data_df_d= data_processing()
    data_df_so=data_df_so.append(data_df_d)
    b=c
    c=c+30000


# In[ ]:


data_df_so.shape


# In[ ]:


sodata1=data_df_so.copy()


# In[ ]:


# data_df_so_k=data_df_so.sort_values(by=["SERIAL_NO","INVOICE_DATE"]).reset_index(drop=True)
# data_df_so_k["SERIAL_NO_lag"]=data_df_so_k["SERIAL_NO"].shift(1)
# data_df_so_k["runningsum"]=""
# for i in range(len(data_df_so_k)):
#     if(data_df_so_k["SERIAL_NO"][i]!=data_df_so_k["SERIAL_NO_lag"][i]):
#         data_df_so_k["runningsum"][i]=data_df_so_k["QTY"][i]
#     else:
#         data_df_so_k["runningsum"][i]=data_df_so_k["runningsum"][i-1]+data_df_so_k["QTY"][i]
        
# data_df_so_k=data_df_so_k.groupby(["SERIAL_NO"]).last().reset_index()
# data_df_so=data_df_so_k[data_df_so_k["runningsum"]>0]


# In[ ]:


data_df_so=data_df_so[data_df_so["QTY"]>0]


# In[ ]:


data_df_so["INVOICE_DATE"]=pd.to_datetime(data_df_so["INVOICE_DATE"],yearfirst=True).dt.date
data_df_so=data_df_so.sort_values(by=["SERIAL_NO","MTM","INVOICE_DATE"])
data_df_so["INVOICE_DATE"]=pd.to_datetime(data_df_so["INVOICE_DATE"],format= '%Y-%m-%d')


# In[ ]:


# so_groupby=data_df_so.groupby(["SERIAL_NO"]).agg(min_date_so=pd.NamedAgg(column="INVOICE_DATE",aggfunc=min)).reset_index()

# aa=so_groupby.merge(data_df_so[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",right_on=["SERIAL_NO","INVOICE_DATE"],left_on=["SERIAL_NO","min_date_so"])

# df_so_no_exc=aa[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]


# df_so_no_exc=df_so_no_exc.merge(DOA_single[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")

# df_so_no_exc_1=df_so_no_exc[df_so_no_exc["matching"]=="both"]

# df_so_no_exc_1["Less_r_Greater"]=""
# for i in range(len(df_so_no_exc_1)):
#     if(df_so_no_exc_1["INVOICE_DATE"][i] >= df_so_no_exc_1["Certificate Issued Date"][i]):
#         df_so_no_exc_1["Less_r_Greater"][i]="check"
        
# w_1=df_so_no_exc_1[df_so_no_exc_1["Less_r_Greater"]=="check"]
# w_2=w_1[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"])
# w_2_name=w_2.groupby(["SERIAL_NO"]).first().reset_index()
# wu_1=df_so_no_exc_1[df_so_no_exc_1["Less_r_Greater"]!="check"]
# wu_2=wu_1[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"],ascending=False)
# wu_2_name=wu_2.groupby(["SERIAL_NO"]).first().reset_index()
# w_2_name=w_2_name.append(wu_2_name)

# DOA_single=DOA_single.merge(so_groupby,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"],indicator="Ex_So")

# DOA_single=DOA_single.merge(w_2_name,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"])

# #DOA_single["Exception at SO"]=""
# #for i in range(len(DOA_single)):
# #    if(DOA_single["Ex_So"][i]=="both"):
# #        if(DOA_single["Certificate Issued Date"][i]< DOA_single["min_date_so"][i]):
# #            DOA_single["Exception at SO"][i]="Sell Out After DOA"
# #        else:
# #            DOA_single["Exception at SO"][i]="No Sell Out After DOA"


# In[124]:


data_df_so.shape


# In[125]:


d_so_s=data_df_so[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]
d_so_s_m=d_so_s.merge(DOA_single[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")
d_so_s_m["Diff of Days between DOA & SO"]=(d_so_s_m["Certificate Issued Date"]-d_so_s_m["INVOICE_DATE"]).dt.days
d_so_w1=d_so_s_m[d_so_s_m["Diff of Days between DOA & SO"]>=0]
d_so_w2=d_so_s_m[d_so_s_m["Diff of Days between DOA & SO"]<0]
d_so_w1_g=d_so_w1.groupby(["SERIAL_NO"]).agg({"INVOICE_DATE":max}).reset_index()
d_so_w2_g=d_so_w2.groupby(["SERIAL_NO"]).agg({"INVOICE_DATE":min}).reset_index()
ser_so_w1=d_so_w1_g["SERIAL_NO"].to_list()
d_so_w2_g.drop(d_so_w2_g[d_so_w2_g["SERIAL_NO"].isin(ser_so_w1)].index,inplace=True)
ser_sow=pd.concat([d_so_w1_g,d_so_w2_g])
ser_sow.shape
aa=ser_sow.merge(data_df_so[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",on=["SERIAL_NO","INVOICE_DATE"])


# In[ ]:





# In[126]:


## Bucket for "Diff of Days between DOA & SO"

# so_groupby=data_df_so.groupby(["SERIAL_NO"]).agg(min_date_so=pd.NamedAgg(column="INVOICE_DATE",aggfunc=min)).reset_index()

# aa=so_groupby.merge(data_df_so[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",right_on=["SERIAL_NO","INVOICE_DATE"],left_on=["SERIAL_NO","min_date_so"])

df_so_no_exc=aa[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]


df_so_no_exc=df_so_no_exc.merge(DOA_single[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")

df_so_no_exc_1=df_so_no_exc[df_so_no_exc["matching"]=="both"]

w_1=df_so_no_exc_1
w_2=w_1[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"])
w_2_name=w_2.groupby(["SERIAL_NO"]).first().reset_index()

DOA_single=DOA_single.merge(w_2_name,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"]).reset_index(drop=True)

DOA_single["Diff of Days between DOA & SO"]=(DOA_single["Certificate Issued Date"]-DOA_single["INVOICE_DATE"]).dt.days
DOA_single["Diff of Days between DOA & SO"]=DOA_single["Diff of Days between DOA & SO"].fillna(value=-22222.0)



# In[127]:


DOA_single["Bucket for SO"]=""
for j in range(len(DOA_single)):
    if(int(DOA_single["Diff of Days between DOA & SO"][j])== -22222):
        DOA_single["Bucket for SO"][j]="SO Data Not Available"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])< 0):
        if(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 7):
            DOA_single["Bucket for SO"][j]="0-7 Days after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 30):
            DOA_single["Bucket for SO"][j]="8-30 Days after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 60):
            DOA_single["Bucket for SO"][j]="31-60 Days after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 180):
            DOA_single["Bucket for SO"][j]="61-180 Days after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 365):
            DOA_single["Bucket for SO"][j]="181-365 Days after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 730):
            DOA_single["Bucket for SO"][j]="1-2 Years after DOA"
        elif(-int(DOA_single["Diff of Days between DOA & SO"][j])<= 1095):
            DOA_single["Bucket for SO"][j]="2-3 Years after DOA"
        else:
            DOA_single["Bucket for SO"][j]="Greater Than 3 Years after DOA"
            
            
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 7):
        DOA_single["Bucket for SO"][j]="0-7 Days before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 30):
        DOA_single["Bucket for SO"][j]="8-30 Days before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 60):
        DOA_single["Bucket for SO"][j]="31-60 Days before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 180):
        DOA_single["Bucket for SO"][j]="61-180 Days before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 365):
        DOA_single["Bucket for SO"][j]="181-365 Days before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 730):
        DOA_single["Bucket for SO"][j]="1-2 Years before DOA"
    elif(int(DOA_single["Diff of Days between DOA & SO"][j])<= 1095):
        DOA_single["Bucket for SO"][j]="2-3 Years before DOA"
    else:
        DOA_single["Bucket for SO"][j]="Greater Than 3 Years before DOA"


# In[128]:


#DOA_single[["Bucket for SO","Diff of Days between DOA & SO"]][ DOA_single["Diff of Days between DOA & SO"]== -22222.0] 

#DOA_single.to_excel('Bucket for SO.xlsx',index=False)


# In[129]:


#DOA_single[DOA_single["Ticket Id"]=="D190427012"]


# In[130]:


### Again Exception reason: "Duplicate DOA Processed"  ###

first_sold=data_1[data_1["fkart"]=="YBF2"]


# In[131]:


dat_2=first_sold.merge(DOA_single[["Serial No","Certificate Issued Date"]],right_on=["Serial No"],left_on=["sernr"],how="left",indicator="First_sold")


# In[132]:


dat_2=dat_2[dat_2["First_sold"]=="both"]


# In[133]:


dat_2["Days Diff DOA - YBF2"]=(dat_2["Certificate Issued Date"]-dat_2["fkdat"]).dt.days
dat_2["Days Diff DOA - YBF2"]=dat_2["Days Diff DOA - YBF2"].fillna(value=-22222.0)
dat_2["First_sold"]=""
dat_2=dat_2.reset_index()
for i in range(len(dat_2)):
    if(int(dat_2["Days Diff DOA - YBF2"][i]>=0)):
        dat_2["First_sold"][i]="Check"


# In[134]:


dat_2=dat_2[dat_2["First_sold"]=="Check"]


# In[135]:


dat_2_1=dat_2[["sernr","name1","fkdat","erdat","erzet"]].sort_values(by=["sernr","fkdat","erdat","erzet"],ascending=False)


# In[136]:


first_sold_group=dat_2_1.groupby(["sernr"]).first().reset_index()


# In[137]:


first_return_group=vbrp_check.groupby(["sernr"]).first().reset_index()


# In[138]:


DOA_single=DOA_single.merge(first_sold_group[["sernr","name1"]],how="left",left_on=["Serial No"],right_on=["sernr"])


# In[139]:


DOA_single=DOA_single.merge(first_return_group[["sernr","name1"]],how="left",left_on=["Serial No"],right_on=["sernr"])


# In[140]:


DOA_single=DOA_single.apply(lambda x: x.astype(str).str.upper())


# In[141]:


DOA_single["Sold by After Removing Stopwords"]=[re.sub("SERVICE","",str(x)) for x in DOA_single["name1_x"]]
DOA_single["Sold by After Removing Stopwords"]=[re.sub("INDIA","",str(x)) for x in DOA_single["name1_x"]]
DOA_single["Returned by After Removing Stopwords"]=[re.sub("SERVICE","",str(x)) for x in DOA_single["name1_y"]]
DOA_single["Returned by After Removing Stopwords"]=[re.sub("INDIA","",str(x)) for x in DOA_single["name1_y"]]


# In[142]:


key_words=["PVT","LTD","PRIVATE","AND","LIMITED","TRADER","TRADERS","INDUSTRIES","INDIA","BANK","BROTHER","BROTHERS","SONS","SYSTEMS","SYSTEM","COMPANY","SERVICES","LIMI","PVT.","LTD.","PVT.LTD","PVT.LTD.","PVT.LTD.","M/S","C/O"]


# In[143]:


def remove_stopwords(a):
    tokens = a.split(" ")
    tokens_filtered= [ word for word in tokens if not word in key_words]
    return (" ").join(tokens_filtered)


# In[144]:


DOA_single["Sold by After Removing Stopwords"]=DOA_single["Sold by After Removing Stopwords"].apply(remove_stopwords)


# In[145]:


DOA_single["Returned by After Removing Stopwords"]=DOA_single["Returned by After Removing Stopwords"].apply(remove_stopwords)


# In[146]:


DOA_single["Returned by After Removing Stopwords"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA_single["Returned by After Removing Stopwords"]]
DOA_single["Sold by After Removing Stopwords"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA_single["Sold by After Removing Stopwords"]]


# In[147]:


#df=DOA_single.copy()
#df["check"]=df.apply(lambda x: "Sold to & Received By Same" if x["Returned by After Removing Stopwords"]==x["Sold by After Removing Stopwords"] else "Sold to & Received by Diff", axis=1)
DOA_single["Seller Mismatch"]=DOA_single.apply(lambda x :"Sold by Not available" if x["name1_x"]=="NAN" else ("Returned by Not available" if x["name1_y"]=="NAN"  else ("Sold to & Received By Same" if x["Returned by After Removing Stopwords"]==x["Sold by After Removing Stopwords"] else "Sold to & Received by Diff")),axis=1)


# In[148]:


DOA_Duplicate_1=DOA_Duplicate[DOA_Duplicate["Exception Reason"]!=""]


# In[149]:


pd.unique(DOA_Duplicate_1["Exception Reason"])


# In[150]:


DOA_Duplicate_1["Exception Reason"]=DOA_Duplicate_1["Exception Reason"].apply(lambda x :"Duplicate DOA Processed" if str(x) =="" else x)


# In[151]:


vbrp_r_d=vbrp_Return.merge(DOA_Duplicate_1[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["sernr"])
vbrp_r_d["Certificate Issued Date"]=pd.to_datetime(vbrp_r_d["Certificate Issued Date"],format= '%Y-%m-%d')
vbrp_r_d["multi_returns"]=""

for i in range(len(vbrp_r_d)):
    a=vbrp_r_d["Certificate Issued Date"][i]
    b=vbrp_r_d["fkdat"][i]
    if(a<=b):
        vbrp_r_d["multi_returns"][i]="Check"

vbrp_check_d=vbrp_r_d[vbrp_r_d["multi_returns"]=="Check"]


# In[152]:


# Working for DOA Duplicateagain same process for material

vbrp_Return_groupby_d=vbrp_check_d.groupby(["sernr"]).agg(Min_date_return=pd.NamedAgg(column="fkdat",aggfunc=min)).reset_index()
Vbrp_return_group_d= vbrp_check_d.groupby(["sernr"])["fkart"].count()
Vbrp_return_group_d=Vbrp_return_group_d.to_frame()
Vbrp_return_group_d=Vbrp_return_group_d.reset_index()

vbrp_Return_groupby_1_d=vbrp_check_d.groupby(["sernr","matnr"]).agg(Min_date_return=pd.NamedAgg(column="fkdat",aggfunc=min)).reset_index()
        
DOA_Duplicate_1=DOA_Duplicate_1.merge(vbrp_Return_groupby_1_d[["sernr","matnr"]],how="left",right_on=["sernr","matnr"],left_on=["Serial No","MTM NO"],indicator="MTM Mismatch")
DOA_Duplicate_1=DOA_Duplicate_1.merge(vbrp_Return_groupby_d[["sernr","Min_date_return"]],how="left",right_on=["sernr"],left_on=["Serial No"],indicator="First return")


DOA_Duplicate_1["MTM Mismatch"]=DOA_Duplicate_1["MTM Mismatch"].map({'both': 'MTM SAME AS DOA', 'left_only': 'MTM DIFF FROM DOA'})



DOA_Duplicate_1["Certificate Issued Date"]=pd.to_datetime(DOA_Duplicate_1["Certificate Issued Date"],format= '%Y-%m-%d')
#DOA_single["Created On"]=DOA_single["Created On"].apply(lambda x : x.date() )


DOA_Duplicate_1["No of Days"]=(DOA_Duplicate_1["Min_date_return"]-DOA_Duplicate_1["Certificate Issued Date"]).dt.days
DOA_Duplicate_1["No of Days"]=DOA_Duplicate_1["No of Days"].fillna(value=-22222.0)


DOA_Duplicate_1["Bucket for No of Day"]=""
for j in range(len(DOA_Duplicate_1)):
    if(int(DOA_Duplicate_1["No of Days"][j])< 0):
        DOA_Duplicate_1["Bucket for No of Day"][j]="NO Returns"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 7):
        DOA_Duplicate_1["Bucket for No of Day"][j]="0-7 Days"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 30):
        DOA_Duplicate_1["Bucket for No of Day"][j]="8-30 Days"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 60):
        DOA_Duplicate_1["Bucket for No of Day"][j]="31-60 Days"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 180):
        DOA_Duplicate_1["Bucket for No of Day"][j]="61-180 Days"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 365):
        DOA_Duplicate_1["Bucket for No of Day"][j]="181-365 Days"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 730):
        DOA_Duplicate_1["Bucket for No of Day"][j]="1-2 Years"
    elif(int(DOA_Duplicate_1["No of Days"][j])<= 1095):
        DOA_Duplicate_1["Bucket for No of Day"][j]="2-3 Years"
    else:
        DOA_Duplicate_1["Bucket for No of Day"][j]="Greater Than 3 Years"
        


# a=list(set(DOA_Duplicate_1["Serial No"].to_list()))
# aa="','".join(str(x) for x in a)
# aaa="('"+aa+"')"
# h="select * from INITSOL.V_CDMS_SO_DSR where SERIAL_NO IN "
# i=" and INVOICE_NO!=''"
# j=h+aaa+i
# 
# 
# data_df_so_d= sql_data_f(j)
# 
# data_df_so_a=data_df_so_d.sort_values(by=["SERIAL_NO","INVOICE_DATE"]).reset_index(drop=True)
# data_df_so_a["SERIAL_NO_lag"]=data_df_so_a["SERIAL_NO"].shift(1)
# data_df_so_a["runningsum"]=""
# for i in range(len(data_df_so_a)):
#     if(data_df_so_a["SERIAL_NO"][i]!=data_df_so_a["SERIAL_NO_lag"][i]):
#         data_df_so_a["runningsum"][i]=data_df_so_a["QTY"][i]
#     else:
#         data_df_so_a["runningsum"][i]=data_df_so_a["runningsum"][i-1]+data_df_so_k["QTY"][i]
#         
# data_df_so_a=data_df_so_a.groupby(["SERIAL_NO"]).last().reset_index()
# data_df_so_d=data_df_so_a[data_df_so_a["runningsum"]>0]
# #data_df_so_d=data_df_so_d[data_df_so_d["QTY"]>0]
# 
# data_df_so_d["INVOICE_DATE"]=pd.to_datetime(data_df_so_d["INVOICE_DATE"],yearfirst=True).dt.date
# data_df_so_d=data_df_so_d.sort_values(by=["SERIAL_NO","MTM","INVOICE_DATE"])
# data_df_so_d["INVOICE_DATE"]=pd.to_datetime(data_df_so_d["INVOICE_DATE"],format= '%Y-%m-%d')
# 
# so_groupby_d=data_df_so_d.groupby(["SERIAL_NO"]).agg(min_date_so=pd.NamedAgg(column="INVOICE_DATE",aggfunc=min)).reset_index()
# 
# aa_d=so_groupby_d.merge(data_df_so_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",right_on=["SERIAL_NO","INVOICE_DATE"],left_on=["SERIAL_NO","min_date_so"])
# 
# df_so_no_exc_d=aa_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]
# 
# 
# df_so_no_exc_d=df_so_no_exc_d.merge(DOA_Duplicate_1[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")
# 
# df_so_no_exc_1_d=df_so_no_exc_d[df_so_no_exc_d["matching"]=="both"]
# 
# df_so_no_exc_1_d["Less_r_Greater"]=""
# for i in range(len(df_so_no_exc_1_d)):
#     if(df_so_no_exc_1_d["INVOICE_DATE"][i] >= df_so_no_exc_1_d["Certificate Issued Date"][i]):
#         df_so_no_exc_1_d["Less_r_Greater"][i]="check"
#         
# w_1_d=df_so_no_exc_1_d[df_so_no_exc_1_d["Less_r_Greater"]=="check"]
# w_2_d=w_1_d[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"])
# w_2_name_d=w_2_d.groupby(["SERIAL_NO"]).first().reset_index()
# wu_1_d=df_so_no_exc_1_d[df_so_no_exc_1_d["Less_r_Greater"]!="check"]
# wu_2_d=wu_1_d[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"],ascending=False)
# wu_2_name_d=wu_2_d.groupby(["SERIAL_NO"]).first().reset_index()
# w_2_name_d=w_2_name_d.append(wu_2_name_d)
# 
# DOA_Duplicate_1=DOA_Duplicate_1.merge(so_groupby_d,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"],indicator="Ex_So")
# 
# DOA_Duplicate_1=DOA_Duplicate_1.merge(w_2_name_d,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"])
# 
# #DOA_Duplicate_1["Exception at SO"]=""
# #for i in range(len(DOA_Duplicate_1)):
# #    if(DOA_Duplicate_1["Ex_So"][i]=="both"):
# #        if(DOA_Duplicate_1["Certificate Issued Date"][i]< DOA_Duplicate_1["min_date_so"][i]):
# #            DOA_Duplicate_1["Exception at SO"][i]="Sell Out After DOA"
# #        else:
# #            DOA_Duplicate_1["Exception at SO"][i]="No Sell Out After DOA"

# In[173]:


a=list(set(DOA_Duplicate_1["Serial No"].to_list()))
aa="','".join(str(x) for x in a)
aaa="('"+aa+"')"
h="select * from INITSOL.V_CDMS_SO_DSR where SERIAL_NO IN "
i=" and INVOICE_NO!=''"
j=h+aaa+i


data_df_so_d=sql_data_f(j)


# In[ ]:


# b= 0
# c= 30000
# d=(len(DOA_Duplicate_1))
# data_df_so_d=pd.DataFrame()
# for i in range(int(d/30000)+1):
#     data_2_=DOA_Duplicate_1["Serial No"][b:c].tolist()
#     data_3="','".join([str(item) for item in data_2_])
#     data_4="('"+data_3+"')"
#     def data_processing():
#         h="select * from INITSOL.V_CDMS_SO_DSR where INVOICE_NO!='' AND SERIAL_NO IN "
#         j=h+data_4
#         data_df_d=sql_data_f(j)
#         return data_df_d
#     data_df_d= data_processing()
#     data_df_so_d=data_df_so_d.append(data_df_d)
#     b=c
#     c=c+30000


# In[174]:


sodata2=data_df_so_d.copy()


# In[175]:


sodata2.shape# data_df_so_d.drop_duplicates()


# In[176]:



# data_df_so_a=data_df_so_d.sort_values(by=["SERIAL_NO","INVOICE_DATE"]).reset_index(drop=True)


# data_df_so_a["SERIAL_NO_lag"]=data_df_so_a["SERIAL_NO"].shift(1)
# data_df_so_a["runningsum"]=""
# for i in range(len(data_df_so_a)):
#     if(data_df_so_a["SERIAL_NO"][i]!=data_df_so_a["SERIAL_NO_lag"][i]):
#         data_df_so_a["runningsum"][i]=data_df_so_a["QTY"][i]
#     else:
#         data_df_so_a["runningsum"][i]=data_df_so_a["runningsum"][i-1]+data_df_so_a["QTY"][i]
        
# data_df_so_a=data_df_so_a.groupby(["SERIAL_NO"]).last().reset_index()
# data_df_so_d=data_df_so_a[data_df_so_a["runningsum"]>0]
data_df_so_d=data_df_so_d[data_df_so_d["QTY"]>0]


# In[177]:


data_df_so_d["INVOICE_DATE"]=pd.to_datetime(data_df_so_d["INVOICE_DATE"],yearfirst=True).dt.date
data_df_so_d=data_df_so_d.sort_values(by=["SERIAL_NO","MTM","INVOICE_DATE"])
data_df_so_d["INVOICE_DATE"]=pd.to_datetime(data_df_so_d["INVOICE_DATE"],format= '%Y-%m-%d')
DOA_Duplicate_1["Certificate Issued Date"]=pd.to_datetime(DOA_Duplicate_1["Certificate Issued Date"],format= '%Y-%m-%d')


# In[178]:


d_so_d=data_df_so_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]
d_so_d_m=d_so_d.merge(DOA_Duplicate_1[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")
d_so_d_m["Diff of Days between DOA & SO"]=(d_so_d_m["Certificate Issued Date"]-d_so_d_m["INVOICE_DATE"]).dt.days
d_so_d_w1=d_so_d_m[d_so_d_m["Diff of Days between DOA & SO"]>=0]
d_so_d_w2=d_so_d_m[d_so_d_m["Diff of Days between DOA & SO"]<0]
d_so_d_w1_g=d_so_d_w1.groupby(["SERIAL_NO"]).agg({"INVOICE_DATE":max}).reset_index()
d_so_d_w2_g=d_so_d_w2.groupby(["SERIAL_NO"]).agg({"INVOICE_DATE":min}).reset_index()
ser_so_d_w1=d_so_d_w1_g["SERIAL_NO"].to_list()
d_so_d_w2_g.drop(d_so_d_w2_g[d_so_d_w2_g["SERIAL_NO"].isin(ser_so_w1)].index,inplace=True)
ser_so_d_w=pd.concat([d_so_d_w1_g,d_so_d_w2_g])
ser_so_d_w.shape
aa_d=ser_so_d_w.merge(data_df_so_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",on=["SERIAL_NO","INVOICE_DATE"])


# In[179]:



d_so_d_m


# In[180]:







# so_groupby_d=data_df_so_d.groupby(["SERIAL_NO"]).agg(min_date_so=pd.NamedAgg(column="INVOICE_DATE",aggfunc=min)).reset_index()

# aa_d=so_groupby_d.merge(data_df_so_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]],how="left",right_on=["SERIAL_NO","INVOICE_DATE"],left_on=["SERIAL_NO","min_date_so"])

df_so_no_exc_d=aa_d[["SERIAL_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","INVOICE_DATE"]]


df_so_no_exc_d=df_so_no_exc_d.merge(DOA_Duplicate_1[["Serial No","Certificate Issued Date"]],how="left",right_on=["Serial No"],left_on=["SERIAL_NO"],indicator="matching")

df_so_no_exc_1_d=df_so_no_exc_d[df_so_no_exc_d["matching"]=="both"]
       
w_1_d=df_so_no_exc_1_d
w_2_d=w_1_d[["T2_PARTNER_NAME","T3_CUSTOMER_NAME","INVOICE_NO","SERIAL_NO","INVOICE_DATE"]].sort_values(by=["SERIAL_NO","INVOICE_DATE","INVOICE_NO"])
w_2_name_d=w_2_d.groupby(["SERIAL_NO"]).first().reset_index()


DOA_Duplicate_1=DOA_Duplicate_1.merge(w_2_name_d,how="left",left_on=["Serial No"],right_on=["SERIAL_NO"]).reset_index(drop=True)

DOA_Duplicate_1["Diff of Days between DOA & SO"]=(DOA_Duplicate_1["Certificate Issued Date"]-DOA_Duplicate_1["INVOICE_DATE"]).dt.days
DOA_Duplicate_1["Diff of Days between DOA & SO"]=DOA_Duplicate_1["Diff of Days between DOA & SO"].fillna(value=-22222.0)


# DOA_Duplicate_1["Bucket for SO"]=""
# for j in range(len(DOA_Duplicate_1)):
#     if(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])== -22222):
#         DOA_Duplicate_1["Bucket for SO"][j]="SO Data Not Available"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])< 0):
#         DOA_Duplicate_1["Bucket for SO"][j]="Sell Out After DOA"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 7):
#         DOA_Duplicate_1["Bucket for SO"][j]="0-7 Days"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 30):
#         DOA_Duplicate_1["Bucket for SO"][j]="8-30 Days"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 60):
#         DOA_Duplicate_1["Bucket for SO"][j]="31-60 Days"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 180):
#         DOA_Duplicate_1["Bucket for SO"][j]="61-180 Days"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 365):
#         DOA_Duplicate_1["Bucket for SO"][j]="181-365 Days"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 730):
#         DOA_Duplicate_1["Bucket for SO"][j]="1-2 Years"
#     elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 1095):
#         DOA_Duplicate_1["Bucket for SO"][j]="2-3 Years"
#     else:
#         DOA_Duplicate_1["Bucket for SO"][j]="Greater Than 3 Years"


# In[181]:


DOA_Duplicate_1["Bucket for SO"]=""
for j in range(len(DOA_Duplicate_1)):
    if(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])== -22222):
        DOA_Duplicate_1["Bucket for SO"][j]="SO Data Not Available"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])< 0):
        if(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 7):
            DOA_Duplicate_1["Bucket for SO"][j]="0-7 Days after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 30):
            DOA_Duplicate_1["Bucket for SO"][j]="8-30 Days after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 60):
            DOA_Duplicate_1["Bucket for SO"][j]="31-60 Days after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 180):
            DOA_Duplicate_1["Bucket for SO"][j]="61-180 Days after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 365):
            DOA_Duplicate_1["Bucket for SO"][j]="181-365 Days after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 730):
            DOA_Duplicate_1["Bucket for SO"][j]="1-2 Years after DOA"
        elif(-int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 1095):
            DOA_Duplicate_1["Bucket for SO"][j]="2-3 Years after DOA"
        else:
            DOA_Duplicate_1["Bucket for SO"][j]="Greater Than 3 Years after DOA"
            
            
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 7):
        DOA_Duplicate_1["Bucket for SO"][j]="0-7 Days before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 30):
        DOA_Duplicate_1["Bucket for SO"][j]="8-30 Days before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 60):
        DOA_Duplicate_1["Bucket for SO"][j]="31-60 Days before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 180):
        DOA_Duplicate_1["Bucket for SO"][j]="61-180 Days before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 365):
        DOA_Duplicate_1["Bucket for SO"][j]="181-365 Days before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 730):
        DOA_Duplicate_1["Bucket for SO"][j]="1-2 Years before DOA"
    elif(int(DOA_Duplicate_1["Diff of Days between DOA & SO"][j])<= 1095):
        DOA_Duplicate_1["Bucket for SO"][j]="2-3 Years before DOA"
    else:
        DOA_Duplicate_1["Bucket for SO"][j]="Greater Than 3 Years before DOA"


# In[182]:


first_sold_d=data_1[data_1["fkart"]=="YBF2"]

dat_2_d=first_sold_d.merge(DOA_Duplicate_1[["Serial No","Certificate Issued Date"]],right_on=["Serial No"],left_on=["sernr"],how="left",indicator="First_sold")

dat_2_d=dat_2_d[dat_2_d["First_sold"]=="both"]

dat_2_d["Certificate Issued Date"]=pd.to_datetime(dat_2_d["Certificate Issued Date"],format= '%Y-%m-%d')
dat_2_d["Days Diff DOA - YBF2"]=(dat_2_d["Certificate Issued Date"]-dat_2_d["fkdat"]).dt.days
dat_2_d["Days Diff DOA - YBF2"]=dat_2_d["Days Diff DOA - YBF2"].fillna(value=-22222.0)
dat_2_d["First_sold"]=""
dat_2_d=dat_2_d.reset_index()
for i in range(len(dat_2_d)):
    if(int(dat_2_d["Days Diff DOA - YBF2"][i]>=0)):
        dat_2_d["First_sold"][i]="Check"
        
dat_2_d=dat_2_d[dat_2_d["First_sold"]=="Check"]

dat_2_1_d=dat_2_d[["sernr","name1","fkdat","erdat","erzet"]].sort_values(by=["sernr","fkdat","erdat","erzet"],ascending=False)

first_sold_group_d=dat_2_1_d.groupby(["sernr"]).first().reset_index()

first_return_group_d=vbrp_check_d.groupby(["sernr"]).first().reset_index()

DOA_Duplicate_1=DOA_Duplicate_1.merge(first_sold_group_d[["sernr","name1"]],how="left",left_on=["Serial No"],right_on=["sernr"])

DOA_Duplicate_1=DOA_Duplicate_1.merge(first_return_group_d[["sernr","name1"]],how="left",left_on=["Serial No"],right_on=["sernr"])

DOA_Duplicate_1=DOA_Duplicate_1.apply(lambda x: x.astype(str).str.upper())

DOA_Duplicate_1["Sold by After Removing Stopwords"]=[re.sub("SERVICE","",str(x)) for x in DOA_Duplicate_1["name1_x"]]
DOA_Duplicate_1["Sold by After Removing Stopwords"]=[re.sub("INDIA","",str(x)) for x in DOA_Duplicate_1["name1_x"]]
DOA_Duplicate_1["Returned by After Removing Stopwords"]=[re.sub("SERVICE","",str(x)) for x in DOA_Duplicate_1["name1_y"]]
DOA_Duplicate_1["Returned by After Removing Stopwords"]=[re.sub("INDIA","",str(x)) for x in DOA_Duplicate_1["name1_y"]]

DOA_Duplicate_1["Sold by After Removing Stopwords"]=DOA_Duplicate_1["Sold by After Removing Stopwords"].apply(remove_stopwords)
DOA_Duplicate_1["Returned by After Removing Stopwords"]=DOA_Duplicate_1["Returned by After Removing Stopwords"].apply(remove_stopwords)

DOA_Duplicate_1["Returned by After Removing Stopwords"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA_Duplicate_1["Returned by After Removing Stopwords"]]
DOA_Duplicate_1["Sold by After Removing Stopwords"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in DOA_Duplicate_1["Sold by After Removing Stopwords"]]

DOA_Duplicate_1["Seller Mismatch"]=DOA_Duplicate_1.apply(lambda x :"Sold by Not available" if x["name1_x"]=="NAN" else ("Returned by Not available" if x["name1_y"]=="NAN"  else ("Sold to & Received By Same" if x["Returned by After Removing Stopwords"]==x["Sold by After Removing Stopwords"] else "Sold to & Received by Diff")),axis=1)


# In[183]:


DOA_Duplicate_1["MTM Mismatch"]=DOA_Duplicate_1.apply(lambda x :"" if x["Bucket for No of Day"] in ["NO RETURNS"] else x["MTM Mismatch"],axis=1)


# In[184]:


DOA_Duplicate_1=DOA_Duplicate_1[["Month","Year","Ticket Id","Created On","State","City","MTM NO","Serial No","INVOICE_DATE","INVOICE_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","Diff of Days between DOA & SO","Bucket for SO","Exception Reason","Seller Mismatch","MTM Mismatch","Min_date_return","No of Days","Bucket for No of Day","Model","Product Category","Certificate Issued Date","Refund Amount","name1_x","name1_y","Organisation","UAT Ref No","Customer Sub Segment","StatusName"]]


# In[185]:


DOA_single=DOA_single[["Month","Year","Ticket Id","Created On","State","City","MTM NO","Serial No","INVOICE_DATE","INVOICE_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","Diff of Days between DOA & SO","Bucket for SO","Exception Reason","Seller Mismatch","MTM Mismatch","Min_date_return","No of Days","Bucket for No of Day","Model","Product Category","Certificate Issued Date","Refund Amount","name1_x","name1_y","Organisation","UAT Ref No","Customer Sub Segment","StatusName"]]


# In[186]:


DOA_single["MTM Mismatch"]=DOA_single.apply(lambda x :"" if x["Bucket for No of Day"] in ["NO RETURNS"] else x["MTM Mismatch"],axis=1)


# In[187]:


DOA_single=DOA_single.append(DOA_Duplicate_1)


# In[188]:


morethan1_1=DOA_single[DOA_single["Exception Reason"]=="MORE THAN 1 RETURN AFTER DOA"]
morethan1=DOA_single[DOA_single["Exception Reason"]=="MORE THAN 1 RETURN AFTER DOA"].reset_index()


# In[189]:


for i in range(len(morethan1)):
    ser=str(morethan1["Serial No"][i])
    con=morethan1[morethan1["Serial No"]==ser]
    df_vbr=data_1[data_1["sernr"]==ser]
    df_vbr_2=morethan1_1[morethan1_1["Serial No"]==ser]
    df_vbr=df_vbr.reset_index(drop=True)
    df_vbr=df_vbr.merge(con[["Serial No","Certificate Issued Date"]],how="left",right_on="Serial No",left_on="sernr")
    df_vbr["befor or not"]=""
    df_vbr["Certificate Issued Date"]=pd.to_datetime(df_vbr["Certificate Issued Date"],format= '%Y-%m-%d')
    for l in range(len(df_vbr)):
        if(df_vbr["Certificate Issued Date"][l]<= df_vbr["fkdat"][l]):
            df_vbr["befor or not"][l]="check"
    df_vbr_fil=df_vbr[df_vbr["befor or not"]=="check"]
    df_vbr_fil=df_vbr_fil.sort_values(by=["sernr","fkdat","erdat","erzet"])
    df_vbr_1=df_vbr_fil[["sernr","fkart","fkdat","erdat","erzet"]].reset_index(drop=True)
    df_vbr_1["fkart_num"]=""
    for j in range(len(df_vbr_1)):
        if(df_vbr_1["fkart"][j]=="YBS2"):
            df_vbr_1["fkart_num"][j]=-1
        elif(df_vbr_1["fkart"][j]=="YBRE"):
            df_vbr_1["fkart_num"][j]=1
        else:
            df_vbr_1["fkart_num"][j]=0
    group_vbr_morethan1=df_vbr_1.groupby(["sernr"]).agg(sum_doc_type=pd.NamedAgg(column="fkart_num",aggfunc=sum)).reset_index()
    kap=df_vbr_2.index.to_list()
    na=kap[0]
    if(group_vbr_morethan1["sum_doc_type"][0] <= 1):
        DOA_single["Exception Reason"][na]=""


# In[190]:


DOA_single["No of Days"]=DOA_single["No of Days"].apply(lambda x :"" if x =="-22222.0" else x)


# In[191]:


DOA_single["Diff of Days between DOA & SO"]=DOA_single["Diff of Days between DOA & SO"].apply(lambda x :"" if x =="-22222.0" else x)


# In[192]:


# Formating


# In[193]:


DOA_single["INVOICE_NO"]=DOA_single["INVOICE_NO"].apply(lambda x :"" if str(x) =="NAN" else x)


# In[194]:


DOA_single["T2_PARTNER_NAME"]=DOA_single["T2_PARTNER_NAME"].apply(lambda x :"" if str(x) =="NAN" else x)


# In[195]:


DOA_single["T3_CUSTOMER_NAME"]=DOA_single["T3_CUSTOMER_NAME"].apply(lambda x :"" if str(x) =="NAN" else x)


# In[196]:


DOA_single["INVOICE_DATE"]=DOA_single["INVOICE_DATE"].apply(lambda x :"" if str(x) =="NAT" else x)


# DOA_single["Exception at SO"]=DOA_single["Exception at SO"].apply(lambda x :"SN Not found in Sell Out Report" if str(x) =="" else x)

# In[197]:


sql_data_fDOA_single["Exception Reason"]=DOA_single["Exception Reason"].apply(lambda x :"NO EXCEPTIONS" if str(x) =="" else x)


# In[198]:


DOA_single["Min_date_return"]=DOA_single["Min_date_return"].apply(lambda x :"" if str(x) =="NAT" else x)


# In[199]:


DOA_single["name1_x"]=DOA_single["name1_x"].apply(lambda x :"" if str(x) =="NAN" else x)


# In[200]:


DOA_single["name1_y"]=DOA_single["name1_y"].apply(lambda x :"" if str(x) =="NAN" else x)


# In[201]:


DOA_single=DOA_single.reset_index(drop=True)


# In[202]:


DOA_single=DOA_single.apply(lambda x: x.astype(str).str.upper())


# In[203]:


billing_types=pd.read_excel("BillingDocTypes.xlsx")


# In[204]:


billing_types.rename(columns={"Billing doc type":"fkart"},inplace=True)


# In[205]:


data_1=data_1.merge(billing_types,on="fkart",how="left")


# In[206]:


df_makt=DOA_single[DOA_single["MTM NO"]!=''].drop_duplicates(["MTM NO"])
df_makt["MTM NO"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in df_makt["MTM NO"]]
k="select * from "
h=" where matnr IN "
b= 0
c= 60000
d=(len(data_df_st_1))
makt_df=pd.DataFrame()
for i in range(int(d/60000)+1):
    df_makt_1=df_makt["MTM NO"][b:c].tolist()
    df_makt_2="','".join(str(x) for x in df_makt_1)
    data_df_st_5="('"+df_makt_2+"')"
    def data_processing_2():
        rows,cols=read_data("prd_updated.ecc_makt_rt_udl",k,h,data_df_st_5)
        column = [col[0] for col in cols]
        data_3=pd.DataFrame(rows,columns=column)
        return data_3
    data_3= data_processing_2()
    data_3=data_3[data_3["spras"]=="E"].reset_index(drop=True)
    makt_df=makt_df.append(data_3)
    #data_df_vbrp_sr.append(data_t_ver)
    b=c
    c=c+60000


# In[207]:


makt_df=makt_df[["matnr","maktx"]]


# In[208]:


makt_df.rename(columns={"matnr":"MTM NO","maktx":"Material Desc"},inplace=True)


# In[209]:


DOA_single=DOA_single.merge(makt_df,on="MTM NO",how="left")


# In[210]:


# DOA_single[DOA_single["Refund Amount"]=="NONE"]
DOA_single.loc[DOA_single["Refund Amount"] == "NONE", "Refund Amount"] ="0.00"


# In[211]:


DOA_single["Refund Amount"]=DOA_single["Refund Amount"].astype(float)
DOA_single=DOA_single[["Month","Year","Ticket Id","Created On","State","City","MTM NO","Material Desc","Serial No","INVOICE_DATE","INVOICE_NO","T2_PARTNER_NAME","T3_CUSTOMER_NAME","Diff of Days between DOA & SO","Bucket for SO","Exception Reason","MTM Mismatch","Min_date_return","No of Days","Bucket for No of Day","Model","Product Category","Certificate Issued Date","Refund Amount","Seller Mismatch","name1_x","name1_y","Organisation","UAT Ref No","Customer Sub Segment","StatusName"]]
DOA_single.rename(columns={"INVOICE_DATE":"SO Date","INVOICE_NO":"SO #","Min_date_return":"1st Return Date after DOA","Created On":"DOA Created On","name1_x":"Sold By","name1_y":"Returned By"},inplace=True)
data_1.rename(columns={"vbeln":"Billing_Doc","posnr":"Item","fkimg":"Billed_Qty","vrkme":"Sales_Unit","vgbel":"Ref_Doc_No","vgpos":"Ref_Doc_Item","aubel":"Sales_Document","aupos":"Sales_Doc_Item","matnr":"Material","charg":"Batch","matkl":"Material_Group","pstyv":"SD Item category","posar":"Item_Type","werks":"Plant","autyp":"SD_Doc_Category","fkart":"Billing_Type","fktyp":"Billing_Category","vbtyp":"Document_category_of_preceding_SD_document","weark":"SD Doc Curr","vtweg":"Distribution_Channel","fkdat":"Billing_Date","kunag":"Sold_To_Party","bukrs":"Company","butxt":"Company_Name","ort01":"City","land1":"Country_Key","netwr":"Net_Value","sernr":"Serial Number","erdat":"Entry Date","erzet":"Entry Time","waerk":"SD Document Currency","maktx":"Material desc","spras":"Language","mtart":"Material Type"},inplace=True)


# In[212]:


mat_sr=DOA_single[["Serial No"]].drop_duplicates()
mat_sr["CaseID"]=np.arange(len(mat_sr))
DOA_single_1=DOA_single.merge(mat_sr,how="left",left_on=["Serial No"],right_on=["Serial No"])
DOA_single_1=DOA_single_1.sort_values(by=["CaseID"])
DOA_single_1=DOA_single_1[["CaseID","Month","Year","Ticket Id","DOA Created On","State","City","MTM NO","Material Desc","Serial No","SO Date","SO #","T2_PARTNER_NAME","T3_CUSTOMER_NAME","Diff of Days between DOA & SO","Bucket for SO","Exception Reason","MTM Mismatch","1st Return Date after DOA","No of Days","Bucket for No of Day","Model","Product Category","Certificate Issued Date","Refund Amount","Seller Mismatch","Sold By","Returned By","Organisation","UAT Ref No","Customer Sub Segment","StatusName"]]
data_1=data_1[["Billing_Doc", "Item", "Billed_Qty", "Sales_Unit", "Ref_Doc_No","Ref_Doc_Item", "Sales_Document", "Sales_Doc_Item", "Material", "Batch","Material_Group", "SD Item category", "Item_Type", "Plant","SD_Doc_Category", "Net_Value", "Billing_Type","Billing doc type desc", "Billing_Category","Document_category_of_preceding_SD_document", "SD Document Currency","Distribution_Channel", "Billing_Date", "Sold_To_Party", "Company","Entry Date", "Entry Time", "Serial Number", "Company_Name", "City","Country_Key", "Material desc", "Language", "Material Type", "name1"]]


# In[213]:


z=abc["Serial No"].to_list()
DOA_single_1["Exception Reason"]=DOA_single_1.apply(lambda x :"DUPLICATE DOA PROCESSED" if str(x["Serial No"]) in z else x["Exception Reason"],axis=1)


# In[214]:


#DOA_single.index.name="CaseId"
df_1=DOA_single_1

vbrp_sale_data=data_1[data_1["Billing_Type"]=="YBF2"]
vbrp_sale_data=vbrp_sale_data[["Serial Number","Billing_Date"]]
df_Doasingle=df_1.merge(vbrp_sale_data,left_on=["Serial No"],right_on=["Serial Number"],how="left")


# In[215]:


df_Doasingle.shape


# In[216]:


# df_Doasingle["Billing_Date"]=pd.to_datetime(df_Doasingle["Billing_Date"])
df_Doasingle["Certificate Issued Date"]=pd.to_datetime(df_Doasingle["Certificate Issued Date"])


# In[ ]:





# In[217]:


vbrp_sale_data_1=df_Doasingle[df_Doasingle["Billing_Date"] < df_Doasingle["Certificate Issued Date"]]


# In[218]:


first_sale=vbrp_sale_data_1.groupby(["Serial Number"]).agg(First_sale=pd.NamedAgg(column="Billing_Date",aggfunc=max)).reset_index()

df_1=df_1.merge(first_sale,left_on=["Serial No"],right_on=["Serial Number"],how="left",indicator=True)


# In[219]:


df_1.shape


# In[220]:


pd.unique(df_1["_merge"])


# In[221]:


df_1[df_1["_merge"]=="left_only"].shape


# In[222]:


left_only=df_1[df_1["_merge"]=="left_only"]
left_only.columns.values


# In[223]:


left_only_1=left_only.merge(vbrp_sale_data,left_on=["Serial No"],right_on=["Serial Number"],how="left")
print(left_only_1.shape)
first_sale_1=left_only_1.groupby(["Serial No"]).agg(First_sale_1=pd.NamedAgg(column="Billing_Date",aggfunc=min)).reset_index()
print(left_only_1.shape)


# In[224]:


df_1=df_1.merge(first_sale_1,left_on=["Serial No"],right_on=["Serial No"],how="left")


# In[225]:


df_1.shape


# In[226]:


df_1["First_sale"]=df_1["First_sale"].fillna("")


# In[227]:


for i in range(0,len(df_1["Serial No"])):
    if df_1["First_sale"][i]=="":
        df_1["First_sale"][i]=df_1["First_sale_1"][i]
    


# In[228]:


df_1["First_sale"]=pd.to_datetime(df_1["First_sale"])


# In[229]:


df_1=df_1.drop(['First_sale_1','_merge'],axis=1)


# In[230]:


# first_sale=vbrp_sale_data_1.groupby(["Serial Number"]).agg(First_sale=pd.NamedAgg(column="Billing_Date",aggfunc=max)).reset_index()

# df_1=df_1.merge(first_sale,left_on=["Serial No"],right_on=["Serial Number"],how="left",indicator=True)

df_1["Certificate Issued Date"]=pd.to_datetime(df_1["Certificate Issued Date"],format= '%Y-%m-%d')

df_1["First sale to DOA"]=(df_1["Certificate Issued Date"]-df_1["First_sale"]).dt.days


# In[231]:


# #DOA_single.index.name="CaseId"
# df_1=DOA_single_1

# vbrp_sale_data=data_1[data_1["Billing_Type"]=="YBF2"]
# vbrp_sale_data=vbrp_sale_data[["Serial Number","Billing_Date"]]
# first_sale=vbrp_sale_data.groupby(["Serial Number"]).agg(First_sale=pd.NamedAgg(column="Billing_Date",aggfunc=max)).reset_index()

# df_1=df_1.merge(first_sale,left_on=["Serial No"],right_on=["Serial Number"],how="left")

# df_1["Certificate Issued Date"]=pd.to_datetime(df_1["Certificate Issued Date"],format= '%Y-%m-%d')

# df_1["First sale to DOA"]=(df_1["Certificate Issued Date"]-df_1["First_sale"]).dt.days



# # with pd.ExcelWriter('DOA.xlsx') as writer:
# #     df_1.to_excel(writer,sheet_name="DOA",index=False)
# #     data_1.to_excel(writer,sheet_name="VBRP",index=False)


# In[232]:


df_1["Bucket for sale to DOA"]=""
df_1["First sale to DOA"]=df_1["First sale to DOA"].fillna("0")
for j in range(len(df_1)):
    if(df_1.loc[j,"First sale to DOA"]!=None):
        if(int(df_1["First sale to DOA"][j])< 0):
            df_1["Bucket for sale to DOA"][j]="DOA Created before any sales"
        elif(int(df_1["First sale to DOA"][j])<= 7):
            df_1["Bucket for sale to DOA"][j]="0-7 Days"
        elif(int(df_1["First sale to DOA"][j])<= 30):
            df_1["Bucket for sale to DOA"][j]="8-30 Days"
        elif(int(df_1["First sale to DOA"][j])<= 60):
            df_1["Bucket for sale to DOA"][j]="31-60 Days"
        elif(int(df_1["First sale to DOA"][j])<= 180):
            df_1["Bucket for sale to DOA"][j]="61-180 Days"
        elif(int(df_1["First sale to DOA"][j])<= 365):
            df_1["Bucket for sale to DOA"][j]="181-365 Days"
        elif(int(df_1["First sale to DOA"][j])<= 730):
            df_1["Bucket for sale to DOA"][j]="1-2 Years"
        elif(int(df_1["First sale to DOA"][j])<= 1095):
            df_1["Bucket for sale to DOA"][j]="2-3 Years"
        else:
            df_1["Bucket for sale to DOA"][j]="Greater Than 3 Years"
    else:
        df_1["Bucket for sale to DOA"][j]="Serial Number not found in Sell In"


# In[233]:


df_1["Bucket for sale to DOA"]=np.where(df_1["First_sale"].isna(),(np.where(df_1["Certificate Issued Date"].isna(),"DOA DATE NOT FOUND","SI data not available")),df_1["Bucket for sale to DOA"])


# In[234]:


df_1[df_1["First_sale"].isna()]


# In[235]:


df_1.columns.values


# In[236]:


# df_1[df_1['Certificate Issued Date']=="NaT"]
df_1_1=df_1.copy()
vbrp_data=data_2_vbrp[['sernr','matnr']].drop_duplicates().reset_index(drop=True)
df_1_1.shape


# In[237]:


print(vbrp_data.shape)
vbrp_data=vbrp_data.drop_duplicates()
vbrp_data.shape


# In[238]:


df_1_1=df_1_1.merge(vbrp_data[["sernr","matnr"]],right_on=["sernr","matnr"],left_on=["Serial No","MTM NO"],how="left",indicator="MTM_Mismatch")
df_1_1["MTM_Mismatch"]=df_1_1["MTM_Mismatch"].map({'both': 'MTM SAME AS DOA', 'left_only': 'MTM DIFF FROM DOA'})


# In[239]:


df_1_1.shape


# In[240]:


for i in range(0,len(df_1_1)-1):
    if df_1_1["MTM Mismatch"][i]=="":
        df_1_1["MTM Mismatch"][i]=df_1_1["MTM_Mismatch"][i]       
    


# In[241]:


df_1_1[["MTM Mismatch","MTM_Mismatch"]]


# In[242]:


df_1_1=df_1_1.drop(["MTM_Mismatch","sernr","matnr"], axis=1)
df_1_1.shape


# In[243]:


df_1_1['Certificate Issued Date_dup']=df_1_1['Certificate Issued Date']
df_1_1['Certificate Issued Date_dup']
df_1_1['Certificate Issued Date_dup']=df_1_1['Certificate Issued Date_dup'].fillna(0)
df_cer_not_na=df_1_1[df_1_1['Certificate Issued Date_dup']!=0]
df_cer_na=df_1_1[df_1_1['Certificate Issued Date_dup']==0]
df_cer_na['Certificate Issued Date_dup']


# In[244]:


data_1.columns.values


# In[245]:


df_3=df_cer_na

vbrp_sale_data=data_1[data_1["Billing_Type"]=="YBF2"]
vbrp_sale_data=vbrp_sale_data[["Serial Number","Billing_Date"]]


# In[246]:


df_4=df_3.merge(vbrp_sale_data,left_on=["Serial No"],right_on=["Serial Number"],how="left")


# In[247]:


df_5=df_4.groupby(["Serial No"]).agg(First_sale_x=pd.NamedAgg(column="Billing_Date",aggfunc=max)).reset_index()


# In[248]:


df_5.columns.values


# In[249]:


df_6=df_3.merge(df_5,left_on=["Serial No"],right_on=["Serial No"],how="left",indicator=True)


# In[250]:


df_6.columns.values


# In[251]:


df_6["First_sale"]=df_6["First_sale_x"]
df_6['First sale to DOA']=''
df_6['Bucket for sale to DOA']='DOA DATE NOT FOUND'   


# In[252]:


df_6=df_6.drop(["First_sale_x","_merge","Certificate Issued Date_dup"], axis=1)
df_6.shape


# In[253]:


df_cer_not_na=df_cer_not_na.drop(["Certificate Issued Date_dup"], axis=1)


# In[254]:


df_1_1_1 = pd.concat([df_cer_not_na, df_6])
df_1_1_1.shape


# In[255]:


df_final=df_1_1_1.copy()
df_no_return=df_1_1_1[df_1_1_1["Exception Reason"]=="NO RETURN DATE AFTER DOA"]
df_final=df_1_1_1[df_1_1_1["Exception Reason"]!="NO RETURN DATE AFTER DOA"]


# In[256]:


df_1_1_1.shape


# In[257]:


vbrp_data=data_2_vbrp.drop_duplicates(['vbeln','posnr','sernr','matnr']).reset_index(drop=True)



df_no_return_vbrp=df_no_return.merge(vbrp_data[["fkdat","sernr","fkart"]],right_on=["sernr"],left_on=["Serial No"],how="left")


# In[258]:


# df_no_return_vbrp.to_excel("df_no_return_vbrp.xlsx")


# In[259]:


df_no_return_vbrp["fkdat"]=pd.to_datetime(df_no_return_vbrp["fkdat"],format='%Y-%m-%d')
df_no_return_vbrp_aft_doa=df_no_return_vbrp[df_no_return_vbrp["fkdat"]>df_no_return_vbrp["Certificate Issued Date"]]
df_no_return_vbrp_aft_doa=df_no_return_vbrp_aft_doa.sort_values(by=["Ticket Id","Serial No","fkdat"]).reset_index(drop=True)
df_no_return_vbrp_aft_doa["Exception Reason"]="NO RETURN BUT TRANSACTION AFTER DOA" 
#df_no_return_vbrp_aft_doa["Exception Reason"]=df_no_return_vbrp_aft_doa.apply(lambda x: "No Return but Transaction After DoA" if x["fkart"]!="YBF2" or x["fkart"]!="YBRE" elif "NO RETURN BUT SOLD AFTER DOA" if x["fkart"]=="YBF2",axis=1)
df_no_return_vbrp_aft_doa=df_no_return_vbrp_aft_doa.drop_duplicates(["Ticket Id","Serial No"]).reset_index(drop=True)
df_no_return_vbrp_aft_doa["ticket_srno"]=df_no_return_vbrp_aft_doa["Ticket Id"]+"-"+df_no_return_vbrp_aft_doa["Serial No"]
trans_ticket_ids=df_no_return_vbrp_aft_doa["ticket_srno"].to_list()

df_no_return_vbrp["ticket_srno"]=df_no_return_vbrp["Ticket Id"]+"-"+df_no_return_vbrp["Serial No"]
df_no_return_vbrp_no_trans=df_no_return_vbrp[~df_no_return_vbrp["ticket_srno"].str.contains('|'.join(trans_ticket_ids),case=False).any(level=0)]
df_no_return_vbrp_no_trans=df_no_return_vbrp_no_trans.drop_duplicates(["Ticket Id","Serial No"]).reset_index(drop=True)                                                                                          
df_no_return_vbrp_aft_doa=df_no_return_vbrp_aft_doa.drop(['fkdat','sernr','fkart','ticket_srno'],axis=1)
df_no_return_vbrp_no_trans=df_no_return_vbrp_no_trans.drop(['fkdat','sernr','fkart','ticket_srno'],axis=1)
df_no_ret_final=df_no_return_vbrp_aft_doa.append(df_no_return_vbrp_no_trans,ignore_index=True)   


# In[260]:


final_op=df_final.append(df_no_ret_final,ignore_index=True)
final_op.sort_values(by=["CaseID"],inplace=True)
final_op=final_op.drop(["Serial Number"],axis=1)


# In[261]:


data_1.columns.values

final_op["DOA Created On"]=pd.to_datetime(final_op["DOA Created On"],format= '%Y-%m-%d')


# In[262]:


vbrp_data_drp_dup=data_1.drop_duplicates(['Billing_Doc','Item','Material','Serial Number']).reset_index()


# In[263]:


final_op.columns.values


# In[264]:


final_op.rename(columns={"SO Date":"Latest SO sale Date before DOA","T2_PARTNER_NAME":"T2_PARTNER_NAME_SO","T3_CUSTOMER_NAME":"T3_CUSTOMER_NAME_SO","1st Return Date after DOA":"1st Return Date after DOA SI","No of Days":"No of Days Between 1st Return SI & DOA Certificate Date","MTM Mismatch":"MTM Mismatch SI","Bucket for No of Day":"Bucket for No of Day Return SI Vs DOA","Exception Reason":"Exception Reason SI","Seller Mismatch":"Seller Mismatch SI","Sold By":"Sold By SI","Returned By":"Returned By SI","First_sale":"Latest_sale_before_DOA_SI","First sale to DOA":"Difference between Latest sale SI to DOA"},inplace=True)
final_op=final_op[["CaseID","Month","Year","Ticket Id","DOA Created On","State","City","MTM NO","Material Desc","Serial No","Latest SO sale Date before DOA","SO #","T2_PARTNER_NAME_SO","T3_CUSTOMER_NAME_SO","Diff of Days between DOA & SO","Bucket for SO","1st Return Date after DOA SI","No of Days Between 1st Return SI & DOA Certificate Date","MTM Mismatch SI","Bucket for No of Day Return SI Vs DOA","Exception Reason SI","Model","Product Category","Certificate Issued Date",	"Refund Amount","Seller Mismatch SI","Sold By SI","Returned By SI","Organisation","UAT Ref No","Customer Sub Segment","StatusName","Latest_sale_before_DOA_SI","Difference between Latest sale SI to DOA","Bucket for sale to DOA"]]


# In[265]:


final_op=final_op.drop_duplicates()
final_op.shape


# In[266]:



# Serial_no=final_op.drop_duplicates(["Serial No"])
# Serial_no=Serial_no[Serial_no["Serial No"]!=""]
# Serial_no=Serial_no["Serial No"].reset_index(drop=True)
# Serial_no=Serial_no.to_frame()
# Serial_no=Serial_no.drop_duplicates()


# In[267]:


# def read_ekko(table,y):
#     conn = prestodb.dbapi.connect(
#     host='presto.dbc.ludp.lenovo.com',
#     port= 30060,
#     user= 'p413_g2861',
#     catalog = 'hive',
#     http_scheme='https',
#     auth=prestodb.auth.BasicAuthentication("p413_g2861","gVer-5217"),)
#     conn._http_session.verify= "presto.cer"
#     cur = conn.cursor()
#     query="select zsernum,bedat from "+table+" where zsernum in "+y
#     cur.execute(query)
#     rows = cur.fetchall()
#     cols=cur.description
#     return rows,cols


# In[268]:


# #Fetching purchace date for the unique serial numbers

# b= 0
# c= 30000
# d=(len(Serial_no))
# ekko=pd.DataFrame()
# for i in range(int(d/30000)+1):
#     data_2=Serial_no["Serial No"][b:c].tolist()
#     data_3="','".join(data_2)
#     data_4="('"+data_3+"')"
#     def data_processing_a():
#         rows,cols=read_ekko("`prd_updated`.`ecc_ekko_rt_udl`",data_4)
#         column = [col[0] for col in cols]
#         data_df_d=pd.DataFrame(rows,columns=column)
#         return data_df_d
#     data_df_d= data_processing_a()
#     ekko=ekko.append(data_df_d)
#     b=c
#     c=c+30000


# In[269]:


# print(ekko.shape)
# ekko.rename(columns={"zsernum":"Serial No","bedat":"Purchasing Date"},inplace=True)


# In[270]:


# ekko=ekko.drop_duplicates()
# ekko.shape
# ekko.to_excel("EKKO_DOA.xlsx")


# In[271]:


final_op.shape


# In[272]:


# grp_ekko=ekko.groupby("Serial No").agg(Max_purchasing_date)


# In[273]:


# final_op_1=final_op.merge(ekko,on="Serial No",how="left")
# final_op_1.shape


# In[274]:


final_op.columns.values


# In[275]:


# final_op['Diff of Days between DOA & SO']=final_op['Diff of Days between DOA & SO'].fillna(0)
# final_op['Diff of Days between DOA & SO']=final_op['Diff of Days between DOA & SO'].astype(float)
# final_op['No of Days Between 1st Return SI & DOA Certificate Date']=final_op['No of Days Between 1st Return SI & DOA Certificate Date'].fillna(0)
# final_op['No of Days Between 1st Return SI & DOA Certificate Date']=final_op['No of Days Between 1st Return SI & DOA Certificate Date'].astype(float)

vbrp_data_drp_dup["Billed_Qty"]=vbrp_data_drp_dup["Billed_Qty"].astype(float)
vbrp_data_drp_dup["Entry Date"]=pd.to_datetime(vbrp_data_drp_dup["Entry Date"],yearfirst=True)
vbrp_data_drp_dup["Entry Time"]=pd.to_datetime(vbrp_data_drp_dup["Entry Time"], format='%H%M%S').dt.strftime('%H:%M:%S')


# In[276]:


vbrp_data_drp_dup["Entry Time"].astype(str)


# In[277]:


# with pd.ExcelWriter('DOA_analysis_final.xlsx') as writer:
#     final_op.to_excel(writer,sheet_name="DOA",index=False)
#     vbrp_data_drp_dup.to_excel(writer,sheet_name="VBRP",index=False)


# In[278]:


sodata1.shape


# In[ ]:





# In[279]:


sodata=pd.concat([sodata1,sodata2])


# In[280]:


sodata=sodata.drop_duplicates()


# In[281]:


sodata_final=sodata[sodata["SERIAL_NO"]!=""]


# In[282]:


sodata_final.shape


# In[283]:


# sodata.to_excel("SO data for doa.xlsx")


# In[268]:


vbrp_data_drp_dup.to_excel('/mnt/data/Codes Final/Outputs/Dead on Arrival Analytics/VBRP.xlsx')
sodata_final.to_excel('/mnt/data/Codes Final/Outputs/Dead on Arrival Analytics/SO Data july.xlsx')


# In[284]:


with pd.ExcelWriter('/mnt/data/Codes Final/Outputs/Dead on Arrival Analytics/DOA_analysis_fina_Feb_2023.xlsx') as writer:
    final_op.to_excel(writer,sheet_name="DOA",index=False)
    vbrp_data_drp_dup.to_excel(writer,sheet_name="VBRP",index=False)
    sodata_final.to_excel(writer,sheet_name="SO",index=False)


# In[285]:


# final_op['T3_CUSTOMER_NAME_SO']=final_op['T3_CUSTOMER_NAME_SO']+'1'


# In[286]:


final_op=final_op.replace(',','_',regex=True)
final_op=final_op.replace('~','|',regex=True)
final_op["SO #"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in final_op["SO #"]]
final_op["T3_CUSTOMER_NAME_SO"]=[re.sub("[^a-zA-Z0-9]","",str(x)) for x in final_op["T3_CUSTOMER_NAME_SO"]]


# In[287]:


final_op.columns=final_op.columns.str.lower()
final_op=final_op.replace(r'\n','',regex=True)
final_op.to_csv("/mnt/data/Codes Final/Dead on Arrival Analytics/DOA_analysis_final.csv",header=False,sep='~',line_terminator="\n",index=False)


# In[288]:


from pyhiveConn import hiveConnector
import sqlalchemy
#from sqlalchemy.types import StringType
import pandas as pd
from pyspark.sql.types import *
from pandas import DataFrame
import time
connection = hiveConnector.connection("p413_g2861", "gVer-5217", "ecc_db_bc", "", "10.122.33.81:2181,10.122.33.82:2181", "/ludp_hive_ha")

cursor = connection.cursor()

cursor.execute("Load2LUDP '/mnt/data/Codes Final/Dead on Arrival Analytics/DOA_analysis_final.csv' Overwrite into table `ecc_db_bc`.`dead_on_arrival`")


# In[ ]:





# In[ ]:




