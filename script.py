import pandas as pd
import numpy as np


def get_min_memory_type(colobj, verbose=0):
    orig_type = colobj.dtype
    new_types = ["uint8", "int8", "uint16", "int16", 'float16', "uint32", "int32", 'float32']
    if (orig_type not in new_types and orig_type not in ["int64", "uint64", "float64"]):
        if (verbose > 0):
            print("Type {} of column {} is unsupported".format(orig_type, colobj.name))
        return orig_type

    for new_type in new_types:
        yes_min = colobj.min() == colobj.min().astype(new_type).astype(orig_type)
        yes_max = colobj.max() == colobj.max().astype(new_type).astype(orig_type)
        if (yes_min and yes_max):
            return new_type

    if (verbose > 0):
        print("No optimisation found for {} column of {} type".format(colobj.name, orig_type))
    return orig_type


def reduce_mem_usage(df, exclude=[], verbose=0):
    start_mem_usg = df.memory_usage().sum() / 1024 ** 2
    if (verbose > 0):
        print("Memory usage before :", start_mem_usg, " MB")

    for colname in df.columns:
        if (colname not in exclude):
            colobj = getattr(df, colname)
            new_type = get_min_memory_type(colobj, verbose)
            if (new_type != colobj.dtype):
                if (verbose > 0):
                    print("Converting {} column from {} to {}".format(colname, colobj.dtype, new_type))
                df[colname] = df[colname].astype(new_type)

    end_mem_usg = df.memory_usage().sum() / 1024 ** 2

    if (verbose > 0):
        print("Memory usage after :", end_mem_usg, " MB")

    mem_usg_diff_prc = float((end_mem_usg - start_mem_usg)) * 100 / start_mem_usg

    if (verbose > 0):
        if (end_mem_usg < start_mem_usg):
            print("Memory gain: {0:0.2f}%".format(-mem_usg_diff_prc))
        else:
            print("Memory loss: {0:0.2f}%".format(mem_usg_diff_prc))

    return -mem_usg_diff_prc

def fillna_mean(df, cols):
    for col in cols:
        mean_values = df[[col]].mean(axis=0)
        print(col, mean_values[col])
        df[col].fillna(mean_values[col], inplace=True)



props_2016 = pd.read_csv('data/properties_2016.csv', index_col='parcelid')
print ("One")
# props_2017 = pd.read_csv('data/properties_2017.csv', index_col='parcelid')
# print("TWO")
# train_2016 = pd.read_csv('data/train_2016_v2.csv', index_col='parcelid')
# print("three")
# train_2017 = pd.read_csv('data/train_2017.csv', index_col='parcelid')
# print("three")
fillna_mean(props_2016, ['airconditioningtypeid',
'architecturalstyletypeid',
'basementsqft',
'bathroomcnt',
'bedroomcnt',
'buildingqualitytypeid',
'buildingclasstypeid',
'calculatedbathnbr',
'decktypeid',
'threequarterbathnbr',
'finishedfloor1squarefeet',
'calculatedfinishedsquarefeet',
'finishedsquarefeet6',
'finishedsquarefeet12',
'finishedsquarefeet13',
'finishedsquarefeet15',
'finishedsquarefeet50',
'fireplacecnt',
'fireplaceflag',
'fullbathcnt',
'garagecarcnt',
'garagetotalsqft',
'hashottuborspa',
'heatingorsystemtypeid',
'lotsizesquarefeet',
'numberofstories',
'poolcnt',
'poolsizesum',
'pooltypeid10',
'pooltypeid2',
'pooltypeid7',
'regionidcounty',
'regionidcity',
'regionidzip',
'regionidneighborhood',
'roomcnt',
'storytypeid',
'typeconstructiontypeid',
'unitcnt',
'yardbuildingsqft17',
'yardbuildingsqft26',
'yearbuilt',
'taxvaluedollarcnt',
'structuretaxvaluedollarcnt',
'landtaxvaluedollarcnt',
'taxamount',
'assessmentyear'
])
# print("three")
# fillna_mean(props_2017, list(props_2017))
# print("three")
# fillna_mean(train_2016, list(train_2016))
# print("three")
# fillna_mean(train_2017, list(train_2017))
# print("four")
print("...")
reduce_mem_usage(props_2016[['airconditioningtypeid',
'architecturalstyletypeid',
'basementsqft',
'bathroomcnt',
'bedroomcnt',
'buildingqualitytypeid',
'buildingclasstypeid',
'calculatedbathnbr',
'decktypeid',
'threequarterbathnbr',
'finishedfloor1squarefeet',
'calculatedfinishedsquarefeet',
'finishedsquarefeet6',
'finishedsquarefeet12',
'finishedsquarefeet13',
'finishedsquarefeet15',
'finishedsquarefeet50',
'fireplacecnt',
'fireplaceflag',
'fullbathcnt',
'garagecarcnt',
'garagetotalsqft',
'hashottuborspa',
'heatingorsystemtypeid',
'lotsizesquarefeet',
'numberofstories',
'poolcnt',
'poolsizesum',
'pooltypeid10',
'pooltypeid2',
'pooltypeid7',
'regionidcounty',
'regionidcity',
'regionidzip',
'regionidneighborhood',
'roomcnt',
'storytypeid',
'typeconstructiontypeid',
'unitcnt',
'yardbuildingsqft17',
'yardbuildingsqft26',
'yearbuilt',
'taxvaluedollarcnt',
'structuretaxvaluedollarcnt',
'landtaxvaluedollarcnt',
'taxamount',
'assessmentyear'
]], [], 1)
print("five")
# reduce_mem_usage(props_2017, [], 1)
# print("six")
# reduce_mem_usage(train_2016,[],1)
# print("seven")
# reduce_mem_usage(train_2017, [], 1)

props_2016.to_csv('new_props.csv')
print("dslfkjs")
new_props = pd.read_csv('new_props.csv', index_col='parcelid')
print("abc")
reduce_mem_usage(new_props,[],1)