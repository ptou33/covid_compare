import requests
import os
from datetime import datetime, timedelta
import csv
import collections
import sqlite3
import pandas
import pickle
from datetime import date, timedelta
import copy
import shutil
import os
import glob
import numpy as np

home="/home/pi/"
cache_build_folder = home + "CACHE_BUILD/"
R0_REALTIME = False


# -----------------------------------------------------------------------------------------
def myint(n):
    return int(float(n))

def cache_needs_new_download(file_name, hours=1):
    """ check if cache exists or it is older than 1 hour """
    one_hour_ago = datetime.now() - timedelta(hours=hours)
    cache_exists = False
    filetime = datetime.now()
    if os.path.exists(file_name):
        cache_exists = True
        filetime = datetime.fromtimestamp(os.path.getctime(file_name))
    rebuild_cache = False
    if filetime < one_hour_ago or not cache_exists:
        rebuild_cache = True
    return rebuild_cache

def get_italia_from_git(file_name):
    # url = 'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province.csv'
    url = 'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-regioni/dpc-covid19-ita-regioni.csv'
    covid_data = requests.get(url, allow_redirects=False)
    covid_data = covid_data.text
    with open(file_name,'w') as csvfile:
        csvfile.write(covid_data)

def missing_days(sdate, edate):
    # create italy missing days from 22/1/2020 to 24/2/2020
    missing_days = []
    delta = edate - sdate       # as timedelta
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        missing_days.append(f'"{day.isoformat()}"')
    return missing_days

def build_and_save_italia():
    """ build Confirmed_italia_regioni_cache.csv Deaths_italia_regioni_cache.csv Recovered_italia_regioni_cache.csv"""
    cache_dir = cache_build_folder
    cache_file = "italia_regioni_cache.csv"

    get_italia_from_git(cache_dir+cache_file)

    df = pandas.read_csv(cache_dir+cache_file)
    with sqlite3.connect(":memory:") as con:
        df.to_sql("italia_regioni", con, if_exists='replace', index=False)
        # cur = con.execute("select * from italia_regioni limit 1")
        # col_names = [f'"{d[0]}"' for d in cur.description]
        # print(col_names)

        stat_types = {
            "Confirmed" : "totale_casi", 
            "Deaths"    : "deceduti", 
            "Recovered" : "dimessi_guariti"
        }
        
        cur = con.execute(f"""select distinct(data) as data from italia_regioni order by 1""")
        days = [f'"{row[0]}"' for row in cur.fetchall()]
        
        sdate = date(2020, 1, 22)   # start missing italy dates
        edate = date(2020, 2, 23)   # end missing italy dates
        days = missing_days( sdate, edate ) + days

        for stat, col_name in stat_types.items():
            # https://stackoverflow.com/questions/52961250/how-to-transpose-a-table-in-sqlite
            # GROUP_CONCAT(case when data="2020-02-28 18:00:00" then totale_attualmente_positivi end) as "2020-02-28"
            row_to_cols = [
                # f""" ifnull(GROUP_CONCAT(case when data={d} then {col_name} end, ';'), 0 ) as "{d[1:11]}" """ 
                f""" ifnull( MAX(case when data={d} then {col_name} end) , 0 ) as "{d[1:11]}" """ 
                for d in days]
            sql = f"""
                select
                        "denominazione_regione" as "Province/State",
                        'Italy' as "Country/Region",
                        Lat,
                        Long,
                        {', '.join(row_to_cols)}
                    from italia_regioni
                    group by 1 """
            df = pandas.read_sql( sql, con )
            stat_file = stat + "_" + cache_file
            df.to_csv(cache_dir+stat_file, index=False)
            print("generated: "+stat_file)

def load_and_fix_hopkins():
    df_dict =  {}

    for stat_type in ["Confirmed", "Deaths", "Recovered"]:
        # url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
        url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_'+stat_type.lower()+'_global.csv'
        df = pandas.read_csv(url)
        df_dict[stat_type] = df

    # fixing different rows from Confirmed and Recovered
    df_dict["Recovered"] = pandas.merge_ordered(df_dict["Recovered"], df_dict["Confirmed"][['Province/State','Country/Region']], 
                                                how="outer", suffixes=(None, None), 
                                                left_on=["Province/State", "Country/Region"], 
                                                right_on=["Province/State","Country/Region"])
    
    # fill nan with 0 except on column Province/State
    cols_to_fill = [i for i in df_dict["Recovered"].columns if i != "Province/State"]
    print("cols_to_fill: ",cols_to_fill)
    df_dict["Recovered"][cols_to_fill] = df_dict["Recovered"][cols_to_fill].fillna(0)

    # fixing different columns from Confirmed and Recovered
    n_col_confirmed = len(df_dict["Confirmed"].columns)
    n_col_recovered = len(df_dict["Recovered"].columns)
    if n_col_recovered < n_col_confirmed:
        print("Reshaping adding one more column to Recovered:",n_col_confirmed-n_col_recovered)
        df_dict["Recovered"][df_dict["Confirmed"].columns[-1]] = None
    return df_dict

def load_hopkins_US(hopkins_df_dict):
    df_dict =  {}
    for stat_type in ["Confirmed", "Deaths"]:
        # url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
        url = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_'
                + stat_type.lower() + '_US.csv' )
        df = pandas.read_csv(url)
        df_dict[stat_type] = df
    return df_dict

def build_merge_and_save_hopkins():

    # df_dict = import_from_daily()
    hopkins_df_dict = load_and_fix_hopkins()
    us_df_dict = load_hopkins_US(hopkins_df_dict)


    for stat_type in ["Confirmed", "Deaths", "Recovered"]:
        df = hopkins_df_dict[stat_type]
        print(df)
        df_italy = pandas.read_csv(cache_build_folder + stat_type + "_italia_regioni_cache.csv")
        print(df_italy)

        with sqlite3.connect(":memory:") as con:
        # with sqlite3.connect(cache_dir + stat_type +"_db_cache.sqlite") as con:
            def merge_hopkins_italy():
                """ merge hopkins with italy protezione civile, fix last date discrepancy """
                n_col_hopkins = len(df.columns)
                n_col_italy = len(df_italy.columns)
                print("n_col_hopkins", n_col_hopkins, "n_col_italy", n_col_italy)
                if n_col_italy != n_col_hopkins:
                    print("hopkins-italy reshaping dbs: ",n_col_italy," ",n_col_hopkins," ",df_italy.columns[-1]," ",df.columns[-1])
                    if n_col_italy > n_col_hopkins:
                        df[df_italy.columns[-1]] = None
                    else:
                        df_italy[df.columns[-1]] = None
                df.to_sql("hopkins", con, if_exists='replace', index=False)
                df_italy.to_sql("italy", con, if_exists='replace', index=False)

                con.execute(f"""delete from hopkins where "Country/Region" like 'Italy' """)

                con.execute(f"""
                    create table hopkins_italy as
                        select * from hopkins
                        union all select * from italy """)
            merge_hopkins_italy()

            def aggregate_US_counties_in_states():
                """ county data in hopkins seems now replicated in the states. Delete the counties """                                
                where_comma_exists_in_province = """where "Country/Region" like 'US' and  "Province/State" like '%, %' """ 
                con.execute(f"delete from hopkins_italy {where_comma_exists_in_province} ")
            # aggregate_US_counties_in_states()

            def aggregates_provinces_of_countries():
                """ create aggreates directly in the saved csv file """
                cur = con.execute("""
                    select "Country/Region" from hopkins_italy 
                        group by 1 
                        having count(*) > 1                                          
                            and "Country/Region" not in 
                            (select "Country/Region" from hopkins_italy where "Province/State" is null) """)
                countries = [f'"{r[0]}"' for r in cur.fetchall()]  # countries = ['"China"', '"US"', '"Australia"', '"France"', '"Italy"']

                cur = con.execute("select * from hopkins_italy limit 1")
                col_names = [f'"{d[0]}"' for d in cur.description]
                sum_col_names = [f"sum({c}) as {c}" for c in col_names]

                con.execute(f"""
                    create table province_aggregate as
                        select 
                                null as "Province/State",
                                "Country/Region",
                                avg(Lat) as Lat,
                                avg(Long) as Long,
                                {', '.join(sum_col_names[4:])} 
                            from hopkins_italy
                            where "Country/Region" in
                                ({', '.join(countries)})
                            group by 2 """)
                con.execute(f"""
                    create table world_aggregate as
                        select 
                                null as "Province/State",
                                'World' as "Country/Region",
                                avg(Lat) as Lat,
                                avg(Long) as Long,
                                {', '.join(sum_col_names[4:])} 
                            from hopkins
                            where "Country/Region" not like 'Italy _Hopkins_'
                            group by 2 """)
            aggregates_provinces_of_countries()

            def merge_aggregates_and_save_csv():
                tables = ["hopkins_italy", "province_aggregate", "world_aggregate"]
                select_tables = [f"select * from {t}" for t in tables]
                df2 = pandas.read_sql(' union all '.join( select_tables ), con )
                cache_file = cache_build_folder + stat_type + '_timeserie_cache.csv'
                df2.to_csv(cache_file, index=False)
            merge_aggregates_and_save_csv()

def build_basic_trends():

    trends = dict()

    for stat_type in ["Confirmed", "Deaths", "Recovered"]:
        cache_file = cache_build_folder+stat_type+'_timeserie_cache.csv'
        with open(cache_file,'r') as csvfile:
            reader = csv.DictReader(csvfile)
            trends[stat_type] = dict()
            for row in reader:
                ps = ""
                if row["Province/State"] != "":
                    ps = "-"+row["Province/State"] 
                k = row["Country/Region"]+ps

                v = dict()
                v["Country/Region"] = row["Country/Region"]
                v["Province/State"] = row["Province/State"]
                v["Lat"]            = row["Lat"]
                v["Long"]           = row["Long"]
                v["Numbers"]      = list()
                for kk, vv in row.items():
                    if kk not in ["Country/Region", "Province/State", "Lat", "Long"]:
                        if vv == "" or vv is None:
                            pass
                        else:
                            v["Numbers"].append(vv)

                trends[stat_type][k] = v

    # build currently infected numbers
    trends["Active"] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()
        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]
        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]      = list()
        for nc, nd, nr in zip(tc["Numbers"], td["Numbers"], tr["Numbers"]):
            try:
                v["Numbers"].append(myint(nc)-myint(nd)-myint(nr))
            except:
                #v["Numbers"].append(0)
                print("Active number error:", k, nc, nd, nr)
        trends["Active"][k] = v
    return trends

def compute_advanced_trends(trends):
    def create_daily():
        """build daily normalized increments"""
        confirmed_daily = "Confirmed daily"
        death_daily     = "Deaths daily"
        recovered_daily = "Recovered daily"
        active_daily    = "Active daily"
        
        trends[confirmed_daily] = dict()
        trends[death_daily] = dict()
        trends[recovered_daily] = dict()
        trends[active_daily] = dict()
        
        for k,d in trends["Confirmed"].items():
            v1 = dict()
            v2 = dict()
            v3 = dict()
            v4 = dict()

            tc = trends["Confirmed"][k]
            td = trends["Deaths"][k]
            tr = trends["Recovered"][k]
            ta = trends["Active"][k]

            v1["Country/Region"] = tc["Country/Region"]
            v1["Province/State"] = tc["Province/State"]
            v1["Lat"]            = tc["Lat"]
            v1["Long"]           = tc["Long"]
            v1["Numbers"]       = list()
            v2 = copy.deepcopy(v1)
            v3 = copy.deepcopy(v1)
            v4 = copy.deepcopy(v1)

            for i_date in range(len(tc["Numbers"])):
                if i_date == 0:
                    v1["Numbers"].append(0)
                    v2["Numbers"].append(0)
                    v3["Numbers"].append(0)
                    v4["Numbers"].append(0)
                else:
                    try:
                        nc     = myint(tc["Numbers"][i_date]  ) 
                        nc_old = myint(tc["Numbers"][i_date-1]) 
                        v1["Numbers"].append(nc-nc_old)
                    except:
                        print("Daily metrics failing number error 1:", k, i_date)

                    try:
                        nd     = myint(td["Numbers"][i_date]   )
                        nd_old = myint(td["Numbers"][i_date-1] )
                        v2["Numbers"].append(nd-nd_old)
                    except:
                        print("Daily metrics failing number error 2:", k, i_date)

                    try:
                        nr     = myint(tr["Numbers"][i_date]  )
                        nr_old = myint(tr["Numbers"][i_date-1])
                        v3["Numbers"].append(nr-nr_old)
                    except:
                        print("Daily metrics failing number error 3:", k, i_date)

                    try:
                        na     = myint(ta["Numbers"][i_date]   )
                        na_old = myint(ta["Numbers"][i_date-1] )
                        v4["Numbers"].append(na-na_old)
                    except:
                        print("Daily metrics failing number error 4:", k, i_date)
            trends[confirmed_daily][k] = v1
            trends[death_daily][k]     = v2
            trends[recovered_daily][k] = v3
            trends[active_daily][k]    = v4
    create_daily()

    def create_r0(end_infect, start_infect=0):
        # compute r0 assuming infected person is infectious from day=start_infect to day=end_infect
        if start_infect == 0:
            r0 = f"R0 estimate{end_infect}"
        else:
            r0 = f"R0 estimate{start_infect}-{end_infect}"
        trends[r0] = {}
        keys_to_copy = [
            "Country/Region",
            "Province/State",
            "Lat",
            "Long" ]
        for country, country_data in trends["Confirmed daily"].items():
            v = { key : country_data[key] for key in keys_to_copy }
            v["Numbers"] = []
            values = country_data["Numbers"]
            for i, value in enumerate(values):
                first_index = max(i-end_infect,0)
                last_index = max(i-start_infect,0)
                infectious_ndays = values[first_index:last_index+1]  # average including today
                try:
                    r0_value = value / (sum(infectious_ndays)/len(infectious_ndays))
                except:
                    r0_value = 0
                v["Numbers"].append(r0_value)
            trends[r0][country] = v
    create_r0(end_infect = 7)
    create_r0(end_infect = 25)
    create_r0(end_infect = 18, start_infect=4)

    for end_infect, start_infect in zip([7,25,18],[0,0,4]):
        # build daily normalized increments smoothed
        if start_infect == 0:
            r0 = f"R0 estimate{end_infect}"
        else:
            r0 = f"R0 estimate{start_infect}-{end_infect}"
        r0s = r0+" smoothed"
        trends[r0s] = dict()
        n_smooth = 7
        for k,d in trends[r0].items():
            v = dict()

            ta = trends[r0][k]

            v["Country/Region"] = ta["Country/Region"]
            v["Province/State"] = ta["Province/State"]
            v["Lat"]            = ta["Lat"]
            v["Long"]           = ta["Long"]
            v["Numbers"]        = list()

            n_numbers = len(ta["Numbers"])
            for i_date in range(n_numbers):
                max_possible_smoothing = min(n_smooth, i_date, n_numbers-1-i_date)
                try:
                    r0_smooth = 0.
                    for i_smooth in range(-max_possible_smoothing, max_possible_smoothing+1):
                        na = ta["Numbers"][i_date+i_smooth]  
                        r0_smooth += na/float(2*max_possible_smoothing+1)
                    v["Numbers"].append(r0_smooth)
                except:
                    #v["Numbers"].append(0)
                    print("R0 smoothed number error:", k, i_date)
            trends[r0s][k] = v
    
    # build daily normalized increments
    a_v = "Active variation %"
    trends[a_v] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            if i_date == 0:
                v["Numbers"].append(0)
            else:
                try:
                    nc = tc["Numbers"][i_date]   ; nd = td["Numbers"][i_date]   ; nr = tr["Numbers"][i_date]
                    active_day = (myint(nc)-myint(nd)-myint(nr))
                    nc = tc["Numbers"][i_date-1] ; nd = td["Numbers"][i_date-1] ; nr = tr["Numbers"][i_date-1]
                    active_previous_day = (myint(nc)-myint(nd)-myint(nr))

                    if active_day == 0 and active_previous_day == 0:
                        v["Numbers"].append(0.)
                    else:
                        active_perc = 100.*(float(active_day)-float(active_previous_day))/(0.5*(float(active_day)+float(active_previous_day)))
                        v["Numbers"].append(active_perc)
                except:
                    #v["Numbers"].append(0)
                    print("Active variation (%) number error:", k, i_date)
        trends[a_v][k] = v

    # build daily normalized increments smoothed
    a_v_s = "Active variation smoothed 3 days %"
    trends[a_v_s] = dict()
    n_smooth = 3
    for k,d in trends[a_v].items():
        v = dict()

        ta = trends[a_v][k]

        v["Country/Region"] = ta["Country/Region"]
        v["Province/State"] = ta["Province/State"]
        v["Lat"]            = ta["Lat"]
        v["Long"]           = ta["Long"]
        v["Numbers"]        = list()

        n_numbers = len(ta["Numbers"])
        for i_date in range(n_numbers):
            max_possible_smoothing = min(n_smooth, i_date, n_numbers-1-i_date)
            try:
                active_smooth = 0.
                for i_smooth in range(-max_possible_smoothing, max_possible_smoothing+1):
                    na = ta["Numbers"][i_date+i_smooth]  
                    active_smooth += na/float(2*max_possible_smoothing+1)
                v["Numbers"].append(active_smooth)
            except:
                #v["Numbers"].append(0)
                print("Active variation smoothed (%) number error:", k, i_date)
        trends[a_v_s][k] = v

    # build smoothed death daily
    a_v_ds = "Death daily smoothed 7 days"
    trends[a_v_ds] = dict()
    n_smooth = 7
    for k,d in trends["Deaths daily"].items():
        v = dict()

        ta = trends["Deaths daily"][k]

        v["Country/Region"] = ta["Country/Region"]
        v["Province/State"] = ta["Province/State"]
        v["Lat"]            = ta["Lat"]
        v["Long"]           = ta["Long"]
        v["Numbers"]        = list()

        n_numbers = len(ta["Numbers"])
        for i_date in range(n_numbers):
            max_possible_smoothing = min(n_smooth, i_date, n_numbers-1-i_date)
            try:
                active_smooth = 0.
                for i_smooth in range(-max_possible_smoothing, max_possible_smoothing+1):
                    na = ta["Numbers"][i_date+i_smooth]  
                    active_smooth += na/float(2*max_possible_smoothing+1)
                v["Numbers"].append(active_smooth)
            except:
                #v["Numbers"].append(0)
                print("Death daily smoothed  number error:", k, i_date)
        trends[a_v_ds][k] = v

    # build smoothed confirmed daily
    a_v_cs = "Confirmed daily smoothed 7 days"
    trends[a_v_cs] = dict()
    n_smooth = 7
    for k,d in trends["Confirmed daily"].items():
        v = dict()

        ta = trends["Confirmed daily"][k]

        v["Country/Region"] = ta["Country/Region"]
        v["Province/State"] = ta["Province/State"]
        v["Lat"]            = ta["Lat"]
        v["Long"]           = ta["Long"]
        v["Numbers"]        = list()

        n_numbers = len(ta["Numbers"])
        for i_date in range(n_numbers):
            max_possible_smoothing = min(n_smooth, i_date, n_numbers-1-i_date)
            try:
                active_smooth = 0.
                for i_smooth in range(-max_possible_smoothing, max_possible_smoothing+1):
                    na = ta["Numbers"][i_date+i_smooth]  
                    active_smooth += na/float(2*max_possible_smoothing+1)
                v["Numbers"].append(active_smooth)
            except:
                #v["Numbers"].append(0)
                print("Confirmed daily smoothed:", k, i_date)
        trends[a_v_cs][k] = v

    # build Fatality (CFR)
    fatality1 = "Fatality-1 %"
    trends[fatality1] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            try:
                nc = tc["Numbers"][i_date] 
                nd = td["Numbers"][i_date]  
                nr = tr["Numbers"][i_date]
                if myint(nc) == 0:
                    fatality = 0
                else:
                    fatality = (myint(nd)/myint(nc))*100
                v["Numbers"].append(fatality)
            except:
                print("Error Fatality 1 k, i_date: ",k, i_date)
                fatality = 0
        trends[fatality1][k] = v

    # build Fatality (CFR) https://academic.oup.com/aje/article/162/5/479/82647
    fatality2 = "Fatality-2 %"
    trends[fatality2] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            try:
                nc = tc["Numbers"][i_date]   ; nd = td["Numbers"][i_date]   ; nr = tr["Numbers"][i_date]
                if myint(nd) + myint(nr) == 0:
                    fatality = 0
                else:
                    fatality = (myint(nd)/(myint(nd)+myint(nr)))*100
                v["Numbers"].append(fatality)
            except:
                fatality = 0
        trends[fatality2][k] = v


    # build Fatality Change according to Timo suggestions
    f_c_1 = "Fatality change-1 %"
    n_days = 5
    trends[f_c_1] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            if i_date < n_days:
                fatality = 0
                v["Numbers"].append(fatality)   
            else:
                i_old = i_date - n_days
                try:
                    nc_old = tc["Numbers"][i_old]  ; nd_old = td["Numbers"][i_old]  ; nr_old = tr["Numbers"][i_old]
                    nc     = tc["Numbers"][i_date] ; nd     = td["Numbers"][i_date] ; nr     = tr["Numbers"][i_date]
                    if myint(nd) - myint(nd_old) == 0:
                        fatality = 0
                    else:
                        #TIMO fatality = (int(nd)-int(nd_old))/(int(nc)-int(nc_old)) * 100
                        if myint(nr)-myint(nr_old) == 0:
                            fatality = 100
                        else:
                            fatality = (myint(nd)-myint(nd_old))/(myint(nr)-myint(nr_old)) * 100
                    v["Numbers"].append(fatality)   
                except:
                    print("Fatality change error: ",k,i_date)
                    #fatality = 0
        trends[f_c_1][k] = v


    # build Fatality Change D/D+R
    f_c_2 = "Fatality change-2 %"
    n_days = 5
    trends[f_c_2] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            if i_date < n_days:
                fatality = 0
                v["Numbers"].append(fatality)   
            else:
                i_old = i_date - n_days
                try:
                    nc_old = tc["Numbers"][i_old]  ; nd_old = td["Numbers"][i_old]  ; nr_old = tr["Numbers"][i_old]
                    nc     = tc["Numbers"][i_date] ; nd     = td["Numbers"][i_date] ; nr     = tr["Numbers"][i_date]
                    if myint(nd) - myint(nd_old) == 0:
                        fatality = 0
                    else:
                        #TIMO fatality = (int(nd)-int(nd_old))/(int(nc)-int(nc_old)) * 100
                        if myint(nr)-myint(nr_old) == 0:
                            fatality = 100
                        else:
                            fatality = (
                                (myint(nd)-myint(nd_old)) /
                                ( 
                                    (myint(nd)-myint(nd_old)) + (myint(nr)-myint(nr_old)) 
                                ) * 100
                            )
                    v["Numbers"].append(fatality)   
                except:
                    print("Fatality change error: ",k,i_date)
                    #fatality = 0
        trends[f_c_2][k] = v

    # build Fatality Change D/D+R
    risk_ind = "Risk"
    n_days = 5
    trends[risk_ind] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]
        ta = trends["Active"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            if i_date < n_days:
                risk = 0
            else:
                i_old = i_date - n_days
                try:
                    nc_old = tc["Numbers"][i_old]  ; nd_old = td["Numbers"][i_old]  
                    nr_old = tr["Numbers"][i_old]  ; na_old = ta["Numbers"][i_old]  ; 
                    nc     = tc["Numbers"][i_date] ; nd     = td["Numbers"][i_date] 
                    nr     = tr["Numbers"][i_date] ; na     = ta["Numbers"][i_date] 
                    death_factor = 0.99
                    active_factor = 0.01
                    risk   =  death_factor*(myint(nd)-myint(nd_old)) + active_factor*(myint(na)-myint(na_old)) 
                except:
                    print("Fatality change error: ",k,i_date)
                    risk = 0
            v["Numbers"].append(risk)   
        trends[risk_ind][k] = v

    # build Fatality 15  D/D+R
    f_c_15 = "Fatality-15 %"
    n_days = 7
    n_offset_days = 15
    trends[f_c_15] = dict()
    for k,d in trends["Confirmed"].items():
        v = dict()

        tc = trends["Confirmed"][k]
        td = trends["Deaths"][k]
        tr = trends["Recovered"][k]

        v["Country/Region"] = tc["Country/Region"]
        v["Province/State"] = tc["Province/State"]
        v["Lat"]            = tc["Lat"]
        v["Long"]           = tc["Long"]
        v["Numbers"]       = list()

        for i_date in range(len(tc["Numbers"])):
            if i_date < n_days + n_offset_days:
                fatality = -1
                v["Numbers"].append(fatality)   
            else:
                i_old        = i_date - n_days
                i_offset     = i_date - n_offset_days
                i_old_offset = i_date - n_days - n_offset_days
                try:
                    nc_old_offset = tc["Numbers"][i_old_offset]  ; nd_old_offset = td["Numbers"][i_old_offset]
                    nc_offset     = tc["Numbers"][i_offset]      ; nd_offset     = td["Numbers"][i_offset] 
                    nc_old        = tc["Numbers"][i_old]         ; nd_old        = td["Numbers"][i_old]    
                    nc            = tc["Numbers"][i_date]        ; nd            = td["Numbers"][i_date]    
                    if myint(nc_offset) - myint(nc_old_offset) == 0:
                        fatality = -2
                    else:
                        #TIMO fatality = (int(nd)-int(nd_old))/(int(nc)-int(nc_old)) * 100
                        if myint(nc_offset)-myint(nc_old_offset) == 0:
                            fatality = -3
                        else:
                            fatality = ( (myint(nd)-myint(nd_old)) / (myint(nc_offset)-myint(nc_old_offset)) ) * 100
                    v["Numbers"].append(fatality)   
                except:
                    print("Fatality change error: ",k,i_date)
                    fatality = -4
        trends[f_c_15][k] = v

def compute_r0_trends(trends):
    from r0_calculator import calculate_r0
    from transpose_date_rows_to_cols import rows_to_cols
    
    cache_build_folder = home + "CACHE_BUILD/"
    confirmed_file = cache_build_folder + "Confirmed_timeserie_cache.csv"
    r0_intermediate_file = cache_build_folder + 'hopkins_r0_cache.csv'
    calculate_r0(input_file_or_url=confirmed_file,
                 save_file=r0_intermediate_file,
                 override_sigma=0.25)
    r0_final_file = cache_build_folder + 'Confirmed_realtime_r0_timeserie_cache.csv'
    rows_to_cols(input_file=r0_intermediate_file, 
                 output_file=r0_final_file, 
                 states_template_file=confirmed_file)

    stat_type = 'R0 realtime'
    with open(r0_final_file,'r') as csvfile:
        reader = csv.DictReader(csvfile)
        trends[stat_type] = dict()
        for row in reader:
            ps = ""
            if row["Province/State"] != "":
                ps = "-"+row["Province/State"] 
            k = row["Country/Region"]+ps

            v = dict()
            v["Country/Region"] = row["Country/Region"]
            v["Province/State"] = row["Province/State"]
            v["Lat"]            = row["Lat"]
            v["Long"]           = row["Long"]
            v["Numbers"]        = list()
            for kk, vv in row.items():
                if kk not in ["Country/Region", "Province/State", "Lat", "Long"]:
                    if vv == "" or vv is None:
                        pass
                    else:
                        v["Numbers"].append(vv)

            trends[stat_type][k] = v

def compute_population_trends(trends):

    manual_numbers = {
    "Taiwan*"                        : 23780000,
    "World"                          : 7700000000,
    "occupied Palestinian territory" : 5052000,
    "Italy-Abruzzo"                  : 1311000,
    "Italy-Basilicata"               : 562000,
    "Italy-Calabria"                 : 1947000,
    "Italy-Campania"                 : 5801000,
    "Italy-Emilia-Romagna"           : 4459000,
    "Italy-Friuli Venezia Giulia"    : 1215000,
    "Italy-Lazio"                    : 5879000,
    "Italy-Liguria"                  : 1550000,
    "Italy-Lombardia"                : 10060000,
    "Italy-Marche"                   : 1525000,
    "Italy-Molise"                   : 305000,
    "Italy-P.A. Bolzano"             : 510000,
    "Italy-P.A. Trento"              : 510000,
    "Italy-Piemonte"                 : 4356000,
    "Italy-Puglia"                   : 4029000,
    "Italy-Sardegna"                 : 1639000,
    "Italy-Sicilia"                  : 4999000,
    "Italy-Toscana"                  : 3729000,
    "Italy-Umbria"                   : 882000,
    "Italy-Valle d'Aosta"            : 125000,
    "Italy-Veneto"                   : 4905000,

    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States_by_population
    "US-California"        : 39512223 ,
    "US-Texas"             : 28995881 ,
    "US-Florida"           : 21477737 ,
    "US-New York"          : 19453561 ,
    "US-Pennsylvania"      : 12801989 ,
    "US-Illinois"          : 12671821 ,
    "US-Ohio"              : 11689100 ,
    "US-Georgia"           : 10617423 ,
    "US-North Carolina"    : 10488084 ,
    "US-Michigan"          : 9986857 ,
    "US-New Jersey"        : 8882190 ,
    "US-Virginia"          : 8535519 ,
    "US-Washington"        : 7614893 ,
    "US-Arizona"           : 7278717 ,
    "US-Massachusetts"     : 6949503 ,
    "US-Tennessee"         : 6833174 ,
    "US-Indiana"           : 6732219 ,
    "US-Missouri"          : 6137428 ,
    "US-Maryland"          : 6045680 ,
    "US-Wisconsin"         : 5822434 ,
    "US-Colorado"          : 5758736 ,
    "US-Minnesota"         : 5639632 ,
    "US-South Carolina"    : 5148714 ,
    "US-Alabama"           : 4903185 ,
    "US-Louisiana"         : 4648794 ,
    "US-Kentucky"          : 4467673 ,
    "US-Oregon"            : 4217737 ,
    "US-Oklahoma"          : 3956971 ,
    "US-Connecticut"       : 3565287 ,
    "US-Utah"              : 3205958 ,
    "US-Puerto Rico"       : 3193694 ,
    "US-Iowa"              : 3155070 ,
    "US-Nevada"            : 3080156 ,
    "US-Arkansas"          : 3017825 ,
    "US-Mississippi"       : 2976149 ,
    "US-Kansas"            : 2913314 ,
    "US-New Mexico"        : 2096829 ,
    "US-Nebraska"          : 1934408 ,
    "US-West Virginia"     : 1792065 ,
    "US-Idaho"             : 1787147 ,
    "US-Hawaii"            : 1415872 ,
    "US-New Hampshire"     : 1359711 ,
    "US-Maine"             : 1344212 ,
    "US-Montana"           : 1068778 ,
    "US-Rhode Island"      : 1059361 ,
    "US-Delaware"          : 973764 ,
    "US-South Dakota"      : 884659 ,
    "US-North Dakota"      : 762062 ,
    "US-Alaska"            : 731545 ,
    "US-District of Columbia"      : 705749 ,
    "US-Vermont"                   : 623989 ,
    "US-Wyoming"                   : 578759 ,
    "US-Guam"                      : 165718 ,
    "US-U.S. Virgin Islands"       : 104914 ,
    "US-American Samoa"            : 55641 ,
    "US-Northern Mariana Islands"  : 55194 ,

    "China-Guangdong"   : 111690000   ,
    "China-Shandong"    : 100060000   ,
    "China-Henan"       : 95590000    ,
    "China-Sichuan"     : 83020000    ,
    "China-Jiangsu"     : 80290000    ,
    "China-Hebei"       : 75200000    ,
    "China-Hunan"       : 68600000    ,
    "China-Anhui"       : 62550000    ,
    "China-Hubei"       : 59020000    ,
    "China-Zhejiang"    : 56570000    ,
    "China-Guangxi"     : 48850000    ,
    "China-Yunnan"      : 48010000    ,
    "China-Jiangxi"     : 46220000    ,
    "China-Liaoning"    : 43690000    ,
    "China-Fujian"      : 39110000    ,
    "China-Shaanxi"     : 38350000    ,
    "China-Heilongjiang" : 37890000   ,
    "China-Shanxi"      : 36820000    ,
    "China-Guizhou"     : 35550000    ,
    "China-Chongqing"   : 30750000    ,
    "China-Jilin"       : 27170000    ,
    "China-Gansu"       : 26260000    ,
    "China-Inner Mongolia" : 25290000 ,
    "China-Xinjiang"    : 24450000    ,
    "China-Shanghai"    : 24180000    ,
    "China-Beijing"     : 21710000    ,
    "China-Tianjin"     : 15570000    ,
    "China-Hainan"      : 9170000     ,
    "China-Hong Kong"   : 7335384     ,
    "China-Ningxia"     : 6820000     ,
    "China-Qinghai"     : 5980000     ,
    "China-Tibet"       : 3370000     ,
    "China-Macau"       : 644900      ,

    "Australia-Australian Capital Territory" : 426709,
    #"Australia-From Diamond Princess" : 0,
    "Australia-New South Wales" : 8089526,
    "Australia-Northern Territory" : 245869,
    "Australia-Queensland" : 5095100,
    "Australia-South Australia" : 1751693,
    "Australia-Tasmania" : 534281,
    "Australia-Victoria" : 6594804,
    "Australia-Western Australia" : 2621680,

    "Denmark-Denmark"           : 5800000 ,
    "Denmark-Faroe Islands"     : 51000 ,
    "Holy See"                  : 800,

    'Canada-British Columbia' : 4648055,
    'Italy _Hopkins_' :  60536709,
    'Martinique' : 376480,
    'Canada-Ontario' : 13448494,
    'Canada-Alberta' : 4067175,
    'Canada-Quebec' : 8164361,
    #'US-Diamond Princess' : 1,
    #'US-Grand Princess' : 1,
    'France-France' : 66990000,
    #'Cruise Ship-Diamond Princess' : 1,
    'France-St Martin' : 32715,
    'United Kingdom-Channel Islands' : 170499,
    'Canada-New Brunswick' : 747101,
    'France-Saint Barthelemy' : 9130,
    'United Kingdom-Gibraltar' : 34571  ,
    'United Kingdom-United Kingdom' : 66440000,
    'France-French Polynesia' : 283007 ,
    'Canada-Manitoba' : 1278365,
    'Canada-Saskatchewan' : 1098352,
    #'Canada-Grand Princess' : 1,
    'Saint Lucia' : 178844 ,
    'Saint Vincent and the Grenadines' : 109897 ,
    'France-French Guiana' : 290691,
    'Canada-Newfoundland and Labrador' : 519716,
    'Canada-Prince Edward Island' : 142907,
    'Congo (Brazzaville)' : 5261000,
    'France-Mayotte' : 270372,
    'Netherlands-Netherlands' : 17180000,
    'Canada-Nova Scotia' : 971395 ,
    'France-Guadeloupe' : 395700,
    'Netherlands-Curacao' : 161014 ,
    'The Bahamas' : 395361 ,
    'US-Virgin Islands' : 107268 ,
    'United Kingdom-Cayman Islands' : 61559 ,
    'France-Reunion' : 859959,
    'Kyrgyzstan' : 6202000,
    'Netherlands-Aruba' : 105264 , 
    'United Kingdom-Montserrat' : 5900,
    'Eritrea' : 4475000,
    'Yemen' : 28500000,
    'Western Sahara' : 500000,
    'Syria': 16910000,
    'Burma': 53710000
    }
    
    wb_to_hopkins = {
        "Brunei Darussalam"  : "Brunei",
        "Congo, Dem. Rep."   : "Congo (Kinshasa)",
        "Czech Republic"     : "Czechia",
        "Egypt, Arab Rep."   : "Egypt",
        "Iran, Islamic Rep." : "Iran",
        "Korea, Rep.": "Korea, South",
        "Russian Federation" : "Russia",
        "Slovak Republic"    : "Slovakia",
        "United States"      : "US",
        "Venezuela, RB"      : "Venezuela"
    }

    # build country-population dataframe
    cr_list = []
    for _ , v in trends["Confirmed"].items():
        pr = "-"+v["Province/State"] if v["Province/State"] != "" else ""
        cr_list.append(v["Country/Region"]+pr )
    cr_list = [ [cr, 0] for cr in cr_list ]
    df_states = pandas.DataFrame(cr_list, columns=["name", "pop"])

    # read world-bank population csv
    df_wb = pandas.read_csv(home + "covidtrends/webapp/flask/states_wb.csv")
    
    # fill populations from work-bank or manual list above
    pop_not_found = []
    for i_st, row_st in df_states.iterrows():
        state_name = row_st["name"]
        pop_number = manual_numbers.get(state_name, None)
        if pop_number is not None:
            df_states.at[i_st,'pop'] = pop_number
        else:
            for _ , row_wb in df_wb.iterrows():
                wb_name = row_wb["Country Name"]
                if state_name == wb_to_hopkins.get(wb_name, wb_name):
                    pop_number = row_wb["2017 [YR2017]"]
                    df_states.at[i_st,'pop'] = pop_number
                    break
            else:
                pop_not_found.append(state_name)

    # save list of not found populations by state
    with open(home + "pop_not_found.dat", "w") as f:
        f.write(str(pop_not_found))

    # prepare population dataframe to be accessed as simple dict
    df_as_dict = df_states.set_index('name').T.to_dict('list')

    # normalize all trends according to population
    trends_pop = dict()
    for k,v in trends.items(): # stat_Type
        trends_pop[k] = dict()
        for cr_k, cr_v in v.items(): # country
            trends_pop[k][cr_k] = copy.deepcopy(cr_v)
            population = df_as_dict[cr_k][0]
            for i , _ in enumerate(trends_pop[k][cr_k]["Numbers"]):
                value = float(trends_pop[k][cr_k]["Numbers"][i])
                try:
                    trends_pop[k][cr_k]["Numbers"][i] = value/population
                    pass
                except:
                    trends_pop[k][cr_k]["Numbers"][i] = -1

    return trends_pop, pop_not_found

def build_countryregion_and_days():
    cache_dir = cache_build_folder
    cache_file = cache_dir+"Confirmed_timeserie_cache.csv"
    # use last cache file to get list of Country-Region-Province-State
    with open(cache_file,'r') as csvfile:
        reader = csv.DictReader(csvfile)
        countryregion = []
        for row in reader:
            ps = ""
            if row["Province/State"] != "":
                ps = "-"+row["Province/State"] 
            countryregion.append(row["Country/Region"]+ps)
        days = [k for k,v in row.items() if k not in ["Country/Region", "Province/State","Lat","Long"]]

    countryregion.sort()  # TODO: PT: only countryregion sorted, not days!?!?!
    return countryregion, days

def build_offsets_and_focuspools():
    offsets = {
        "China-Hubei"                   : 0,
        "Korea, South"                  : 33,
        "Iran"                          : 39,
        "France-France"                 : 46,
        "France"                        : 46,
        "United Kingdom"                : 46,
        "Ireland"                       : 46,
        "Portugal"                      : 46,
        "Norway"                        : 46,
        "Finland"                       : 46,
        "Sweden"                        : 46,
        "Finland"                       : 46,
        "Belgium"                       : 46,
        "Israel"                        : 46,
        "Ireland"                       : 46,
        "Greece"                       : 46,
        "Iceland"                       : 46,
        "Denmark"                       : 46,
        "Switzerland"                   : 46,
        "Austria"                       : 46,
        "Romania"                       : 46,
        "Bulgaria"                      : 46,
        "Poland"                        : 46,
        "Czechia"                       : 46,
        "Netherlands"                   : 46,
        "Hungary"                       : 46,
        "Germany"                       : 46,
        "Japan"                         : 32,
        "China-Zhejiang"                : 10,
        "Spain"                         : 46,
        "US"                            : 47,
        "Italy"                         : 37,
        "Italy-Abruzzo"                 : 37,
        "Italy-Basilicata"              : 37,
        "Italy-Calabria"                : 37,
        "Italy-Campania"                : 37,
        "Italy-Emilia-Romagna"          : 37,
        "Italy-Friuli Venezia Giulia"   : 37,
        "Italy-Lazio"                   : 37,
        "Italy-Liguria"                 : 37,
        "Italy-Lombardia"               : 37,
        "Italy-Marche"                  : 37,
        "Italy-Molise"                  : 37,
        "Italy-P.A. Bolzano"            : 37,
        "Italy-P.A. Trento"             : 37,
        "Italy-Piemonte"                : 37,
        "Italy-Puglia"                  : 37,
        "Italy-Sardegna"                : 37,
        "Italy-Sicilia"                 : 37,
        "Italy-Toscana"                 : 37,
        "Italy-Umbria"                  : 37,
        "Italy-Valle d'Aosta"           : 37,
        "Italy-Veneto"                  : 37,
    }

    focuspools = [
        {
            "name": "G20 Countries",
            "cut_item_name": "no",
            "i_day_min": 0,
            # "southWest" : [5.539516, 152.134475],
            # "northEast": [73.652082, -59.979969],
            "southWest" : [-28, -55],
            "northEast": [55, 100],
            "items": [
                #"Italy", "China", "US", "Korea, South", "Iran", "France", "Germany", "Japan", "United Kingdom", "Brazil", "Canada", "Australia",
                #"Nigeria", "India", "Spain", "Turkey", "Tunisia", "Argentina", "Russia"
                "US", "Japan", "Germany", "France", "United Kingdom", "Italy", "Canada", "Russia", "China", "Brazil", "India", "Australia", "Mexico", 
                "Korea, South", "Indonesia", "Turkey", "Saudi Arabia", "Argentina", "South Africa"
            ]
        },
        {
            "name" : "Italian regions",
            "cut_item_name":"yes",
            "i_day_min": 25,
            "southWest" : [35.464691, 5.877990],
            "northEast": [47.758600, 19.437906],
            "items": [
                #"Italy",
                "Italy-Abruzzo", 
                "Italy-Basilicata", 
                "Italy-Calabria", 
                "Italy-Campania", 
                "Italy-Emilia-Romagna",
                "Italy-Friuli Venezia Giulia",
                "Italy-Lazio",
                "Italy-Liguria", 
                "Italy-Lombardia",
                "Italy-Marche", 
                "Italy-Molise", 
                "Italy-P.A. Bolzano",
                "Italy-P.A. Trento",
                "Italy-Piemonte", 
                "Italy-Puglia",
                "Italy-Sardegna", 
                "Italy-Sicilia",
                "Italy-Toscana",
                "Italy-Umbria", 
                "Italy-Valle d'Aosta",
                "Italy-Veneto"
            ]
        },
        #{
        #    "name" : "USA states",
        #    "cut_item_name":"yes",

        #    "southWest" : [18.91619,71.3577635769],
        #    "northEast": [73.652082, -171.791110603],

        #    #"southWest" : [5.539516, 152.134475],
        #    #"northEast": [73.652082, -59.979969],

        #    "i_day_min": 45,
        #    "items": [
        #        "US-Alabama",
        #        "US-Alaska",
        #        "US-Arizona",
        #        "US-Arkansas",
        #        "US-California",
        #        "US-Colorado",
        #        "US-Connecticut",
        #        "US-Delaware",
        #        "US-Florida",
        #        "US-Georgia",
        #        "US-Hawaii",
        #        "US-Idaho",
        #        "US-Illinois",
        #        "US-Indiana",
        #        "US-Iowa",
        #        "US-Kansas",
        #        "US-Kentucky",
        #        "US-Louisiana",
        #        "US-Maine",
        #        "US-Maryland",
        #        "US-Massachusetts",
        #        "US-Michigan",
        #        "US-Minnesota",
        #        "US-Mississippi",
        #        "US-Missouri",
        #        "US-Montana",
        #        "US-Nebraska",
        #        "US-Nevada",
        #        "US-New Hampshire",
        #        "US-New Jersey",
        #        "US-New Mexico",
        #        "US-New York",
        #        "US-North Carolina",
        #        "US-North Dakota",
        #        "US-Ohio",
        #        "US-Oklahoma",
        #        "US-Oregon",
        #        "US-Pennsylvania",
        #        "US-Rhode Island",
        #        "US-South Carolina",
        #        "US-South Dakota",
        #        "US-Tennessee",
        #        "US-Texas",
        #        "US-Utah",
        #        "US-Vermont",
        #        "US-Virginia",
        #        "US-Washington",
        #        "US-West Virginia",
        #        "US-Wisconsin",
        #        "US-Wyoming",
        #    ]
        #},
        {
            "name" : "European states",
            "cut_item_name":"no",
            "southWest" : [5.539516, 152.134475],
            "northEast": [73.652082, -59.979969],
            "i_day_min": 25,
            "items": [
                "Italy",
                "France",
                "Germany",
                "Spain",
                "United Kingdom",
                "Ireland",
                "Portugal",
                "Netherlands",
                "Sweden",
                "Norway",
                "Finland", 
                "Belgium",
                "Israel",
                "Denmark",
                "Iceland",
                "Greece",
                "Switzerland", 
                "Austria",
                "Czechia",
                "Hungary",
                "Romania",
                "Bulgaria",
                "Poland"
            ]
        },
        {
            "name" : "Canada provinces",
            "cut_item_name":"yes",

            "southWest" : [40.91619, 55.3577635769],
            "northEast": [77.652082, -201.791110603],

            #"southWest" : [5.539516, 152.134475],
            #"northEast": [73.652082, -59.979969],

            "i_day_min": 30,
            "items": [
                "Canada-Alberta",
                "Canada-British Columbia",
                "Canada-Manitoba",
                "Canada-New Brunswick",
                "Canada-Newfoundland and Labrador",
                "Canada-Northwest Territories",
                "Canada-Nova Scotia",
                "Canada-Ontario",
                "Canada-Saskatchewan",
                "Canada-Yukon",
                "Canada-Quebec",
                "Canada-Prince Edward Island"
            ]
        },
        {
            "name" : "Australian regions",
            "cut_item_name":"yes",

            "southWest" : [-10.6681857235, 113.338953078],
            "northEast" : [-43.6345972634, 153.569469029],

            #"southWest" : [5.539516, 152.134475],
            #"northEast": [73.652082, -59.979969],

            "i_day_min": 30,
            "items": [
                "Australia-Australian Capital Territory",
                "Australia-New South Wales",
                "Australia-Northern Territory",
                "Australia-Queensland",
                "Australia-South Australia",
                "Australia-Tasmania",
                "Australia-Victoria",
                "Australia-Western Australia",
            ]
        },
    ]
    return offsets, focuspools

def import_from_daily():

    force_rebuild = False

    debug_folder = home + "/covidtrends/flask/csv/"

    # daterange helper function to loop over dates
    from datetime import timedelta, date
    def daterange(start_date, end_date):
        #for n in range(int ((end_date - start_date).days) ):
        for n in range(int ((end_date - start_date).days) + 1):
            yield start_date + timedelta(n)

    column_schema_history = [
        'Province/State__COL__Country/Region__COL__Last Update__COL__Confirmed__COL__Deaths__COL__Recovered', 
        'Province/State__COL__Country/Region__COL__Last Update__COL__Confirmed__COL__Deaths__COL__Recovered__COL__Latitude__COL__Longitude', 
        'FIPS__COL__Admin2__COL__Province_State__COL__Country_Region__COL__Last_Update__COL__Lat__COL__Long___COL__Confirmed__COL__Deaths__COL__Recovered__COL__Active__COL__Combined_Key'
    ]

    # get files from web or from local cache
    start_date = date(2020, 1, 22) # RIMETTERE 1 INVECE di 3
    end_date = date.today()
    print("end_date is : ",end_date)
    #end_date = date.today() + timedelta(days=1) # TEST
    #yesterday = end_date - timedelta(days=1)
    always_update_last_days = 4

    df_list = []
    days = []
    for single_date in daterange(start_date, end_date):
        d = single_date.strftime("%m-%d-%Y")
        cache_file = cache_build_folder+d+".csv"

        try:
            if not os.path.exists(cache_file) or force_rebuild or (end_date - single_date).days < always_update_last_days or cache_needs_new_download(cache_file, hours=96):
                df = pandas.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'+d+'.csv')
                df.to_csv(cache_file, index=False)
            else:
                df = pandas.read_csv(cache_file)

            #if d == "03-23-2020":
            #    new_row = dict(FIPS="dummy", Admin2="dummy", Province_State=None, Country_Region="France", Last_Update="dummy", \
            #        Lat=0, Long_=0, Confirmed=19161, Deaths=887, Recovered=1650, Active=0, Combined_Key="dummy")
            #    df = df.append(new_row, ignore_index=True)

            df_list.append(df)
            d_formatted = single_date.strftime("%m/%d/%y")
            days.append(d_formatted)
        except:
            print("day: ",single_date," not found. Hope it is today or yesterday. Going on...")

    print("days: ",days)

    # build column history (to be activated only when error below is triggered, requires code manual modification)
    build_column_history = False
    if build_column_history:
        columns_schema_history = set()
        for df in df_list:
            col_list = list(df.columns)
            col_string = "__COL__".join(col_list)
            print("col_list: ",col_list)
            columns_schema_history.add(col_string)
        print("columns_history: ",columns_schema_history, len(columns_schema_history))
        import sys; sys.exit(0)

    # renaming
    #df_list_fixed = []
    ## rule to translate names
    #for i_df, df in enumerate(df_list):
    #    cr = "Country/Region" 
    #    #df[cr] = df[cr].replace(to_replace = ".*Russia.*", value = "Russia", regex=True)
    #    indexNames = (df["Country_Region"] == "Mainland China") 
    #    df.loc[indexNames, "Country_Region"] = "China"

    #    #df[cr] = df[cr].replace(to_replace = ".*Iran.*", value = "Iran", regex=True)
    #    #df[cr] = df[cr].replace(to_replace = ".*United.*States", value = "US", regex=True)
    #    df_list_fixed.append(df)
    #df_list = copy.deepcopy(df_list_fixed)

    def rename_cr(df, date=""):
        indexNames = (df["Country_Region"] == "Mainland China")
        df.loc[indexNames, "Country_Region"] = "China"
        indexNames = (df["Country_Region"].str.contains("Russia"))
        df.loc[indexNames, "Country_Region"] = "Russia"
        indexNames = (df["Country_Region"].str.contains("Iran"))
        df.loc[indexNames, "Country_Region"] = "Iran"
        indexNames = (df["Country_Region"].str.contains("United States"))
        df.loc[indexNames, "Country_Region"] = "US"
        indexNames = (df["Country_Region"].str.contains("Kingdom"))
        df.loc[indexNames, "Country_Region"] = "United Kingdom"
        indexNames = (df["Country_Region"].str.contains("Korea")) 
        df.loc[indexNames, "Country_Region"] = "Korea, South"

        indexNames = (df["Province_State"] == "United Kingdom")
        df.loc[indexNames, "Province_State"] = None
        indexNames = (df["Country_Region"].str.contains("United Kingdom")) & (df["Province_State"].str.contains("UK"))
        df.loc[indexNames, "Province_State"] = None
        indexNames = (df["Country_Region"].str.contains("UK")) 
        df.loc[indexNames, "Country_Region"] = "United Kingdom"
        indexNames = (df["Country_Region"].str.contains("France")) & (df["Province_State"].str.contains("France"))
        df.loc[indexNames, "Province_State"] = None

        # fix France error in special date, seems to be confused mainland with polynesia
        if len(date) > 0:
            print("data to rename: ",date)
            if date == "03/23/20":
                indexNames = (df["Country_Region"].str.contains("France")) & (df["Province_State"].str.contains("French Polynesia"))
                print("Fixing Polynesia :",len(indexNames))
                df.loc[indexNames, "Confirmed"] = 18
                df.loc[indexNames, "Recovered"] = 18
                df.loc[indexNames, "Deaths"] = 18

        return df

    # uniform different column schema and aggregate Admin2 (counties) US
    df_list_fixed = []
    df_abbr = pandas.read_csv(home + "/covidtrends/webapp/flask/state_abbreviation.csv")

    for i_df, df in enumerate(df_list):
        col_list = list(df.columns)
        col_string = "__COL__".join(col_list)
        try:
            ics = column_schema_history.index(col_string)
        except:
            print("New column schema found. Need to manually correct this part of code! Sorry exiting...")
            raise

        if ics in [0,1]:
            US_counties_present = df[(df["Country/Region"] == "US") & (df["Province/State"].str.contains(","))]
            US_states_present = df[(df["Country/Region"] == "US") & (df["Province/State"].str.contains(",") == False)]
            # US: counties and not states (aggregate to build states)
            if US_states_present.empty and not US_counties_present.empty:
                print("column_schema = ",ics, "counties present/states NOT present => aggregating counties in states")
                df.loc[df['Country/Region'] == 'US', 'Province/State'] = \
                    df[df['Country/Region'] == 'US']['Province/State'].copy().str.split(",").str[1].str.strip()
                df = df.merge(df_abbr, left_on=["Province/State"], right_on=["Abbreviation"], suffixes=(None,None), how="left")
                df.loc[df['Country/Region'] == 'US', 'Province/State'] = df[df['Country/Region'] == 'US']['State'].copy()
                df = df.drop(['Abbreviation', 'State'], axis=1)
            # US: both counties and states (delete rows with counties)
            elif not US_states_present.empty and not US_counties_present.empty:
                print("column_schema = ",ics, "counties present/states present => deleting counties")
                # Get rows with US counties
                indexNames = df[ (df["Country/Region"] == "US") & (df["Province/State"].str.contains(",")) ].index
                # Delete these rows indexes from dataFrame
                df.drop(indexNames , inplace=True)
            # US: only states (at least we hope so, if empty get warning!)
            elif not US_states_present.empty and US_counties_present.empty:
                print("column_schema = ",ics, "counties NOT present/states present => doing nothing")
            else:
                print("column_schema = ",ics, "Error! Both states and counties NOT present")

            df = df.rename(columns={'Province/State': 'Province_State', "Country/Region": 'Country_Region'})

            if ics == 1:
                df = df.drop(["Latitude", "Longitude"], axis=1)

            # renaming must be before groupby
            df = rename_cr(df, days[i_df])

            # aggregate counties (same Province_State and Country_Region)
            df = df[["Province_State", "Country_Region", "Confirmed", "Deaths", "Recovered"]]
            # NaN values are excluded from groupy. To avoid it:
            # https://stackoverflow.com/questions/18429491/groupby-columns-with-nan-missing-values/43375020#43375020
            df = df.fillna(0).groupby(["Province_State", "Country_Region"], as_index=False)
            df = df.agg({'Confirmed':'sum', 'Deaths':'sum', 'Recovered':'sum'})
            df[["Province_State"]] = df[["Province_State"]].replace(to_replace = 0, value = np.nan)

        if ics == 2:
            print("column schema = ",ics)

            # renaming must be before groupby
            df = rename_cr(df, days[i_df])

            df = df[["Province_State", "Country_Region", "Lat", "Long_", "Confirmed", "Deaths", "Recovered"]]
            # NaN values are excluded from groupy. To avoid it:
            # https://stackoverflow.com/questions/18429491/groupby-columns-with-nan-missing-values/43375020#43375020
            df = df.fillna(0).groupby(["Province_State", "Country_Region"], as_index=False)
            df = df.agg({'Lat':'mean', 'Long_':'mean', 'Confirmed':'sum', 'Deaths':'sum', 'Recovered':'sum'})
            df = df.rename(columns={'Long_': 'Long'})
            df[["Province_State"]] = df[["Province_State"]].replace(to_replace = 0, value = np.nan)
            #debug df.to_csv("/home/www-data/covidtrends/flask/debug"+str(i_df)+".csv", index=False) ; raise

        df = df.rename(columns={'Province_State': 'Province/State', 'Country_Region':'Country/Region'})

        df_list_fixed.append(df)
    

    # merge all days to three days ago have uniform rows
    # we do not use the last two because they may have partial results with only a subset of countries
    df_current = df_list_fixed[-3]
    #debug df_current.to_csv("/home/www-data/covidtrends/flask/debug_current.csv", index=False)
    print("number of rows current: ",len(df_current.index))
    df_list = []
    for i_df, df in enumerate(df_list_fixed):
        # beware: 
        # pandas bug: right join does not preserve order of rows of the right table (is it really a bug?)
        #BUGdf = df[["Province/State", "Country/Region", "Confirmed", "Deaths", "Recovered"]].merge(df_current[["Province/State", "Country/Region", "Lat", "Long"]], 
        #BUG    left_on=["Province/State", "Country/Region"], right_on=["Province/State","Country/Region"],
        #BUG    how="right", suffixes=(None, None))
        df = df_current[["Province/State", "Country/Region", "Lat", "Long"]].merge(df[["Province/State", "Country/Region", "Confirmed", "Deaths", "Recovered"]],
            left_on=["Province/State", "Country/Region"], right_on=["Province/State","Country/Region"],
            how="left", suffixes=(None, None), indicator=True)

        ### I do not remember why there is -1 in the next line
        ##cols_to_fill = [i for i in df.columns[0:-1] if i != "Province/State"]
        ##df[cols_to_fill] = df[cols_to_fill].fillna(0)

        #debug df.to_csv("/home/www-data/covidtrends/flask/debug"+str(i_df).zfill(3)+".csv", index=False)

        df_list.append(df)

    #df_list.append(df_current)
    

    # merge all days in time-series: Confirmed, Deaths, Recovered
    df_dict = dict()
    df = df_list[-1]
    df_dict["Confirmed"] = df[["Province/State", "Country/Region", "Lat", "Long"]].copy()
    df_dict["Deaths"]    = df[["Province/State", "Country/Region", "Lat", "Long"]].copy()
    df_dict["Recovered"] = df[["Province/State", "Country/Region", "Lat", "Long"]].copy()
    for i_df, df in enumerate(df_list):
        print("number of rows i_df: ",len(df[["Confirmed"]].index))
        day = days[i_df]
        df_dict["Confirmed"][day] = df[["Confirmed"]].copy()
        df_dict["Deaths"][day]    = df[["Deaths"]].copy()
        df_dict["Recovered"][day] = df[["Recovered"]].copy()
    
    for stat_type in ["Confirmed", "Deaths", "Recovered"]:
        df = df_dict[stat_type]
        # avoiding filling the last available two days (today and yesterday) with 0 because data may be not available
        cols_to_fill = [i for i in df.columns[0:-2] if i != "Province/State"]
        df[cols_to_fill] = df[cols_to_fill].fillna(0)

    # special countries with Recovered are only aggregated in "Recovered" fake state
    # estimation of recovered for each state as:
    # state_recovered = all_recovered * state_deaths / all_deaths
    special_countries = ["US", "Canada"]
    for sc in special_countries:
        # Drop "Recovered" fake state for Confirmed and Deaths (meaningless)
        for stat_type in ["Confirmed", "Deaths"]:
            df = df_dict[stat_type]
            indexNames = df[ (df["Country/Region"] == sc) & (df["Province/State"] == "Recovered")  ].index
            df.drop(indexNames , inplace=True)
        print("Analyzing special: ",sc)

        df = df_dict["Deaths"]
        df = df[ df['Country/Region'] == sc ]
        special_deaths = df[ [ col for col in df.columns if col not in ["Province/State", "Country/Region", "Lat", "Long"] ] ]
        special_deaths_all = special_deaths.sum(axis=0)
        #print("list(special_deaths_all): ",list(special_deaths_all))
        #special_deaths_all.to_csv(debug_folder+"debug_deaths.csv", index=False)

        df = df_dict["Recovered"]
        special_recovered_all = df[ (df['Country/Region'] == sc) & (df['Province/State'] == 'Recovered') ]
        #print("list(special_recovered_all): ",list(special_recovered_all.iloc[0]))
        #special_recovered_all.to_csv(debug_folder+"debug_recovered.csv", index=False)
        #print("sda: ",special_deaths_all[["03-28-2020"]])
        #print("sdr: ",special_recovered_all[["03-28-2020"]])

        for icol, col_day in enumerate(special_deaths.columns):
            all_recovered = special_recovered_all.loc[ special_recovered_all.index[0], col_day] 
            all_deaths = list(special_deaths_all)[icol]
            print("country date/recovered/deaths: ",sc, col_day, all_recovered, all_deaths)
            for i_state, _ in special_deaths[[col_day]].iterrows():
                state_deaths  = special_deaths.at[ i_state, col_day]
                #state_deaths  = special_deaths.loc[ special_deaths.index[i_state], col_day]
                print("i_state, state_deaths: ",i_state, state_deaths)
                if all_deaths > 0:
                    float_recovered = all_recovered * state_deaths / all_deaths
                else:
                    float_recovered = 0
                try:
                    df.at[i_state, col_day] = int(float_recovered)
                except:
                    print("Error! Cannot reconstruct recovered per state i_state, col_day, float_recovered: ", \
                        i_state, col_day, float_recovered)

        # Drop "Recovered" fake state also for Recovered (data was used above)
        for stat_type in ["Recovered"]:
            df = df_dict[stat_type]
            indexNames = df[ (df["Province/State"] == "Recovered") & (df["Country/Region"] == sc) ].index
            df.drop(indexNames , inplace=True)

    df_dict["Confirmed"].to_csv("/home/www-data/covidtrends/flask/debug_Confirmed.csv", index=False)
    df_dict["Deaths"].to_csv("/home/www-data/covidtrends/flask/debug_Deaths.csv", index=False)
    df_dict["Recovered"].to_csv("/home/www-data/covidtrends/flask/debug_Recovered.csv", index=False)
    
    return df_dict

import copy
def filter_trends(trends, countryregion, cr_allowed):
    countryregion_filtered = []
    for cr in countryregion:
        if cr in cr_allowed:
            countryregion_filtered.append(cr)

    trends_filtered = dict()
    for tk in trends.keys(): # ["confirmed", "active", ...]
        trends_filtered[tk] = dict()
    for cr , v in trends["Confirmed"].items():
        if cr in cr_allowed:
            for tk, tv in trends.items():
                trends_filtered[tk][cr] = copy.deepcopy(tv[cr])
    return trends_filtered, countryregion_filtered

def build_all_cache():
    if not os.path.exists(cache_build_folder):
        os.mkdir(cache_build_folder)

    build_and_save_italia()
    build_merge_and_save_hopkins()

    countryregion, days = build_countryregion_and_days()
    #with open("crlist.dat", "w") as f:
    #    f.write(str(countryregion))
    trends = build_basic_trends()
    compute_advanced_trends(trends)
    if R0_REALTIME:
        compute_r0_trends(trends)

    #cr_allowed = ["Italy", "Spain", "Germany"]
    #cr_allowed['Albania', 'Algeria', 'Andorra', 'Argentina', 'Armenia', 'Australia', 'Australia-Australian Capital Territory', 
    #           'Australia-New South Wales', 'Australia-Northern Territory', 'Australia-Queensland', 'Australia-South Australia', 
    #           'Australia-Tasmania', 'Australia-Victoria', 'Australia-Western Australia', 'Austria', 'Azerbaijan', 'Bahamas', 
    #           'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan', 'Bolivia', 
    #           'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burma', 'Burundi', 
    #           'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Canada-Alberta', 'Canada-British Columbia', 'Canada-Diamond Princess', 
    #           'Canada-Grand Princess', 'Canada-Manitoba', 'Canada-New Brunswick', 'Canada-Newfoundland and Labrador', 
    #           'Canada-Northwest Territories', 'Canada-Nova Scotia', 'Canada-Nunavut', 'Canada-Ontario', 'Canada-Prince Edward Island', 
    #           'Canada-Quebec', 'Canada-Repatriated Travellers', 'Canada-Saskatchewan', 'Canada-Yukon', 'Central African Republic', 
    #           'Chad', 'Chile', 'China', 'China-Anhui', 'China-Beijing', 'China-Chongqing', 'China-Fujian', 'China-Gansu', 
    #           'China-Guangdong', 'China-Guangxi', 'China-Guizhou', 'China-Hainan', 'China-Hebei', 'China-Heilongjiang', 'China-Henan', 
    #           'China-Hong Kong', 'China-Hubei', 'China-Hunan', 'China-Inner Mongolia', 'China-Jiangsu', 'China-Jiangxi', 'China-Jilin', 
    #           'China-Liaoning', 'China-Macau', 'China-Ningxia', 'China-Qinghai', 'China-Shaanxi', 'China-Shandong', 'China-Shanghai', 
    #           'China-Shanxi', 'China-Sichuan', 'China-Tianjin', 'China-Tibet', 'China-Xinjiang', 'China-Yunnan', 'China-Zhejiang', 
    #           'Colombia', 'Comoros', 'Congo (Brazzaville)', 'Congo (Kinshasa)', 'Costa Rica', "Cote d'Ivoire", 'Croatia', 'Cuba', 
    #           'Cyprus', 'Czechia', 'Denmark', 'Denmark-Faroe Islands', 'Denmark-Greenland', 'Diamond Princess', 'Djibouti', 'Dominica', 
    #           'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 
    #           'Fiji', 'Finland', 'France', 'France-French Guiana', 'France-French Polynesia', 'France-Guadeloupe', 'France-Martinique', 
    #           'France-Mayotte', 'France-New Caledonia', 'France-Reunion', 'France-Saint Barthelemy', 'France-Saint Pierre and Miquelon', 
    #           'France-St Martin', 'France-Wallis and Futuna', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Grenada', 
    #           'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Holy See', 'Honduras', 'Hungary', 'Iceland', 'India', 
    #           'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Italy-Abruzzo', 'Italy-Basilicata', 'Italy-Calabria', 
    #           'Italy-Campania', 'Italy-Emilia-Romagna', 'Italy-Friuli Venezia Giulia', 'Italy-Lazio', 'Italy-Liguria', 'Italy-Lombardia', 
    #           'Italy-Marche', 'Italy-Molise', 'Italy-P.A. Bolzano', 'Italy-P.A. Trento', 'Italy-Piemonte', 'Italy-Puglia', 'Italy-Sardegna', 
    #           'Italy-Sicilia', 'Italy-Toscana', 'Italy-Umbria', "Italy-Valle d'Aosta", 'Italy-Veneto', 'Jamaica', 'Japan', 'Jordan', 
    #           'Kazakhstan', 'Kenya', 'Korea, South', 'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 
    #           'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'MS Zaandam', 'Madagascar', 'Malawi', 'Malaysia', 
    #           'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco', 
    #           'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Namibia', 'Nepal', 'Netherlands', 'Netherlands-Aruba', 
    #           'Netherlands-Bonaire, Sint Eustatius and Saba', 'Netherlands-Curacao', 'Netherlands-Sint Maarten', 'New Zealand', 
    #           'Nicaragua', 'Niger', 'Nigeria', 'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Panama', 'Papua New Guinea', 
    #           'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis', 
    #           'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 
    #           'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 
    #           'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 'Syria', 'Taiwan*', 'Tajikistan', 'Tanzania', 
    #           'Thailand', 'Timor-Leste', 'Togo', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'US', 'Uganda', 'Ukraine', 'United Arab Emirates', 
    #           'United Kingdom', 'United Kingdom-Anguilla', 'United Kingdom-Bermuda', 'United Kingdom-British Virgin Islands', 
    #           'United Kingdom-Cayman Islands', 'United Kingdom-Channel Islands', 'United Kingdom-Falkland Islands (Malvinas)', 
    #           'United Kingdom-Gibraltar', 'United Kingdom-Isle of Man', 'United Kingdom-Montserrat', 
    #           'United Kingdom-Saint Helena, Ascension and Tristan da Cunha', 'United Kingdom-Turks and Caicos Islands', 'Uruguay', 
    #           'Uzbekistan', 'Vanuatu', 'Venezuela', 'Vietnam', 'West Bank and Gaza', 'World', 'Yemen', 'Zambia', 'Zimbabwe']
    cr_allowed = [ 'Argentina', 'Australia', 'Austria', 'Belgium', 'Bosnia and Herzegovina', 'Brazil', 'Bulgaria', 
               'Canada', 'Chile', 'China', 'Colombia', 'Croatia', 'Cuba', 'Cyprus', 'Czechia', 'Denmark', 
               'Ecuador', 'Egypt', 'El Salvador', 'Finland', 'France', 'Georgia', 'Germany', 'Greece', 
               'Holy See', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Italy-Abruzzo', 'Italy-Basilicata', 'Italy-Calabria', 
               'Italy-Campania', 'Italy-Emilia-Romagna', 'Italy-Friuli Venezia Giulia', 'Italy-Lazio', 'Italy-Liguria', 'Italy-Lombardia', 
               'Italy-Marche', 'Italy-Molise', 'Italy-P.A. Bolzano', 'Italy-P.A. Trento', 'Italy-Piemonte', 'Italy-Puglia', 'Italy-Sardegna', 
               'Italy-Sicilia', 'Italy-Toscana', 'Italy-Umbria', "Italy-Valle d'Aosta", 'Italy-Veneto', 'Japan', 'Jordan', 
               'Korea, South', 'Kosovo', 'Kuwait', 'Latvia', 'Lebanon', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 
               'Mexico', 'Monaco', 'Mongolia', 'Montenegro', 'Morocco', 'Netherlands', 'New Zealand', 
               'Norway', 'Pakistan', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 
               'San Marino', 'Saudi Arabia', 'Serbia', 'Singapore', 'Slovakia', 'Slovenia', 'South Africa',
               'Spain', 'Sri Lanka', 'Sweden', 'Switzerland', 'Syria', 'Taiwan*', 
               'Thailand', 'Tunisia', 'Turkey', 'US', 'Ukraine', 'United Arab Emirates', 
               'United Kingdom', 'Uruguay', 'Vietnam', 'West Bank and Gaza', 'World']
    trends, countryregion = filter_trends(trends, countryregion, cr_allowed)

    trends_pop, pop_not_found = compute_population_trends(trends)

    offsets, focuspools = build_offsets_and_focuspools()

    pickle.dump( trends, open(cache_build_folder+"trends_cache.bin", "wb"))
    pickle.dump( trends_pop, open(cache_build_folder+"trends_pop_cache.bin", "wb"))
    pickle.dump( pop_not_found, open(cache_build_folder+"pop_not_found_cache.bin", "wb"))
    pickle.dump( list(trends.keys()), open(cache_build_folder+"stat_types_cache.bin", "wb"))
    pickle.dump( countryregion, open(cache_build_folder+"countryregion_cache.bin", "wb"))
    pickle.dump( days, open(cache_build_folder+"days_cache.bin", "wb"))
    pickle.dump( focuspools, open(cache_build_folder+"focuspools_cache.bin", "wb"))
    pickle.dump( offsets, open(cache_build_folder+"offsets_cache.bin", "wb"))

    for cachefile in glob.glob(cache_build_folder+"*cache*"):
        print("copying cachefile: ",cachefile)
        shutil.copy(cachefile, home + "www-data/")

import json
def build_all_cache_nginx():
    stat_types = pickle.load(open(home + "www-data/stat_types_cache.bin", "rb"))
    days = pickle.load(open(home + "www-data/days_cache.bin", "rb"))
    countryregion = pickle.load(open(home + "www-data/countryregion_cache.bin", "rb"))
    focuspools = pickle.load(open(home + "www-data/focuspools_cache.bin", "rb"))
    offsets = pickle.load(open(home + "www-data/offsets_cache.bin", "rb"))
    trends = pickle.load(open(home + "www-data/trends_cache.bin", "rb"))
    trends_pop = pickle.load(open(home + "www-data/trends_pop_cache.bin", "rb"))
    pop_not_found = pickle.load(open(home + "www-data/pop_not_found_cache.bin", "rb"))

    days2 = []
    for day in days:
        if "/" in day:
            y = "20"+day[-2:]
            d = day[day.find("/")+1 : day.find("/", day.find("/")+1)]
            m = day[0:day.find("/")]
            days2.append(y + "-" + m + "-" + d)
        else:
            days2.append(day)
    print(days2)


    cr_json = dict(countryregion=countryregion, days=days2, stat_types=stat_types, offsets=offsets, focuspools=focuspools, pop_not_found=pop_not_found)
    with open(home + "www-data/cr.json","w") as f:
        json.dump(cr_json, f)

    trends_json = dict(trends=trends, trends_pop=trends_pop)
    with open(home + "www-data/trends.json","w") as f:
        json.dump(trends_json, f)

    for cachefile in glob.glob(cache_build_folder+"*json"):
        print("copying json cachefile: ",cachefile)
        shutil.copy(cachefile, home + "www-data/")

if __name__ == "__main__":
    build_all_cache()
    build_all_cache_nginx()
    #import_from_daily()
