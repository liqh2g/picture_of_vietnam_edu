import pandas as pd
import requests
import numpy as np

class extract_data_to_csv():

    def get_api(self, api: str) -> dict:
        self.api = api

        req = requests.get(api)
        res = req.json()

        return res
    
    def gdp_extract(self, api_json: str, field: str) -> pd.DataFrame:
        self.api_json = api_json
        self.field = field

        df = pd.DataFrame(api_json[1])
        df['country_name'] = df['country'].apply(lambda x : x['value'])
        df.drop(['country', 'indicator','countryiso3code'], axis= 1, inplace= True)
        df.insert(0, 'country_name', df.pop('country_name'))
        df = df.loc[:, 'country_name':'value']
        df.rename(columns= {'value':field, 'date': 'year'}, inplace = True)
        gdp = pd.DataFrame(df).pivot_table(index='year', columns='country_name', values= field).reset_index()

        return gdp
    
    def pop_extract(self, api_json: str) -> pd.DataFrame:
        self.api_pop = api_json

        df = pd.DataFrame(api_json[1])
        name = df['indicator'].apply(lambda x:x['value'])[0]
        df.rename(columns= {'date': 'year', 'value': name}, inplace= True)
        df = df[['year', name]]
        return df
    
    def vnStudent_extract(self, api_json: str, field: list) -> pd.DataFrame:
        self.api_json = api_json
        self.field = field

        dimension = api_json['dataset']['dimension']
        locate = [i for i in dimension if 'Năm' in i][0]
        semeter = dimension[locate]['category']['label']
        year = [int(value.split('-')[0].strip()) for value in semeter.values()]
        value = api_json['dataset']['value']
        df = pd.DataFrame({'year': year,
                           field[0]: value[0:len(year)],
                           field[1]: value[len(year):len(year)*2],
                           field[2]: value[len(year)*2:]})
        
        df[df.isnull()] = 0

        return df
    
    def gdp_compare_to_csv(self, api_gpd_growth: str, api_gdp_total: str, api_gdp_capital: str) -> pd.DataFrame:
        self.api_gdp_growth = api_gpd_growth
        self.api_gdp_total = api_gdp_total
        self.api_gdp_capital = api_gdp_capital

        e = extract_data_to_csv()

        # gdp growth
        growth_json = e.get_api(api= api_gpd_growth)
        growth_df = e.gdp_extract(api_json= growth_json, field= 'gdp_growth')

        # gdp total
        total_json = e.get_api(api= api_gdp_total)
        total_df = e.gdp_extract(api_json= total_json, field= 'gdp')

        # gdp PPP
        ppp_json = e.get_api(api= api_gdp_capital)
        ppp_df = e.gdp_extract(api_json= ppp_json, field= 'gdp_cap')

        # gdp compare
        gdp_compare = pd.merge(total_df, pd.merge(growth_df, ppp_df, how= 'outer', on= 'year', suffixes= (' %', ' GDP PPP')), how= 'outer', on= 'year')
        gdp_compare.columns.name = None
        gdp_compare['year'] = gdp_compare['year'].astype('int64')

        return gdp_compare
    
    def population_to_csv(self, api_pop: list, gdp_compare: pd.DataFrame) -> pd.DataFrame:
        self.api_pop = api_pop
        self.gdp_compare = gdp_compare
        e = extract_data_to_csv()

        df_lst = []
        name = []

        for api in api_pop:
            api_json = e.get_api(api)
            df = e.pop_extract(api_json)
            name.append(df.columns[1])
            df_lst.append(df)

        pop_df = pd.concat(df_lst, axis= 1)
        pop = pop_df.drop('year', axis= 1)
        pop.insert(0, 'year', pop_df.iloc[:, 0].astype('int64'))
        # pop.rename(columns= {'SP.POP.TOTL':'Total population', 'SP.RUR.TOTL': 'Rural Population', 'SP.URB.TOTL': 'Urban Population', 
        #                      'SP.POP.GROW': 'Population growth %', 'SP.DYN.CBRT.IN': 'Birth rate per 1000'}, inplace = True)
        pop['Newborn'] = round(pop[name[0]]*(pop[name[-1]]/1000), 0)
        gdp_vn = gdp_compare[['year', 'Viet Nam', 'Viet Nam %']].rename(columns={'Viet Nam': 'GDPinUSD', 'Viet Nam %': 'GDP growth %'})
        pop = pop.merge(gdp_vn, on= 'year').sort_values(by= 'year').reset_index(drop= True)
        
        return pop

    def vietNamStudent_to_csv(self, api_k_12: str, api_higher_edu: str) -> pd.DataFrame:
        self.api_k_12 = api_k_12
        self.api_higher_edu = api_higher_edu

        e = extract_data_to_csv()

        # K-12 education
        k_12_js = e.get_api(api= api_k_12)
        df_k_12 = e.vnStudent_extract(api_json= k_12_js, field= ['Primary', 'Secondary', 'Highschool'])

        # higher education
        high_json = e.get_api(api= api_higher_edu)
        df_higher_edu = e.vnStudent_extract(api_json= high_json, field= ['Total_higher_edu', 'Higher_public_edu', 'Higher_private_edu'])

        # merge to Vietnam Student
        df_k_12.insert(1, 'K_12', df_k_12.loc[:, 'Primary':].sum(axis=1))
        df_higher_edu['Total_higher_edu'] = df_higher_edu['Higher_private_edu'] + df_higher_edu['Higher_public_edu']
        vnStd = df_higher_edu.merge(df_k_12, how='outer', on='year')
        vnStd.iloc[:, 1:] *= 1000

        return vnStd
    
    def vietNamStudent2_to_csv(self, api_edu_rate: str, vnStd: pd.DataFrame) -> pd.DataFrame:
        '''the estimating version coming along with Vietnamstudent.csv, with estimated number of students in college schools in 2015, 2016, 2017 and 2018'''
        self.api_edu_rate = api_edu_rate
        self.vnStd = vnStd
        
        e = extract_data_to_csv()

        # increasing rate in Vietnamese education
        vnStd_rate_json = e.get_api(api= api_edu_rate)
        vnStd_rate = e.vnStudent_extract(api_json= vnStd_rate_json, field= ['Total_higher_edu', 'Higher_public_edu', 'Higher_private_edu'])
        vnStd_rate = vnStd_rate.loc[(vnStd_rate['year']>2014)].set_index('year').T

        # higher education from 2014 to 2020
        high_edu_14_21 = vnStd.loc[(vnStd['year'] == 2014), :'Higher_private_edu'].set_index('year').T
        for y in range(2015, 2021):
            high_edu_14_21[y] = high_edu_14_21[y-1]*(vnStd_rate[y]/100)
        high_edu_14_21 = high_edu_14_21.T.reset_index()
        high_edu_14_21['year'] = high_edu_14_21['year'].astype('int32')

        vnStd.iloc[20:, 0:4] = high_edu_14_21.iloc[1:]

        return vnStd